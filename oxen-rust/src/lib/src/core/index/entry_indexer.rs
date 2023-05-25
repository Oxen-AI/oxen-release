//! EntryIndexer is responsible for pushing, pulling and syncing commit entries
//!

use async_recursion::async_recursion;

use indicatif::ProgressBar;
use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use std::sync::Arc;

use crate::constants::HISTORY_DIR;
use crate::core::index::{self, puller, versioner, Merger};
use crate::core::index::{
    CommitDirEntryReader, CommitDirEntryWriter, CommitEntryReader, CommitWriter, RefWriter,
};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, RemoteBranch, RemoteRepository};
use crate::util;
use crate::{api, current_function};

use super::pusher;

pub struct EntryIndexer {
    pub repository: LocalRepository,
}

impl EntryIndexer {
    pub fn new(repository: &LocalRepository) -> Result<EntryIndexer, OxenError> {
        Ok(EntryIndexer {
            repository: repository.clone(),
        })
    }

    pub async fn push(&self, rb: &RemoteBranch) -> Result<RemoteRepository, OxenError> {
        pusher::push(&self.repository, rb).await
    }

    pub async fn pull(&self, rb: &RemoteBranch) -> Result<(), OxenError> {
        println!("🐂 Oxen pull {} {}", rb.remote, rb.branch);

        let remote = self
            .repository
            .get_remote(&rb.remote)
            .ok_or(OxenError::remote_not_set(&rb.remote))?;

        let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
            Err(err) => return Err(err),
        };

        let head_commit = api::local::commits::head_commit(&self.repository)?;
        if let Some(commit) = self.pull_all_commit_objects(&remote_repo, rb).await? {
            self.pull_all_entries_for_commit(&remote_repo, &head_commit, &commit)
                .await?;
        }
        Ok(())
    }

    pub async fn pull_all_entries_for_commit(
        &self,
        remote_repo: &RemoteRepository,
        head_commit: &Commit,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        log::debug!(
            "pull_all_entries_for_commit for commit: {} -> {}",
            commit.id,
            commit.message
        );
        let limit: usize = 0; // zero means pull all
        self.pull_entries_for_commit(remote_repo, head_commit, commit, limit)
            .await
    }

    pub async fn pull_all_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
    ) -> Result<Option<Commit>, OxenError> {
        let remote_branch_err = format!("Remote branch not found: {}", rb.branch);
        let remote_branch = api::remote::branches::get_by_name(remote_repo, &rb.branch)
            .await?
            .ok_or_else(|| OxenError::basic_str(&remote_branch_err))?;
        match api::remote::commits::get_by_id(remote_repo, &remote_branch.commit_id).await {
            Ok(Some(commit)) => {
                log::debug!(
                    "Oxen pull got remote commit: {} -> '{}'",
                    commit.id,
                    commit.message
                );

                // Make sure this branch points to this commit
                self.set_branch_name_for_commit(&rb.branch, &commit)?;

                println!("🐂 fetching commit objects {}", commit.id);
                // Sync the commit objects
                self.rpull_missing_commit_objects(remote_repo, &commit)
                    .await?;
                log::debug!(
                    "pull_all_commit_objects DONE {} -> '{}'",
                    commit.id,
                    commit.message
                );
                return Ok(Some(commit));
            }
            Ok(None) => {
                eprintln!("oxen pull error: remote head does not exist");
            }
            Err(err) => {
                log::debug!("oxen pull could not get remote head: {}", err);
            }
        }

        Ok(None)
    }

    fn set_branch_name_for_commit(&self, name: &str, commit: &Commit) -> Result<(), OxenError> {
        let ref_writer = RefWriter::new(&self.repository)?;
        // Make sure head is pointing to that branch
        ref_writer.set_head(name);
        ref_writer.set_branch_commit_id(name, &commit.id)
    }

    /// Just pull the commit db and history dbs that are missing (not the entries)
    async fn rpull_missing_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        remote_head_commit: &Commit,
    ) -> Result<(), OxenError> {
        // See if we have the DB pulled
        let commit_db_dir = util::fs::oxen_hidden_dir(&self.repository.path)
            .join(HISTORY_DIR)
            .join(remote_head_commit.id.clone());
        if !commit_db_dir.exists() {
            // We don't have db locally, so pull it
            log::debug!(
                "commit db for {} not found, pull from remote",
                remote_head_commit.id
            );
            self.check_parent_and_pull_commit_objects(remote_repo, remote_head_commit)
                .await?;
        } else {
            // else we are synced
            log::debug!("commit db for {} already downloaded", remote_head_commit.id);
        }

        Ok(())
    }

    #[async_recursion]
    async fn check_parent_and_pull_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        match api::remote::commits::get_remote_parent(remote_repo, &commit.id).await {
            Ok(parents) => {
                if parents.is_empty() {
                    log::debug!("no parents for commit {}", commit.id);
                } else {
                    // Recursively sync the parents
                    for parent in parents.iter() {
                        self.check_parent_and_pull_commit_objects(remote_repo, parent)
                            .await?;
                    }
                }
            }
            Err(err) => {
                log::warn!("oxen pull could not get commit parents: {}", err);
            }
        }

        // Pulls dbs and commit object
        self.pull_commit_data_objects(remote_repo, commit).await?;

        Ok(())
    }

    async fn pull_commit_data_objects(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        log::debug!(
            "pull_commit_data_objects {} `{}`",
            commit.id,
            commit.message
        );

        // Download the specific commit_db that holds all the entries
        api::remote::commits::download_commit_db_to_repo(&self.repository, remote_repo, &commit.id)
            .await?;

        // Get commit and write it to local DB
        let remote_commit = api::remote::commits::get_by_id(remote_repo, &commit.id)
            .await?
            .unwrap();
        let writer = CommitWriter::new(&self.repository)?;
        writer.add_commit_to_db(&remote_commit)
    }

    // For unit testing a half synced commit
    pub async fn pull_entries_for_commit_with_limit(
        &self,
        remote_repo: &RemoteRepository,
        head_commit: &Commit,
        commit: &Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        self.pull_commit_data_objects(remote_repo, commit).await?;
        self.pull_entries_for_commit(remote_repo, head_commit, commit, limit)
            .await
    }

    fn read_pulled_commit_entries(
        &self,
        commit: &Commit,
        mut limit: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let commit_reader = CommitEntryReader::new(&self.repository, commit)?;
        let entries = commit_reader.list_entries()?;
        log::debug!(
            "{} limit {} entries.len() {}",
            current_function!(),
            limit,
            entries.len()
        );
        if limit == 0 {
            limit = entries.len();
        }
        Ok(entries[0..limit].to_vec())
    }

    pub async fn pull_entries_for_commit(
        &self,
        remote_repo: &RemoteRepository,
        head_commit: &Commit,
        commit: &Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        log::debug!(
            "🐂 pull_entries_for_commit_id commit {} -> '{}'",
            commit.id,
            commit.message
        );

        if index::commit_sync_status::commit_is_synced(&self.repository, commit) {
            log::debug!(
                "🐂 commit {} -> '{}' is already synced",
                commit.id,
                commit.message
            );
            return Ok(());
        }

        let entries = self.read_pulled_commit_entries(commit, limit)?;
        log::debug!(
            "🐂 pull_entries_for_commit_id commit_id {} limit {} entries.len() {}",
            commit.id,
            limit,
            entries.len()
        );

        // Pull all the entries and unpack them to the versions dir
        puller::pull_entries(remote_repo, &entries, &self.repository.path, &|| {
            self.backup_to_versions_dir(commit, &entries).unwrap();

            if limit == 0 {
                // limit == 0 means we pulled everything, so mark it as complete
                self.pull_complete(commit).unwrap();
            }
        })
        .await?;

        // Cleanup files that shouldn't be there
        self.cleanup_removed_entries(commit)?;

        // If the branches have diverged, we need to merge the commit into the base
        let merger = Merger::new(&self.repository)?;
        merger.merge_commit_into_base(commit, head_commit)?;

        Ok(())
    }

    fn backup_to_versions_dir(
        &self,
        commit: &Commit,
        entries: &Vec<CommitEntry>,
    ) -> Result<(), OxenError> {
        println!("Unpacking...");
        let bar = Arc::new(ProgressBar::new(entries.len() as u64));
        let dir_entries = api::local::entries::group_entries_to_parent_dirs(entries);

        dir_entries.par_iter().for_each(|(dir, entries)| {
            let committer = CommitDirEntryWriter::new(&self.repository, &commit.id, dir).unwrap();
            entries.par_iter().for_each(|entry| {
                let filepath = self.repository.path.join(&entry.path);
                versioner::backup_file(&self.repository, &committer, entry, filepath).unwrap();
                bar.inc(1);
            });
        });

        bar.finish();

        log::debug!("Done Unpacking.");

        Ok(())
    }

    fn pull_complete(&self, commit: &Commit) -> Result<(), OxenError> {
        // This is so that we know when we switch commits that we don't need to pull versions again
        index::commit_sync_status::mark_commit_as_synced(&self.repository, commit)?;

        // When we successfully pull the data, the repo is no longer shallow
        self.repository.write_is_shallow(false)?;

        Ok(())
    }

    fn cleanup_removed_entries(&self, commit: &Commit) -> Result<(), OxenError> {
        let repository = self.repository.clone();
        let commit = commit.clone();
        for dir_entry_result in WalkDirGeneric::<((), Option<bool>)>::new(&self.repository.path)
            .skip_hidden(true)
            .process_read_dir(move |_, parent, _, dir_entry_results| {
                let parent = util::fs::path_relative_to_dir(parent, &repository.path).unwrap();
                let commit_reader =
                    CommitDirEntryReader::new(&repository, &commit.id, &parent).unwrap();

                dir_entry_results
                    .par_iter_mut()
                    .for_each(|dir_entry_result| {
                        if let Ok(dir_entry) = dir_entry_result {
                            if !dir_entry.file_type.is_dir() {
                                let short_path = util::fs::path_relative_to_dir(
                                    dir_entry.path(),
                                    &repository.path,
                                )
                                .unwrap();
                                let path = short_path.file_name().unwrap().to_str().unwrap();
                                // If we don't have the file in the commit, remove it
                                if !commit_reader.has_file(path) {
                                    let full_path = repository.path.join(short_path);
                                    if util::fs::remove_file(full_path).is_ok() {
                                        dir_entry.client_state = Some(true);
                                    }
                                }
                            }
                        }
                    })
            })
        {
            match dir_entry_result {
                Ok(dir_entry) => {
                    if let Some(was_removed) = &dir_entry.client_state {
                        if !*was_removed {
                            log::debug!("Removed file {:?}", dir_entry)
                        }
                    }
                }
                Err(err) => {
                    log::error!("Could not remove file {}", err)
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::core::index::EntryIndexer;
    use crate::error::OxenError;
    use crate::model::RemoteBranch;
    use crate::opts::CloneOpts;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_indexer_partial_pull_then_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);

            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let remote_repo = api::remote::repositories::create(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            command::push(&repo).await?;

            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let mut opts = CloneOpts::new(remote_repo.remote.url.to_owned(), &new_repo_dir);
                opts.shallow = true;

                let cloned_repo = command::clone(&opts).await?;
                let indexer = EntryIndexer::new(&cloned_repo)?;
                let head_commit = api::local::commits::head_commit(&repo)?;

                // Pull a part of the commit
                let commits = api::local::commits::list(&repo)?;
                let latest_commit = commits.first().unwrap();
                let page_size = 2;
                let limit = page_size;
                indexer
                    .pull_entries_for_commit_with_limit(
                        &remote_repo,
                        &head_commit,
                        latest_commit,
                        limit,
                    )
                    .await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                // try to pull the full thing again even though we have only partially pulled some
                let rb = RemoteBranch::default();
                indexer.pull(&rb).await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(og_num_files, num_files);

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_indexer_partial_pull_multiple_commits() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let train_dir = repo.path.join("train");
            command::add(&repo, &train_dir)?;
            // Commit the file
            command::commit(&repo, "Adding training data")?;

            let test_dir = repo.path.join("test");
            command::add(&repo, &test_dir)?;
            // Commit the file
            command::commit(&repo, "Adding testing data")?;

            // Create remote
            let remote_repo = api::remote::repositories::create(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            // Push it
            command::push(&repo).await?;

            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let mut opts = CloneOpts::new(remote_repo.remote.url.to_owned(), &new_repo_dir);
                opts.shallow = true;
                let cloned_repo = command::clone(&opts).await?;
                let indexer = EntryIndexer::new(&cloned_repo)?;
                let head_commit = api::local::commits::head_commit(&repo)?;

                // Pull a part of the commit
                let commits = api::local::commits::list(&repo)?;
                let last_commit = commits.first().unwrap();
                let limit = 7;
                indexer
                    .pull_entries_for_commit_with_limit(
                        &remote_repo,
                        &head_commit,
                        last_commit,
                        limit,
                    )
                    .await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }
}
