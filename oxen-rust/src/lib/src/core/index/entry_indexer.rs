//! EntryIndexer is responsible for pushing, pulling and syncing commit entries
//!

use indicatif::ProgressBar;
use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use crate::constants::{self, DEFAULT_REMOTE_NAME, HISTORY_DIR};
use crate::core::index::{self, puller, versioner, Merger};
use crate::core::index::{
    CommitDirEntryReader, CommitDirEntryWriter, CommitEntryReader, RefWriter,
};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, RemoteBranch, RemoteRepository};
use crate::opts::PullOpts;
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{api, current_function};

use super::{pusher, CommitReader};

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

    pub async fn pull(&self, rb: &RemoteBranch, opts: PullOpts) -> Result<(), OxenError> {
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

        // original head commit, only applies to pulling commits after initial clone
        let maybe_head_commit = api::local::commits::head_commit(&self.repository);

        let mut commit = if opts.should_pull_all {
            self.pull_all(&remote_repo, rb, opts.should_update_head)
                .await?
        } else {
            self.pull_one(&remote_repo, rb, opts.should_update_head)
                .await?
        };

        // TODO Do we add a flag for if this pull is a merge somehow...?
        // If the branches have diverged, we need to merge the commit into the base
        if maybe_head_commit.is_ok() {
            let head_commit = maybe_head_commit.unwrap();
            if head_commit.id != commit.id {
                let merger = Merger::new(&self.repository)?;
                if let Some(merge_commit) = merger.merge_commit_into_base(&commit, &head_commit)? {
                    commit = merge_commit;
                }
            }
        }

        // Cleanup files that shouldn't be there
        self.cleanup_removed_entries(&commit)?;

        Ok(())
    }

    pub async fn pull_commit(&self, commit: &Commit) -> Result<(), OxenError> {
        // Get the remote, TODO: make this configurable
        let remote = self
            .repository
            .get_remote(DEFAULT_REMOTE_NAME)
            .ok_or(OxenError::remote_not_set(DEFAULT_REMOTE_NAME))?;
        let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
            Err(err) => return Err(err),
        };

        self.pull_commit_entries_db(&remote_repo, commit).await?;
        self.pull_all_entries_for_commit(&remote_repo, commit)
            .await?;

        Ok(())
    }

    async fn pull_all(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
        should_update_head: bool,
    ) -> Result<Commit, OxenError> {
        match self.pull_all_commit_objects(remote_repo, rb).await {
            Ok(Some(commit)) => {
                log::debug!("pull_result: {} -> {}", commit.id, commit.message);
                // Make sure this branch points to this commit
                self.set_branch_name_for_commit(&rb.branch, &commit, should_update_head)?;

                for c in api::local::commits::list_all(&self.repository)? {
                    self.pull_all_entries_for_commit(remote_repo, &c).await?;
                }
                Ok(commit)
            }
            Ok(None) => api::local::commits::head_commit(&self.repository),
            Err(err) => {
                // if no commit objects, means repo is empty, so instantiate the local repo
                log::error!("pull_all error: {}", err);
                eprintln!("warning: You appear to have cloned an empty repository. Initializing with an empty commit.");
                api::local::commits::commit_with_no_files(
                    &self.repository,
                    constants::INITIAL_COMMIT_MSG,
                )
            }
        }
    }

    async fn pull_one(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
        should_update_head: bool,
    ) -> Result<Commit, OxenError> {
        match self
            .pull_most_recent_commit_object(remote_repo, rb, should_update_head)
            .await
        {
            Ok(Some(commit)) => {
                log::debug!("pull_result: {} -> {}", commit.id, commit.message);
                self.pull_all_entries_for_commit(remote_repo, &commit)
                    .await?;
                Ok(commit)
            }
            Ok(None) => api::local::commits::head_commit(&self.repository),
            Err(err) => {
                // if no commit objects, means repo is empty, so instantiate the local repo
                log::debug!("pull_one empty repo: {}", err);
                eprintln!("warning: You appear to have cloned an empty repository. Initializing with an empty commit.");
                api::local::commits::commit_with_no_files(
                    &self.repository,
                    constants::INITIAL_COMMIT_MSG,
                )
            }
        }
    }

    pub async fn pull_all_entries_for_commit(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        log::debug!(
            "pull_all_entries_for_commit for commit: {} -> {}",
            commit.id,
            commit.message
        );
        let limit: usize = 0; // zero means pull all
        self.pull_entries_for_commit(remote_repo, commit.clone(), limit)
            .await?;
        log::debug!(
            "DONE! pull_all_entries_for_commit for commit: {} -> {}",
            commit.id,
            commit.message
        );
        Ok(())
    }

    pub async fn pull_most_recent_commit_object(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
        should_update_head: bool,
    ) -> Result<Option<Commit>, OxenError> {
        let remote_branch_err = format!("Remote branch not found: {}", rb.branch);
        let remote_branch = api::remote::branches::get_by_name(remote_repo, &rb.branch)
            .await?
            .ok_or_else(|| OxenError::basic_str(&remote_branch_err))?;

        // Download the commits db
        println!("🐂 fetching commits for branch {}", rb.branch);
        api::remote::commits::download_commits_db_to_repo(&self.repository, remote_repo).await?;

        match api::remote::commits::get_by_id(remote_repo, &remote_branch.commit_id).await {
            Ok(Some(commit)) => {
                log::debug!(
                    "Oxen pull got remote commit: {} -> '{}'",
                    commit.id,
                    commit.message
                );

                // Make sure this branch points to this commit
                self.set_branch_name_for_commit(&rb.branch, &commit, should_update_head)?;

                // Sync the commit entries objects
                self.pull_commit_entries_db(remote_repo, &commit).await?;

                log::debug!(
                    "pull_commit_object DONE {} -> '{}'",
                    commit.id,
                    commit.message
                );
                return Ok(Some(commit));
            }
            Ok(None) => {
                println!("Everything up to date.");
            }
            Err(err) => {
                log::warn!(
                    "pull_most_recent_commit_object could not get remote commit: {}",
                    err
                );
            }
        }

        Ok(None)
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

        // list all the remote commits on a branch, so we know how many we have to pull
        let remote_commits =
            api::remote::commits::list_commit_history(remote_repo, &remote_branch.commit_id)
                .await?;

        // We may not have any commits yet, in the case of a fresh clone
        let local_commits: Vec<Commit> = match CommitReader::new(&self.repository) {
            // empty vector if we can't read the commits db
            Ok(reader) => reader.history_from_head().unwrap_or(Vec::new()),
            _ => Vec::new(),
        };
        let local_commits: HashSet<&Commit> = HashSet::from_iter(local_commits.iter());

        // Figure out how many we are missing
        let mut missing_commits = Vec::new();
        for remote_commit in remote_commits {
            if !local_commits.contains(&remote_commit) {
                missing_commits.push(remote_commit);
            }
        }

        let total_missing = missing_commits.len();
        if total_missing == 0 {
            // Nothing to do
            return Ok(None);
        }
        println!("🐂 fetching {} commit objects", total_missing);

        // Download full commits db
        api::remote::commits::download_commits_db_to_repo(&self.repository, remote_repo).await?;

        // Download the missing commit objects
        let progress_bar = oxen_progress_bar(total_missing as u64, ProgressBarType::Counter);
        match api::remote::commits::get_by_id(remote_repo, &remote_branch.commit_id).await {
            Ok(Some(commit)) => {
                log::debug!(
                    "Oxen pull got remote commit: {} -> '{}'",
                    commit.id,
                    commit.message
                );

                // Sync the commit objects
                self.pull_missing_commit_objects(remote_repo, missing_commits, &progress_bar)
                    .await?;
                log::debug!(
                    "pull_all_commit_objects DONE {} -> '{}'",
                    commit.id,
                    commit.message
                );
                return Ok(Some(commit));
            }
            Ok(None) => {
                log::debug!("pull_all_commit_objects commit does not exist");
            }
            Err(err) => {
                log::warn!(
                    "pull_all_commit_objects could not get remote commit: {}",
                    err
                );
            }
        }
        progress_bar.finish();
        Ok(None)
    }

    fn set_branch_name_for_commit(
        &self,
        name: &str,
        commit: &Commit,
        set_head: bool,
    ) -> Result<(), OxenError> {
        let ref_writer = RefWriter::new(&self.repository)?;
        if set_head {
            // Make sure head is pointing to that branch
            ref_writer.set_head(name);
        }
        ref_writer.set_branch_commit_id(name, &commit.id)
    }

    /// Just pull the commit db and history dbs that are missing (not the entries)
    async fn pull_missing_commit_objects(
        &self,
        remote_repository: &RemoteRepository,
        commits: Vec<Commit>,
        bar: &Arc<ProgressBar>,
    ) -> Result<(), OxenError> {
        // TODO: these async task queues are gnarly...abstract away
        use tokio::time::{sleep, Duration};
        type PieceOfWork = (LocalRepository, RemoteRepository, Commit, Arc<ProgressBar>);
        type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
        type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

        let total_missing = commits.len();
        log::debug!("Chunking and sending {} larger files", total_missing);
        let commits: Vec<PieceOfWork> = commits
            .iter()
            .map(|c| {
                (
                    self.repository.to_owned(),
                    remote_repository.to_owned(),
                    c.to_owned(),
                    bar.to_owned(),
                )
            })
            .collect();

        let queue = Arc::new(TaskQueue::new(total_missing));
        let finished_queue = Arc::new(FinishedTaskQueue::new(total_missing));
        for commit in commits.iter() {
            queue.try_push(commit.to_owned()).unwrap();
            finished_queue.try_push(false).unwrap();
        }

        let worker_count: usize = if num_cpus::get() > total_missing {
            total_missing
        } else {
            num_cpus::get()
        };

        log::debug!(
            "worker_count {} total_missing {}",
            worker_count,
            total_missing
        );

        for worker in 0..worker_count {
            let queue = queue.clone();
            let finished_queue = finished_queue.clone();
            tokio::spawn(async move {
                loop {
                    let (repository, remote_repo, commit, bar) = queue.pop().await;
                    log::debug!("worker[{}] processing task...", worker);

                    // See if we have the DB pulled
                    let commit_db_dir = util::fs::oxen_hidden_dir(&repository.path)
                        .join(HISTORY_DIR)
                        .join(commit.id.clone());
                    if !commit_db_dir.exists() {
                        // We don't have db locally, so pull it
                        log::debug!("commit db for {} not found, pull from remote", commit.id);

                        // Pulls dbs and commit object
                        match api::remote::commits::download_commit_entries_db_to_repo(
                            &repository,
                            &remote_repo,
                            &commit.id,
                        )
                        .await
                        {
                            Ok(_) => {
                                log::debug!("commit db for {} downloaded", commit.id);
                                bar.inc(1);
                            }
                            Err(err) => {
                                log::debug!("commit db for {} failed: {}", commit.id, err);
                            }
                        }
                    } else {
                        // else we are synced
                        log::debug!("commit db for {} already downloaded", commit.id);
                    }

                    finished_queue.pop().await;
                }
            });
        }

        while finished_queue.len() > 0 {
            // log::debug!("Before waiting for {} workers to finish...", queue.len());
            sleep(Duration::from_secs(1)).await;
        }
        log::debug!("All commit db downloads tasks done. :-)");

        Ok(())
    }

    async fn pull_commit_entries_db(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        log::debug!("pull_commit_entries_db {} `{}`", commit.id, commit.message);

        // Download the specific commit_db that holds all the entries
        api::remote::commits::download_commit_entries_db_to_repo(
            &self.repository,
            remote_repo,
            &commit.id,
        )
        .await?;
        Ok(())
    }

    // For unit testing a half synced commit
    pub async fn pull_entries_for_commit_with_limit(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        self.pull_commit_entries_db(remote_repo, commit).await?;
        self.pull_entries_for_commit(remote_repo, commit.clone(), limit)
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
        commit: Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        log::debug!(
            "🐂 pull_entries_for_commit_id commit {} -> '{}'",
            commit.id,
            commit.message
        );

        if index::commit_sync_status::commit_is_synced(&self.repository, &commit) {
            log::debug!(
                "🐂 commit {} -> '{}' is already synced",
                commit.id,
                commit.message
            );
            return Ok(());
        }

        let entries = self.read_pulled_commit_entries(&commit, limit)?;
        log::debug!(
            "🐂 pull_entries_for_commit_id commit_id {} limit {} entries.len() {}",
            commit.id,
            limit,
            entries.len()
        );

        // Pull all the entries and unpack them to the versions dir
        puller::pull_entries(remote_repo, &entries, &self.repository.path, &|| {
            self.backup_to_versions_dir(&commit, &entries).unwrap();

            if limit == 0 {
                // limit == 0 means we pulled everything, so mark it as complete
                self.pull_complete(&commit).unwrap();
            }
        })
        .await?;

        Ok(())
    }

    fn backup_to_versions_dir(
        &self,
        commit: &Commit,
        entries: &Vec<CommitEntry>,
    ) -> Result<(), OxenError> {
        if entries.is_empty() {
            return Ok(());
        }
        println!("Unpacking...");
        let bar = oxen_progress_bar(entries.len() as u64, ProgressBarType::Counter);
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
        log::debug!("CLEANUP_REMOVED_ENTRIES commit {}", commit);
        let repository = self.repository.clone();
        let commit = commit.clone();
        let commit_reader = CommitEntryReader::new(&repository, &commit)?;
        for dir_entry_result in WalkDirGeneric::<((), Option<bool>)>::new(&self.repository.path)
            .skip_hidden(true)
            .process_read_dir(move |_, parent, _, dir_entry_results| {
                log::debug!(
                    "cleanup_removed_entries checking parent dir {:?} for repo {:?}",
                    parent,
                    repository.path
                );

                let parent = util::fs::path_relative_to_dir(parent, &repository.path).unwrap();

                log::debug!(
                    "cleanup_removed_entries got parent dir {:?} for commit {} -> '{}'",
                    parent,
                    commit.id,
                    commit.message
                );

                let commit_entry_reader =
                    CommitDirEntryReader::new(&repository, &commit.id, &parent).unwrap();

                dir_entry_results
                    .par_iter_mut()
                    .for_each(|dir_entry_result| {
                        if let Ok(dir_entry) = dir_entry_result {
                            log::debug!(
                                "{} considering entry {:?}",
                                current_function!(),
                                dir_entry
                            );

                            let short_path =
                                util::fs::path_relative_to_dir(dir_entry.path(), &repository.path)
                                    .unwrap();

                            log::debug!(
                                "{} considering short path {:?}",
                                current_function!(),
                                short_path
                            );

                            if !dir_entry.file_type.is_dir() {
                                let path = short_path.file_name().unwrap().to_str().unwrap();
                                // If we don't have the file in the commit, remove it
                                if !commit_entry_reader.has_file(path) {
                                    log::debug!(
                                        "{} commit reader does not have file {:?}",
                                        current_function!(),
                                        path
                                    );

                                    let full_path = repository.path.join(short_path);
                                    if util::fs::remove_file(full_path).is_ok() {
                                        dir_entry.client_state = Some(true);
                                    }
                                }
                            } else {
                                // is dir
                                // make sure we have the dir in the commit and it is a subdir (!= "")
                                if !commit_reader.has_dir(&short_path)
                                    && short_path != Path::new("")
                                {
                                    log::debug!(
                                        "{} commit reader does not have dir {:?}",
                                        current_function!(),
                                        short_path
                                    );

                                    let full_path = repository.path.join(short_path);
                                    if full_path.exists()
                                        && util::fs::remove_dir_all(full_path).is_ok()
                                    {
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
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::core::index::EntryIndexer;
    use crate::error::OxenError;
    use crate::model::RemoteBranch;
    use crate::opts::CloneOpts;
    use crate::opts::PullOpts;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_indexer_pull_full_commit_history() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Get the commits from the local repo to compare against later
            let og_commits = api::local::commits::list_all(&repo)?;

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

                let rb = RemoteBranch {
                    remote: DEFAULT_REMOTE_NAME.to_owned(),
                    branch: DEFAULT_BRANCH_NAME.to_owned(),
                };

                // Pull all the commit objects
                indexer.pull_all_commit_objects(&remote_repo, &rb).await?;

                let pulled_commits = api::local::commits::list_all(&repo)?;
                assert_eq!(pulled_commits.len(), og_commits.len());

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

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

                // Pull a part of the commit
                let commits = api::local::commits::list(&repo)?;
                let latest_commit = commits.first().unwrap();
                let page_size = 2;
                let limit = page_size;
                indexer
                    .pull_entries_for_commit_with_limit(&remote_repo, latest_commit, limit)
                    .await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                // try to pull the full thing again even though we have only partially pulled some
                let rb = RemoteBranch::default();
                indexer
                    .pull(
                        &rb,
                        PullOpts {
                            should_update_head: true,
                            should_pull_all: true,
                        },
                    )
                    .await?;

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

                // Pull a part of the commit
                let commits = api::local::commits::list(&repo)?;
                let last_commit = commits.first().unwrap();
                let limit = 7;
                indexer
                    .pull_entries_for_commit_with_limit(&remote_repo, last_commit, limit)
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
