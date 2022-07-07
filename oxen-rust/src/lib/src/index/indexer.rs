use filetime::FileTime;
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::fs;
use std::path::Path;
use std::{thread, time};

use crate::api;
use crate::constants::HISTORY_DIR;
use crate::error::OxenError;
use crate::index::{CommitEntryReader, CommitEntryWriter, CommitReader, CommitWriter, RefWriter};
use crate::model::{
    Commit, CommitEntry, CommitStats, LocalRepository, RemoteBranch, RemoteRepository,
};
use crate::util;

pub struct Indexer {
    pub repository: LocalRepository,
}

impl Indexer {
    pub fn new(repository: &LocalRepository) -> Result<Indexer, OxenError> {
        Ok(Indexer {
            repository: repository.clone(),
        })
    }

    pub fn push(&self, rb: &RemoteBranch) -> Result<RemoteRepository, OxenError> {
        println!("🐂 Oxen push {} {}", rb.remote, rb.branch);
        let remote = self
            .repository
            .get_remote(&rb.remote)
            .ok_or_else(OxenError::remote_not_set)?;

        // Create or fetch the remote repository
        let remote_repo = match api::remote::repositories::get_by_url(&remote.url) {
            Ok(Some(repo)) => repo,
            _ => api::remote::repositories::create(&self.repository)?,
        };

        // Push unsynced commit db and history dbs
        let commit_reader = CommitReader::new(&self.repository)?;
        let head_commit = commit_reader.head_commit()?;
        self.rpush_missing_commit_objects(&head_commit, rb)?;

        let remote_branch = api::remote::branches::create_or_get(&remote_repo, &rb.branch)?;
        match api::remote::commits::get_by_id(&self.repository, &remote_branch.commit_id) {
            Ok(Some(commit)) => {
                log::debug!(
                    "push {} {} got commit {} '{}'",
                    rb.remote,
                    rb.branch,
                    commit.id,
                    commit.message
                );
                // recursively check commits against remote head
                // and sync ones that have not been synced
                let remote_stats = api::remote::commits::get_stats(&self.repository, &commit)?;
                self.rpush_entries(&commit_reader, &remote_stats, &head_commit.id, 0)?;
                Ok(remote_repo)
            }
            Ok(None) => {
                println!("No commits to push.");
                Ok(remote_repo)
            }
            Err(err) => {
                let msg = format!("Err: {}", err);
                Err(OxenError::basic_str(&msg))
            }
        }
    }

    fn rpush_missing_commit_objects(
        &self,
        local_commit: &Commit,
        rb: &RemoteBranch,
    ) -> Result<(), OxenError> {
        // check if commit exists on remote
        // if not, push the commit and it's dbs
        match api::remote::commits::get_by_id(&self.repository, &local_commit.id) {
            Ok(Some(remote_commit)) => {
                // We have remote commit, stop syncing
                log::debug!(
                    "rpush_missing_commit_objects stop, we have remote parent {} -> '{}'",
                    remote_commit.id,
                    remote_commit.message
                );

                api::remote::commits::post_commit_to_server(
                    &self.repository,
                    &rb.branch,
                    local_commit,
                )?;
            }
            Ok(None) => {
                // We don't have remote commit
                // Recursively find local parent and remote parents
                for parent_id in local_commit.parent_ids.iter() {
                    // We should have a local parent if the local_commit has parent id
                    let local_parent = api::local::commits::get_by_id(&self.repository, parent_id)?
                        .ok_or_else(|| OxenError::local_parent_link_broken(&local_commit.id))?;

                    self.rpush_missing_commit_objects(&local_parent, rb)?;

                    // Unroll and post commits
                    api::remote::commits::post_commit_to_server(
                        &self.repository,
                        &rb.branch,
                        local_commit,
                    )?;
                }

                log::debug!(
                    "rpush_missing_commit_objects stop, no more local parents {} -> '{}'",
                    local_commit.id,
                    local_commit.message
                );
            }
            Err(err) => {
                let err = format!("Could not push missing commit err: {}", err);
                return Err(OxenError::basic_str(&err));
            }
        }

        Ok(())
    }

    fn rpush_entries(
        &self,
        commit_reader: &CommitReader,
        remote_stats: &Option<CommitStats>,
        local_commit_id: &str,
        depth: usize,
    ) -> Result<(), OxenError> {
        log::debug!(
            "rpush_entries depth {} commit_id {}",
            depth,
            local_commit_id
        );
        if let Some(stats) = remote_stats {
            if local_commit_id == stats.commit.id {
                if depth == 0 && stats.is_synced() {
                    println!("No commits to push, remote is synced.");
                    return Ok(());
                } else if stats.is_synced() {
                    log::debug!("rpush_entries stats.is_synced {:?}", stats);
                    return Ok(());
                }
            }
        }

        if let Some(commit) = commit_reader.get_commit_by_id(local_commit_id)? {
            for parent_id in commit.parent_ids.iter() {
                // Recursive call
                self.rpush_entries(commit_reader, remote_stats, parent_id, depth + 1)?;
            }

            log::debug!(
                "Unroll no parent_id on commit: {} -> '{}'",
                commit.id,
                commit.message
            );

            let entries = self.read_unsynced_entries(&commit)?;
            if !entries.is_empty() {
                // Unroll stack to post entries
                log::debug!(
                    "Unroll push commit entries: {} -> '{}'",
                    commit.id,
                    commit.message
                );
                self.push_entries(&entries, &commit)?;
            } else {
                log::debug!(
                    "Unroll no entries to push: {} -> '{}'",
                    commit.id,
                    commit.message
                );
            }
        } else {
            let err = format!("Err: could not find commit: {}", local_commit_id);
            return Err(OxenError::basic_str(&err));
        }

        Ok(())
    }

    fn read_unsynced_entries(&self, commit: &Commit) -> Result<Vec<CommitEntry>, OxenError> {
        // In function scope to open and close this DB for a read, because we are going to write
        // to entries later
        let entry_reader = CommitEntryReader::new(&self.repository, commit)?;
        entry_reader.list_unsynced_entries()
    }

    fn push_entries(&self, entries: &[CommitEntry], commit: &Commit) -> Result<(), OxenError> {
        println!("🐂 push {} files", entries.len());
        for entry in entries.iter() {
            log::debug!("push entry {:?}", entry.path);
        }

        // len is usize and progressbar requires u64, I don't think we'll overflow...
        let size: u64 = unsafe { std::mem::transmute(entries.len()) };
        let bar = ProgressBar::new(size);

        let entry_writer = CommitEntryWriter::new(&self.repository, commit)?;
        entries.par_iter().for_each(|entry| {
            // Retry logic
            let total_tries = 5;
            let mut num_tries = 0;
            for i in 0..total_tries {
                if let Ok(_) = self.push_entry(&entry_writer, entry) {
                    break;
                }
                let duration = time::Duration::from_secs(i + 1);
                thread::sleep(duration);
                num_tries += 1;
            }

            if num_tries == total_tries {
                log::error!("Error pushing entry {:?}", entry);
            }

            bar.inc(1);
        });

        bar.finish();

        Ok(())
    }

    pub fn push_entry(
        &self,
        entry_writer: &CommitEntryWriter,
        entry: &CommitEntry,
    ) -> Result<(), OxenError> {
        /*
        Check if the entry is synced or not, if it is not, go back and make sure
        all parent commit versions are synced as well
        */
        if entry.is_synced {
            return Ok(());
        }

        // Upload entry to server
        match api::remote::entries::create(&self.repository, entry) {
            Ok(_entry) => {
                // The last thing we do is update is_synced for the entry in the local db
                // after it has been posted to the server, so that even if the process
                // is killed, and we don't get here, the worst thing that can happen
                // is we re-upload it.
                match entry_writer.set_is_synced(entry) {
                    Ok(_) => {
                        log::debug!("Entry is synced! {:?}", entry.path);
                        Ok(())
                    }
                    Err(err) => {
                        let err =
                            format!("Error updating hash path: {:?} Err: {}", entry.path, err);
                        Err(OxenError::basic_str(&err))
                    }
                }
            }
            Err(err) => {
                let err = format!("Error uploading {:?} {}", entry.path, err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn pull(&self, rb: &RemoteBranch) -> Result<(), OxenError> {
        println!("🐂 Oxen pull {} {}", rb.remote, rb.branch);

        self.pull_all_commit_objects_then(rb, |commit| {
            // Sync the HEAD commit data
            let limit: usize = 0; // zero means pull all
            self.pull_entries_for_commit(&commit, limit)?;
            Ok(())
        })
    }

    pub fn pull_all_commit_objects(&self, rb: &RemoteBranch) -> Result<(), OxenError> {
        self.pull_all_commit_objects_then(rb, |_commit| {
            // then nothing
            Ok(())
        })
    }

    pub fn pull_all_commit_objects_then<F>(
        &self,
        rb: &RemoteBranch,
        then: F,
    ) -> Result<(), OxenError>
    where
        F: FnOnce(Commit) -> Result<(), OxenError>,
    {
        let remote = self
            .repository
            .get_remote(&rb.remote)
            .ok_or_else(OxenError::remote_not_set)?;

        // Get the remote commit from branch name, and try to recursively pull subsequent commits
        let remote_repo = api::remote::repositories::get_by_url(&remote.url)?
            .ok_or_else(|| OxenError::remote_repo_not_found(&rb.remote))?;
        let remote_branch_err = format!("Remote branch not found: {}", rb.branch);
        let remote_branch = api::remote::branches::get_by_name(&remote_repo, &rb.branch)?
            .ok_or_else(|| OxenError::basic_str(&remote_branch_err))?;
        match api::remote::commits::get_by_id(&self.repository, &remote_branch.commit_id) {
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
                self.rpull_missing_commit_objects(&commit)?;

                then(commit)?;
            }
            Ok(None) => {
                eprintln!("oxen pull error: remote head does not exist");
            }
            Err(err) => {
                log::debug!("oxen pull could not get remote head: {}", err);
            }
        }

        Ok(())
    }

    fn set_branch_name_for_commit(&self, name: &str, commit: &Commit) -> Result<(), OxenError> {
        let ref_writer = RefWriter::new(&self.repository)?;
        // Make sure head is pointing to that branch
        ref_writer.set_head(name);
        ref_writer.set_branch_commit_id(name, &commit.id)
    }

    /// Just pull the commit db and history dbs that are missing (not the entries)
    fn rpull_missing_commit_objects(&self, remote_head_commit: &Commit) -> Result<(), OxenError> {
        // See if we have the DB pulled
        let commit_db_dir = util::fs::oxen_hidden_dir(&self.repository.path)
            .join(HISTORY_DIR)
            .join(remote_head_commit.id.clone());
        if !commit_db_dir.exists() {
            // We don't have db locally, so pull it
            self.check_parent_and_pull_commit_objects(remote_head_commit)?;
        } // else we are synced

        Ok(())
    }

    fn check_parent_and_pull_commit_objects(&self, commit: &Commit) -> Result<(), OxenError> {
        // If we have a parent on the remote
        if let Ok(parents) = api::remote::commits::get_remote_parent(&self.repository, &commit.id) {
            // Recursively sync the parents
            for parent in parents.iter() {
                self.check_parent_and_pull_commit_objects(parent)?;
            }
        }

        // Pulls dbs and commit object
        self.pull_commit_data_objects(commit)?;

        Ok(())
    }

    fn pull_commit_data_objects(&self, commit: &Commit) -> Result<(), OxenError> {
        log::debug!(
            "pull_commit_data_objects {} `{}`",
            commit.id,
            commit.message
        );
        // Download the specific commit_db that holds all the entries
        api::remote::commits::download_commit_db_by_id(&self.repository, &commit.id)?;

        // Get commit and write it to local DB
        let remote_commit = api::remote::commits::get_by_id(&self.repository, &commit.id)?.unwrap();
        let writer = CommitWriter::new(&self.repository)?;
        writer.add_commit_to_db(&remote_commit)
    }

    // For unit testing a half synced commit
    pub fn pull_entries_for_commit_with_limit(
        &self,
        commit: &Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        self.pull_commit_data_objects(commit)?;
        self.pull_entries_for_commit(commit, limit)
    }

    fn read_pulled_commit_entries(
        &self,
        commit: &Commit,
        mut limit: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let commit_reader = CommitEntryReader::new(&self.repository, commit)?;
        let entries = commit_reader.list_entries()?;
        if limit == 0 {
            limit = entries.len();
        }
        Ok(entries[0..limit].to_vec())
    }

    fn pull_entries_for_commit(&self, commit: &Commit, limit: usize) -> Result<(), OxenError> {
        let entries = self.read_pulled_commit_entries(commit, limit)?;
        log::debug!(
            "🐂 pull_entries_for_commit_id commit_id {} limit {} entries.len() {}",
            commit.id,
            limit,
            entries.len()
        );
        if !entries.is_empty() {
            let total = if limit > 0 { limit } else { entries.len() };
            println!("🐂 pulling commit {} with {} entries", commit.id, total);
            let size: u64 = unsafe { std::mem::transmute(total) };
            let bar = ProgressBar::new(size);

            let committer = CommitEntryWriter::new(&self.repository, commit)?;
            // Pull and write all the entries
            entries.par_iter().for_each(|entry| {
                // Retry logic
                let total_tries = 5;
                let mut num_tries = 0;
                for i in 0..total_tries {
                    if let Ok(_) = self.download_remote_entry(entry, &committer) {
                        break;
                    }
                    let duration = time::Duration::from_secs(i + 1);
                    thread::sleep(duration);
                    num_tries += 1;
                }

                if num_tries == total_tries {
                    eprintln!("Pull entry could not download entry {:?}", entry.path);
                }

                bar.inc(1);
            });

            bar.finish();
        }

        // Cleanup files that shouldn't be there
        self.cleanup_removed_entries(commit)?;

        Ok(())
    }

    fn cleanup_removed_entries(&self, commit: &Commit) -> Result<(), OxenError> {
        let commit_reader = CommitEntryReader::new(&self.repository, commit)?;
        for file in util::fs::rlist_files_in_dir(&self.repository.path).iter() {
            let short_path = util::fs::path_relative_to_dir(file, &self.repository.path)?;
            if !commit_reader.contains_path(&short_path)? {
                std::fs::remove_file(file)?;
            }
        }
        Ok(())
    }

    fn download_remote_entry(
        &self,
        entry: &CommitEntry,
        committer: &CommitEntryWriter,
    ) -> Result<(), OxenError> {
        let fpath = self.repository.path.join(&entry.path);
        log::debug!("should_download_entry? {:?}", entry.path);
        if self.should_download_entry(entry, &fpath) {
            if api::remote::entries::download_entry(&self.repository, entry)? {
                log::debug!("Downloaded entry {:?}", entry.path);
            } else {
                log::debug!("Did not download entry {:?}", entry.path);
            }
        } else {
            log::debug!("Skip download entry {:?}", entry.path);
        }

        // Always update modified time to last pulled
        let metadata = fs::metadata(fpath).unwrap();
        let mtime = FileTime::from_last_modification_time(&metadata);
        committer.set_file_timestamps(entry, &mtime)?;

        Ok(())
    }

    fn should_download_entry(&self, entry: &CommitEntry, path: &Path) -> bool {
        !path.exists() || self.path_hash_is_different(entry, path)
    }

    fn path_hash_is_different(&self, entry: &CommitEntry, path: &Path) -> bool {
        if let Ok(hash) = util::hasher::hash_file_contents(path) {
            log::debug!(
                "path_hash_is_different({:?})? {} == {}",
                entry.path,
                hash,
                entry.hash
            );
            return hash != entry.hash;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::index::Indexer;
    use crate::model::RemoteBranch;
    use crate::test;
    use crate::util;

    #[test]
    fn test_indexer_partial_pull_then_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|mut repo| {
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);

            // Set the proper remote
            let remote = api::endpoint::repo_url_from(&repo.name);
            command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push it
            let remote_repo = command::push(&repo)?;

            command::push(&repo)?;

            test::run_empty_dir_test(|new_repo_dir| {
                let cloned_repo = command::clone(&remote_repo.url, new_repo_dir)?;
                let indexer = Indexer::new(&cloned_repo)?;

                // Pull a part of the commit
                let commits = command::log(&repo)?;
                let latest_commit = commits.first().unwrap();
                let page_size = 2;
                let limit = page_size;
                indexer.pull_entries_for_commit_with_limit(latest_commit, limit)?;

                let num_files = util::fs::rcount_files_in_dir(new_repo_dir);
                assert_eq!(num_files, limit);

                // try to pull the full thing again even though we have only partially pulled some
                let rb = RemoteBranch::default();
                indexer.pull(&rb)?;

                let num_files = util::fs::rcount_files_in_dir(new_repo_dir);
                assert_eq!(og_num_files, num_files);

                Ok(())
            })
        })
    }

    #[test]
    fn test_indexer_partial_pull_multiple_commits() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|mut repo| {
            // Set the proper remote
            let remote = api::endpoint::repo_url_from(&repo.name);
            command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let train_dir = repo.path.join("train");
            command::add(&repo, &train_dir)?;
            // Commit the file
            command::commit(&repo, "Adding training data")?;

            let test_dir = repo.path.join("test");
            command::add(&repo, &test_dir)?;
            // Commit the file
            command::commit(&repo, "Adding testing data")?;

            // Push it
            let remote_repo = command::push(&repo)?;
            command::push(&repo)?;

            test::run_empty_dir_test(|new_repo_dir| {
                let cloned_repo = command::clone(&remote_repo.url, new_repo_dir)?;
                let indexer = Indexer::new(&cloned_repo)?;

                // Pull a part of the commit
                let commits = command::log(&repo)?;
                let last_commit = commits.first().unwrap();
                let limit = 7;
                indexer.pull_entries_for_commit_with_limit(last_commit, limit)?;

                let num_files = util::fs::rcount_files_in_dir(new_repo_dir);
                assert_eq!(num_files, limit);

                Ok(())
            })
        })
    }
}
