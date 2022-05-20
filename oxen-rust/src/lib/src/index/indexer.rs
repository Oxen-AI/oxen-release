use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::fs::File;
use std::path::Path;

use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::index::committer::HISTORY_DIR;
use crate::error::OxenError;
use crate::index::{Committer, CommitEntryReader, Referencer};
use crate::model::{
    Commit, CommitEntry, CommitHead, LocalRepository, RemoteRepository,
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

    pub fn create_or_get_repo(&self) -> Result<RemoteRepository, OxenError> {
        let name = &self.repository.name;
        api::remote::repositories::create_or_get(name)
    }

    fn push_entries(&self, committer: &Committer, commit: &Commit) -> Result<(), OxenError> {
        let entries = committer.list_unsynced_entries_for_commit(commit)?;
        if entries.is_empty() {
            return Ok(());
        }

        println!("🐂 push {} files", entries.len());

        // len is usize and progressbar requires u64, I don't think we'll overflow...
        let size: u64 = unsafe { std::mem::transmute(entries.len()) };
        let bar = ProgressBar::new(size);

        let commit_db = &committer.head_commit_db;
        entries.par_iter().for_each(|entry| {
            match self.push_entry(committer, commit_db, entry) {
                Ok(_) => {}
                Err(err) => {
                    log::error!("Error pushing entry {:?} Err {}", entry, err)
                }
            }
            bar.inc(1);
        });

        bar.finish();

        Ok(())
    }

    pub fn push_entry(
        &self,
        committer: &Committer,
        db: &Option<DBWithThreadMode<MultiThreaded>>,
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
                // The last thing we do is update the hash in the local db
                // after it has been posted to the server, so that even if the process
                // is killed, and we don't get here, the worst thing that can happen
                // is we re-upload it.
                match committer.set_is_synced(db, entry) {
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

    pub fn push(&self, committer: &Committer) -> Result<RemoteRepository, OxenError> {
        if self.repository.remote().is_none() {
            return Err(OxenError::basic_str("Must set remote on repository. `oxen set-remote <URL>`"));
        }

        let remote_repo = self.create_or_get_repo()?;
        log::debug!("indexer::push got remote repo: {}", remote_repo.url);
        match committer.get_head_commit() {
            Ok(Some(commit)) => {
                // maybe_push() will recursively check commits head against remote head
                // and sync ones that have not been synced
                let remote_head = api::remote::commits::get_remote_head(&self.repository)?;
                self.maybe_push(committer, &remote_head, &commit.id, 0)?;
                Ok(remote_repo)
            }
            Ok(None) => Err(OxenError::basic_str("No commits to push.")),
            Err(err) => {
                let msg = format!("Err: {}", err);
                Err(OxenError::basic_str(&msg))
            }
        }
    }

    fn maybe_push(
        &self,
        committer: &Committer,
        remote_head: &Option<CommitHead>,
        commit_id: &str,
        depth: usize,
    ) -> Result<(), OxenError> {
        if let Some(head) = remote_head {
            if commit_id == head.commit.id {
                if depth == 0 && head.is_synced() {
                    println!("No commits to push, remote is synced.");
                    return Ok(());
                } else if head.is_synced() {
                    return Ok(());
                }
            }
        }

        if let Some(commit) = committer.get_commit_by_id(commit_id)? {
            if let Some(parent_id) = &commit.parent_id {
                // Recursive call
                self.maybe_push(committer, remote_head, parent_id, depth + 1)?;
            }
            // Unroll stack to post in reverse order
            api::remote::commits::post_commit_to_server(&self.repository, &commit)?;
            self.push_entries(committer, &commit)?;
        } else {
            eprintln!("Err: could not find commit: {}", commit_id);
        }

        Ok(())
    }

    pub fn pull(&self) -> Result<(), OxenError> {
        println!("🐂 Oxen pull");
        // Get the remote head commit, and try to recursively pull subsequent commits
        match api::remote::commits::get_remote_head(&self.repository) {
            Ok(Some(remote_head)) => {
                log::debug!("Oxen pull got remote head: {}", remote_head.commit.id);

                // TODO: Be able to pull a different branch than main
                self.set_branch_name_for_commit(DEFAULT_BRANCH_NAME, &remote_head.commit)?;

                println!("🐂 fetching commit objects...");
                // Sync the commit objects
                self.rpull_missing_commit_objects(&remote_head.commit)?;
                
                // Sync the HEAD commit data
                let limit: usize = 0; // zero means pull all
                self.pull_entries_for_commit(&remote_head.commit, limit)?;
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
        let referencer = Referencer::new(&self.repository)?;
        // Make sure head is pointing to that branch
        referencer.set_head(name);
        referencer.set_branch_commit_id(name, &commit.id)
    }

    /// Just pull the commit objects that are missing (not the data)
    fn rpull_missing_commit_objects(
        &self,
        remote_head_commit: &Commit
    ) -> Result<(), OxenError> {
        // See if we have the DB pulled
        let commit_db_dir = util::fs::oxen_hidden_dir(&self.repository.path).join(HISTORY_DIR).join(remote_head_commit.id.clone());
        if !commit_db_dir.exists() {
            // We don't have HEAD locally, so pull it
            self.check_parent_and_pull_commit_object(&remote_head_commit)?;
        } // else we are synced

        Ok(())
    }

    fn check_parent_and_pull_commit_object(
        &self,
        commit: &Commit
    ) -> Result<(), OxenError> {
        // If we have a parent on the remote
        if let Ok(Some(parent)) =
            api::remote::commits::get_remote_parent(&self.repository, &commit.id)
        {
            // Recursively sync the parent
            self.check_parent_and_pull_commit_object(&parent)?;
        }

        // Pulls dbs and commit object
        self.pull_commit_data_objects(&commit)?;

        Ok(())
    }

    fn pull_commit_data_objects(
        &self,
        commit: &Commit
    ) -> Result<(), OxenError> {
        log::debug!("pull_commit_data_objects {} `{}`", commit.id, commit.message);
        // Download the specific commit_db that holds all the entries
        api::remote::commits::download_commit_db_by_id(&self.repository, &commit.id)?;

        // Get commit and write it to local DB
        // The committer relys on the commit dir being downloaded to add the commit to the commit db
        // Might want to separate this functionality out of the large "committer" into a smaller commit writer...
        let remote_commit = api::remote::commits::get_by_id(&self.repository, &commit.id)?;
        let mut committer = Committer::new(&self.repository)?;
        committer.add_commit(&remote_commit)
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

    fn pull_entries_for_commit(
        &self,
        commit: &Commit,
        mut limit: usize,
    ) -> Result<(), OxenError> {
        
        
        let commit_reader = CommitEntryReader::new(&self.repository, commit)?;
        let entries = commit_reader.list_entries()?;
        if limit == 0 {
            limit = entries.len();
        }
        log::debug!("🐂 pull_entries_for_commit_id commit_id {} limit {} entries.len() {}", commit.id, limit, entries.len());
        if entries.len() > 0 {
            println!("🐂 pulling commit {} with {} entries", commit.id, limit);
            let size: u64 = unsafe { std::mem::transmute(limit) };
            let bar = ProgressBar::new(size);

            // Pull and write all the entries
            entries[0..limit].par_iter().for_each(|entry| {
                if let Err(err) = self.download_remote_entry(entry) {
                    eprintln!("Could not download entry {:?} Err: {:?}", entry.path, err);
                }
                bar.inc(1);
            });

            bar.finish();
        }

        // Cleanup files that shouldn't be there
        self.cleanup_removed_entries(&commit_reader)?;
        
        Ok(())
    }

    fn cleanup_removed_entries(
        &self,
        commit_reader: &CommitEntryReader,
    ) -> Result<(), OxenError> {
        for file in util::fs::rlist_files_in_dir(&self.repository.path).iter() {
            let short_path = util::fs::path_relative_to_dir(file, &self.repository.path)?;
            if !commit_reader.contains_path(&short_path)? {
                log::debug!("REMOVE IT {:?}", file);
                std::fs::remove_file(file)?;
            }
        }
        Ok(())
    }

    fn download_remote_entry(
        &self,
        entry: &CommitEntry,
    ) -> Result<(), OxenError> {
        if self.repository.remote().is_none() {
            return Err(OxenError::basic_str("Must set remote"));
        }

        let config = AuthConfig::default()?;
        let fpath = self.repository.path.join(&entry.path);
        log::debug!("download_remote_entry entry {:?}", entry.path);
        if !fpath.exists() || self.path_hash_is_different(entry, &fpath) {
            let remote = self.repository.remote().unwrap().value;
            let filename = entry.path.to_str().unwrap();
            let url = format!("{}/{}", remote, filename);

            let client = reqwest::blocking::Client::new();
            let mut response = client
                .get(&url)
                .header(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {}", config.auth_token()),
                )
                .send()?;

            if let Some(parent) = fpath.parent() {
                if !parent.exists() {
                    log::debug!("Create parent dir {:?}", parent);
                    std::fs::create_dir_all(parent)?;
                }
            }

            let mut dest = { File::create(fpath)? };
            response.copy_to(&mut dest)?;
        }

        Ok(())
    }

    fn path_hash_is_different(&self, entry: &CommitEntry, path: &Path) -> bool {
        if let Ok(hash) = util::hasher::hash_file_contents(path) {
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
    use crate::test;
    use crate::util;

    #[test]
    fn test_indexer_partial_pull_then_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|mut repo| {
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);

            // Set the proper remote
            let remote = api::endpoint::repo_url_from(&repo.name);
            command::set_remote(&mut repo, constants::DEFAULT_ORIGIN_NAME, &remote)?;

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
                indexer.pull_entries_for_commit_with_limit(&latest_commit, limit)?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                // try to pull the full thing again even though we have only partially pulled some
                indexer.pull()?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
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
            command::set_remote(&mut repo, constants::DEFAULT_ORIGIN_NAME, &remote)?;

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

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                Ok(())
            })
        })
    }
}
