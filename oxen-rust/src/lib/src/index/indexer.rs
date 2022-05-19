use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::fs::File;
use std::path::Path;

use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{
    Commit, CommitEntry, CommitHead, LocalRepository, RemoteEntry, RemoteRepository,
};
use crate::util;

use crate::view::PaginatedEntries;

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

    // TODO: The only reason we need a mutable committer is that we are writing the commit entries
    // Which is already very inefficient
    // Instead when we pull each commit meta data, we should make a request to
    // zip up the remote history/COMMIT_ID entry
    // Download it, unzip it, and put it in the local history dir
    pub fn pull(&self, committer: &mut Committer) -> Result<(), OxenError> {
        log::debug!("🐂 ##### Oxen pull!");
        // Get the remote head commit, and try to recursively pull subsequent commits
        match api::remote::commits::get_remote_head(&self.repository) {
            Ok(Some(remote_head)) => {
                log::debug!("Oxen pull got remote head: {}", remote_head.commit.id);

                // TODO: Be able to pull a different branch than main

                // Make sure head is pointing to that branch
                committer.referencer.set_head(DEFAULT_BRANCH_NAME);
                committer
                    .referencer
                    .set_branch_commit_id(DEFAULT_BRANCH_NAME, &remote_head.commit.id)?;

                // Sync the commit objects
                self.rpull_missing_commit_objects(committer, &remote_head.commit)?;
                
                // Sync the HEAD commit data
                self.rpull_commit_id(committer, &remote_head.commit)?;
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

    /// Just pull the commit objects that are missing (not the data)
    fn rpull_missing_commit_objects(
        &self,
        committer: &mut Committer,
        remote_head_commit: &Commit
    ) -> Result<(), OxenError> {
        let local_head_commit = committer.get_commit_by_id(&remote_head_commit.id)?;
        if local_head_commit.is_none() {
            // We don't have HEAD locally, so pull it
            self.check_parent_and_pull_commit_object(committer, &remote_head_commit)?;
        } // else we are synced

        Ok(())
    }

    fn check_parent_and_pull_commit_object(
        &self,
        committer: &mut Committer,
        commit: &Commit
    ) -> Result<(), OxenError> {
        // If we have a parent on the remote
        if let Ok(Some(parent)) =
            api::remote::commits::get_remote_parent(&self.repository, &commit.id)
        {
            // Check if we have the parent locally
            let local_parent_commit = committer.get_commit_by_id(&parent.id)?;
            if local_parent_commit.is_none() {
                // Recursively sync the parent
                self.check_parent_and_pull_commit_object(committer, &parent)?;
            }
        }

        // Get commit and write it to local DB
        let remote_commit = api::remote::commits::get_by_id(&self.repository, &commit.id)?;
        log::debug!(
            "check_parent_and_pull_commit_object adding commit {:?}",
            remote_commit
        );
        committer.add_commit(&remote_commit)?;

        // Download the specific commit_db that holds all the entries
        api::remote::commits::download_commit_db_by_id(&self.repository, &commit.id)?;

        Ok(())
    }

    fn rpull_commit_id(&self, committer: &mut Committer, remote_commit_head: &Commit) -> Result<(), OxenError> {
        // Check if we have the local commit
        log::debug!("🐂 START rpull_commit_id commit_id {}", remote_commit_head.id);

        // Optimize...
        // only need to really pull the entries for head commit, 
        // and then dbs for the other commits if you are rolling back?
        // Or we could compress the history dir, and pull that, then pull the entries for head

        // Pull all the entry files for that commit
        let page_size: usize = 512;
        let limit = 0; // if limit is 0, we pull it all
        self.pull_entries_for_commit_id(committer, &remote_commit_head.id, page_size, limit)?;

        log::debug!("🐂 END rpull_commit_id commit_id {}", remote_commit_head.id);
        Ok(())
    }

    /// Public for unit testing a partially killed pull
    pub fn pull_entries_for_commit_id_with_limit(
        &self,
        committer: &Committer,
        commit_id: &str,
        limit: usize,
    ) -> Result<(), OxenError> {
        let page_size: usize = 100;
        self.pull_entries_for_commit_id(committer, commit_id, page_size, limit)
    }

    fn pull_entries_for_commit_id(
        &self,
        committer: &Committer,
        commit_id: &str,
        page_size: usize,
        mut limit: usize,
    ) -> Result<(), OxenError> {
        log::debug!("🐂 pull_entries_for_commit_id commit_id {}", commit_id);
        let first_page_idx = 1;
        let entries = api::remote::entries::list_page(&self.repository, commit_id, first_page_idx, page_size)?;
        if limit == 0 {
            limit = entries.total_entries;
        }

        let commit_db_path = committer.history_dir.join(Path::new(&commit_id));
        log::debug!("pull_entries_for_commit_id before open commit_db {:?}", commit_db_path);
    
        let opts = Committer::db_opts();
        let db = DBWithThreadMode::open(&opts, &commit_db_path)?;
        log::debug!("pull_entries_for_commit_id after open commit_db {:?}", commit_db_path);

        let total: usize = limit;
        if total > 0 {
            println!("🐂 pulling commit {} limit {} entries", commit_id, total);
            let size: u64 = unsafe { std::mem::transmute(total) };
            let bar = ProgressBar::new(size);

            // Pull and write all the entries
            self.pull_entries(committer, &db, &entries, commit_id, &bar, first_page_idx, page_size, limit)?;
            log::debug!("🐂 DONE pull_entries_for_commit_id pulling commit {} limit {} entries", commit_id, total);
            bar.finish();
        }

        // Cleanup files that shouldn't be there
        self.cleanup_removed_entries(committer, &db)?;
        
        Ok(())
    }

    fn cleanup_removed_entries(
        &self,
        committer: &Committer,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        for file in util::fs::rlist_files_in_dir(&self.repository.path).iter() {
            let short_path = util::fs::path_relative_to_dir(file, &self.repository.path)?;
            let key = short_path.to_str().unwrap().as_bytes();
            match db.get(key) {
                Ok(Some(_value)) => {
                    // we have it, keep it
                    // log::debug!("Keep file: {:?}", file);
                }
                Ok(None) => {
                    // we don't have it, remove it
                    if committer.head_contains_file(&short_path)? {
                        log::debug!("REMOVE IT {:?}", file);
                        std::fs::remove_file(file)?;
                    }
                }
                Err(err) => {
                    log::error!("Error cleaning removed entries {}", err)
                }
            }
        }
        Ok(())
    }

    fn pull_entries(
        &self,
        committer: &Committer,
        db: &DBWithThreadMode<MultiThreaded>,
        entries: &PaginatedEntries,
        commit_id: &str,
        progress: &ProgressBar,
        page_num: usize,
        page_size: usize,
        limit: usize,
    ) -> Result<(), OxenError> {
        // Download all the files
        let elem_num = (page_num-1)*page_size;
        let mut num_to_take = entries.entries.len();
        log::debug!("pull_entries checking if we change num_to_take {}, {} >= {}", num_to_take, elem_num+num_to_take, limit);
        if elem_num+num_to_take >= limit && limit > elem_num {
            num_to_take = limit - elem_num;
        }

        log::debug!("pull_entries maybe stop: {} < {} = {} entries.entries.len({})", elem_num, limit, num_to_take, entries.entries.len());
        entries.entries[0..num_to_take].par_iter().for_each(|entry| {
            if elem_num < limit {
                if let Err(err) = self.download_remote_entry(committer, db, entry, commit_id) {
                    eprintln!("Could not download entry {:?} Err: {:?}", entry.filename, err);
                }
            }
            progress.inc(1);
        });

        if elem_num+num_to_take < limit {
            let next_page = page_num + 1;
            let entries = api::remote::entries::list_page(&self.repository, commit_id, next_page, page_size)?;
            self.pull_entries(committer, db, &entries, commit_id, progress, next_page, page_size, limit)?;
        }

        Ok(())
    }

    fn download_remote_entry(
        &self,
        committer: &Committer,
        db: &DBWithThreadMode<MultiThreaded>,
        entry: &RemoteEntry,
        commit_id: &str,
    ) -> Result<(), OxenError> {
        if self.repository.remote().is_none() {
            return Err(OxenError::basic_str("Must set remote"));
        }

        let config = AuthConfig::default()?;
        let fpath = committer.repository.path.join(&entry.filename);
        log::debug!("download_remote_entry entry {}", entry.filename);
        if !fpath.exists() || self.path_hash_is_different(entry, &fpath) {
            let remote = self.repository.remote().unwrap().value;
            let url = format!("{}/{}", remote, entry.filename);

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

        // Add to db, even if we do not need the file
        let commit_entry = CommitEntry::from_remote_and_commit_id(entry, commit_id);
        committer.add_commit_entry(&commit_entry, db)?;
        Ok(())
    }

    fn path_hash_is_different(&self, entry: &RemoteEntry, path: &Path) -> bool {
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
    use crate::index::{Indexer, Committer};
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
                let mut committer = Committer::new(&cloned_repo)?;
                let indexer = Indexer::new(&cloned_repo)?;

                // Pull a part of the commit
                let commits = command::log(&repo)?;
                let last_commit = commits.first().unwrap();
                let page_size = 2;
                let limit = page_size;
                indexer.pull_entries_for_commit_id(&committer, &last_commit.id, page_size, limit)?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                // try to pull the full thing again even though we have only partially pulled some
                indexer.pull(&mut committer)?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(og_num_files, num_files);

                Ok(())
            })
        })
    }

    #[test]
    fn test_indexer_partial_pull_odd_size() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|mut repo| {
            // Set the proper remote
            let remote = api::endpoint::repo_url_from(&repo.name);
            command::set_remote(&mut repo, constants::DEFAULT_ORIGIN_NAME, &remote)?;

            // Push it
            let remote_repo = command::push(&repo)?;

            command::push(&repo)?;

            test::run_empty_dir_test(|new_repo_dir| {
                let cloned_repo = command::clone(&remote_repo.url, new_repo_dir)?;
                let committer = Committer::new(&cloned_repo)?;
                let indexer = Indexer::new(&cloned_repo)?;

                // Pull a part of the commit
                let commits = command::log(&repo)?;
                let last_commit = commits.first().unwrap();
                let page_size = 3; // make sure it is not an even multiple of limit
                let limit = 8;
                indexer.pull_entries_for_commit_id(&committer, &last_commit.id, page_size, limit)?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

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
                let committer = Committer::new(&cloned_repo)?;
                let indexer = Indexer::new(&cloned_repo)?;

                // Pull a part of the commit
                let commits = command::log(&repo)?;
                let last_commit = commits.first().unwrap();
                let page_size = 3; // make sure it is not an even multiple of limit
                let limit = 7;
                indexer.pull_entries_for_commit_id(&committer, &last_commit.id, page_size, limit)?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                Ok(())
            })
        })
    }
}
