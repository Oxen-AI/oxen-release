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
            return Err(OxenError::basic_str("Must set remote on repository"));
        }

        let remote_repo = self.create_or_get_repo()?;
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

    pub fn pull(&self, committer: &mut Committer) -> Result<(), OxenError> {
        log::debug!("Oxen pull!");
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

                // Pull the commit
                self.rpull_commit_id(committer, &remote_head.commit.id)?;
            }
            Ok(None) => {
                log::debug!("oxen pull remote head does not exist");
            }
            Err(err) => {
                log::debug!("oxen pull could not get remote head: {}", err);
            }
        }

        Ok(())
    }

    fn rpull_commit_id(&self, committer: &mut Committer, commit_id: &str) -> Result<(), OxenError> {
        // Check if we have the local commit
        let local_commit = committer.get_commit_by_id(commit_id)?;
        if local_commit.is_none() {
            // If we don't have it locally

            // Recursively see if we need to sync the parent
            if let Ok(Some(parent)) =
                api::remote::commits::get_remote_parent(&self.repository, commit_id)
            {
                self.rpull_commit_id(committer, &parent.id)?;
            }

            // Get commit and write it to local DB
            let remote_commit = api::remote::commits::get_by_id(&self.repository, commit_id)?;
            log::debug!(
                "rpull_commit_id adding commit {} -> `{}`",
                remote_commit.id,
                remote_commit.message
            );
            committer.add_commit(&remote_commit)?;

            // Pull all the entry files for that commit
            self.pull_entries_for_commit_id(committer, commit_id)?;
        } else {
            log::debug!("we have local commit: {:?}", local_commit);
        }

        Ok(())
    }

    fn pull_entries_for_commit_id(
        &self,
        committer: &Committer,
        commit_id: &str,
    ) -> Result<(), OxenError> {
        log::debug!("🐂 pull_entries_for_commit_id commit_id {}", commit_id);
        let entries = api::remote::entries::first_page(&self.repository, commit_id)?;

        let total: usize = entries.total_entries;
        if total == 0 {
            return Ok(());
        }

        println!("🐂 pulling commit {} with {} entries", commit_id, total);
        let size: u64 = unsafe { std::mem::transmute(total) };
        let bar = ProgressBar::new(size);

        let commit_db_path = committer.history_dir.join(Path::new(&commit_id));
        let opts = Committer::db_opts();
        let db = DBWithThreadMode::open(&opts, &commit_db_path)?;

        // Pull and write all the entries
        self.pull_entries(committer, &db, &entries, commit_id, &bar, 1)?;

        // Cleanup files that shouldn't be there
        self.cleanup_removed_entries(committer, &db)?;

        bar.finish();

        Ok(())
    }

    fn cleanup_removed_entries(
        &self,
        committer: &Committer,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        for file in util::fs::rlist_files_in_dir(&self.repository.path).iter() {
            let short_path = util::fs::path_relative_to_dir(file, &self.repository.path)?;
            log::debug!("Checking if we should remove: {:?}", short_path);
            let key = short_path.to_str().unwrap().as_bytes();
            match db.get(key) {
                Ok(Some(_value)) => {
                    // we have it, keep it
                    log::debug!("Keep file: {:?}", file);
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
    ) -> Result<(), OxenError> {
        // Download all the files
        for entry in entries.entries.iter() {
            self.download_remote_entry(committer, db, entry, commit_id)?;
            progress.inc(1);
        }

        if page_num < entries.total_pages {
            let next_page = page_num + 1;
            let entries = api::remote::entries::nth_page(&self.repository, commit_id, next_page)?;
            self.pull_entries(committer, db, &entries, commit_id, progress, next_page)?;
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
    // use crate::command;
    // use crate::error::OxenError;
    // use crate::index::Indexer;
    // use crate::test;

    // #[test]
    // fn test_indexer_post_commit_to_server() -> Result<(), OxenError> {
    //     test::run_training_data_repo_test_no_commits(|repo| {

    //         Ok(())
    //     })
    // }
}
