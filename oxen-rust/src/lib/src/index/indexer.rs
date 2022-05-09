use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::sync::Arc;
use std::path::Path;
use std::fs::File;

use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{
    Commit,
    CommitEntry,
    CommitHead,
    RemoteEntry,
    LocalRepository,
    RemoteRepository
};

use crate::view::{
    PaginatedEntries
};

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

    fn push_entries(&self, committer: &Arc<Committer>, commit: &Commit) -> Result<(), OxenError> {
        let entries = committer.list_unsynced_entries_for_commit(commit)?;

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
        committer: &Arc<Committer>,
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

    pub fn push(&self, committer: &Arc<Committer>) -> Result<RemoteRepository, OxenError> {
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
        committer: &Arc<Committer>,
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
        log::debug!("Oxen pull!");
        // Get the remote head commit, and try to recursively pull subsequent commits
        if let Some(remote_head) = api::remote::commits::get_remote_head(&self.repository)? {
            log::debug!("Oxen pull got remote head: `{}`", remote_head.commit.message);
            self.rpull_commit_id(&remote_head.commit.id)?;
        } else {
            log::debug!("Oxen pull Could not get remote head...");
        }

        Ok(())
    }

    fn rpull_commit_id(&self, commit_id: &str) -> Result<(), OxenError> {
        // Check if we have the local commit
        let local_commit = api::local::commits::get_by_id(&self.repository, &commit_id)?;
        if local_commit.is_none() {
            // If we don't have it locally, we have to pull dbs and entries
            self.pull_dbs_for_commit_id(commit_id)?;
            self.pull_entries_for_commit_id(commit_id)?;

            // Then recursively see if we need to sync the parent
            if let Some(parent) = api::remote::commits::get_remote_parent(&self.repository, commit_id)? {
                self.rpull_commit_id(&parent.commit.id)?;
            }
        }

        Ok(())
    }

    fn pull_dbs_for_commit_id(&self, commit_id: &str) -> Result<(), OxenError> {
        log::error!("TODO: pull_dbs_for_commit_id {}", commit_id);
        Ok(())
    }

    fn pull_entries_for_commit_id(&self, commit_id: &str) -> Result<(), OxenError> {
        let entries = api::remote::entries::first_page(&self.repository, commit_id)?;

        let total: usize = entries.total_entries;
        println!("🐂 pulling commit {} with {} entries", commit_id, total);
        let size: u64 = unsafe { std::mem::transmute(total) };
        let bar = ProgressBar::new(size);

        self.pull_entries(&entries, commit_id, &bar, 1)?;

        bar.finish();

        Ok(())
    }

    fn pull_entries(&self, entries: &PaginatedEntries, commit_id: &str, progress: &ProgressBar, page_num: usize) -> Result<(), OxenError> {
        // Download all the files
        for entry in entries.entries.iter() {
            self.download_remote_entry(entry)?;
            progress.inc(1);
        }

        
        if page_num < entries.total_pages {
            let next_page = page_num + 1;
            let entries = api::remote::entries::nth_page(&self.repository, commit_id, next_page)?;
            self.pull_entries(&entries, commit_id, progress, next_page)?;
        }

        Ok(())
    }


    fn download_remote_entry(
        &self,
        entry: &RemoteEntry,
    ) -> Result<(), OxenError> {
        let config = AuthConfig::default()?;
        let fpath = Path::new(&entry.filename);
        // println!("Downloading file {:?}", &fname);
        if !fpath.exists() {
            let remote = self.repository.remote().unwrap().value;
            let url = format!("{}/{}/{}", remote, self.repository.name, entry.filename);

            let client = reqwest::blocking::Client::new();
            let mut response = client.get(&url)
                .header(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {}", config.auth_token()),
                )
                .send()?;
            let mut dest = { File::create(fpath)? };
            response.copy_to(&mut dest)?;
        }
        Ok(())
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
