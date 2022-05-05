use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::Path;
use std::sync::Arc;

use crate::api;
use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{Commit, CommitEntry, CommitHead, LocalRepository, RemoteRepository};
use crate::view::{CommitResponse, RemoteRepositoryHeadResponse};

pub struct Indexer {
    pub repository: LocalRepository,
}

impl Indexer {
    pub fn new(repository: &LocalRepository) -> Result<Indexer, OxenError> {
        Ok(Indexer {
            repository: repository.clone(),
        })
    }

    pub fn create_or_get_repo(&self) -> Result<(), OxenError> {
        let name = &self.repository.name;
        api::remote::repositories::create_or_get_repo(name)
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
        let remote_repo = RemoteRepository::from_local(&self.repository)?;
        match api::remote::entries::create(&remote_repo, entry) {
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

    pub fn push(&self, committer: &Arc<Committer>) -> Result<(), OxenError> {
        self.create_or_get_repo()?;
        match committer.get_head_commit() {
            Ok(Some(commit)) => {
                // maybe_push() will recursively check commits head against remote head
                // and sync ones that have not been synced
                let remote_head = self.get_remote_head()?;
                self.maybe_push(committer, &remote_head, &commit.id, 0)?;
                Ok(())
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
            if commit_id == head.commit_id {
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
            self.post_commit_to_server(&commit)?;
            self.push_entries(committer, &commit)?;
        } else {
            eprintln!("Err: could not find commit: {}", commit_id);
        }

        Ok(())
    }

    pub fn get_remote_head(&self) -> Result<Option<CommitHead>, OxenError> {
        // TODO move into another api class, need to better delineate what we call these
        // also is this remote the one in the config? I think so, need to draw out a diagram
        let name = &self.repository.name;
        let url = format!("http://0.0.0.0:3000/repositories/{}", name);
        let client = reqwest::blocking::Client::new();
        if let Ok(res) = client.get(url).send() {
            // TODO: handle if remote repo does not exist...
            // Do we create it then push for now? Or add separate command to create?
            // I think we create and push, and worry about authorized keys etc later
            let body = res.text()?;
            let response: Result<RemoteRepositoryHeadResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(j_res) => Ok(j_res.head),
                Err(err) => Err(OxenError::basic_str(&format!(
                    "get_remote_head() Could not serialize response [{}]\n{}",
                    err, body
                ))),
            }
        } else {
            Err(OxenError::basic_str("get_remote_head() Request failed"))
        }
    }

    pub fn post_commit_to_server(&self, commit: &Commit) -> Result<CommitResponse, OxenError> {
        // zip up the rocksdb in history dir, and post to server
        let commit_dir = Committer::history_dir(&self.repository.path).join(commit.id.clone());
        // This will be the subdir within the tarball
        let tar_subdir = Path::new("history").join(commit.id.clone());

        println!("Compressing commit {}", commit.id);
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);

        tar.append_dir_all(&tar_subdir, commit_dir)?;
        tar.finish()?;

        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        self.post_tarball_to_server(&buffer, commit)
    }

    fn post_tarball_to_server(
        &self,
        buffer: &[u8],
        commit: &Commit,
    ) -> Result<CommitResponse, OxenError> {
        println!("Syncing commit {}...", commit.id);

        let name = &self.repository.name;
        let client = reqwest::blocking::Client::new();
        let url = format!(
            "http://0.0.0.0:3000/repositories/{}/commits?{}",
            name,
            commit.to_uri_encoded()
        );
        if let Ok(res) = client
            .post(url)
            .body(reqwest::blocking::Body::from(buffer.to_owned()))
            .send()
        {
            let status = res.status();
            let body = res.text()?;
            let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(_) => Err(OxenError::basic_str(&format!(
                    "Error serializing CommitResponse: status_code[{}] \n\n{}",
                    status, body
                ))),
            }
        } else {
            Err(OxenError::basic_str(
                "post_tarball_to_server error sending data from file",
            ))
        }
    }

    pub fn pull(&self) -> Result<(), OxenError> {
        // Get list of commits we have to pull

        // For each commit
        // - pull dbs
        // - pull entries given the db

        let total: usize = 0;
        println!("🐂 pulling {} entries", total);
        let size: u64 = unsafe { std::mem::transmute(total) };
        let bar = ProgressBar::new(size);

        bar.finish();
        Ok(())
    }

    /*
    fn download_url(
        &self,
        entry: &crate::model::Entry,
    ) -> Result<(), OxenError> {
        let fname = path.join(&entry.filename);
        // println!("Downloading file {:?}", &fname);
        if !fname.exists() {
            let mut response = reqwest::blocking::get(&entry.url)?;
            let mut dest = { File::create(fname)? };
            response.copy_to(&mut dest)?;
        }
        Ok(())
    }
    */
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::Indexer;
    use crate::test;

    #[test]
    fn test_indexer_post_commit_to_server() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Track the annotations dir
            // has format
            //   annotations/
            //     train/
            //       one_shot.txt
            //       annotations.txt
            //     test/
            //       annotations.txt
            let annotations_dir = repo.path.join("annotations");
            command::add(&repo, &annotations_dir)?;
            // Commit the file
            let commit =
                command::commit(&repo, "Adding annotations data dir, which has two levels")?;
            assert!(commit.is_some());
            let commit = commit.unwrap();

            let indexer = Indexer::new(&repo)?;
            // Create repo on the server
            indexer.create_or_get_repo()?;

            // Post commit
            let result_commit = indexer.post_commit_to_server(&commit)?;
            assert_eq!(result_commit.commit.id, commit.id);

            Ok(())
        })
    }
}
