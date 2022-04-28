use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use serde_json::json;
use std::path::Path;
use std::sync::Arc;

use crate::api;
use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{Commit, CommitHead, LocalRepository, RemoteRepository};
use crate::util;
use crate::view::{CommitResponse, RemoteRepositoryHeadResponse, RepositoryResponse};

pub struct Indexer {
    pub repository: LocalRepository,
}

impl Indexer {
    pub fn new(repository: &LocalRepository) -> Result<Indexer, OxenError> {
        Ok(Indexer {
            repository: repository.clone(),
        })
    }

    fn push_entries(&self, committer: &Arc<Committer>, commit: &Commit) -> Result<(), OxenError> {
        let paths = committer.list_unsynced_files_for_commit(commit)?;

        println!("🐂 push {} files", paths.len());

        // len is usize and progressbar requires u64, I don't think we'll overflow...
        let size: u64 = unsafe { std::mem::transmute(paths.len()) };
        let bar = ProgressBar::new(size);

        let commit_db = &committer.head_commit_db;
        paths.par_iter().for_each(|path| {
            match self.hash_and_push(committer, commit_db, path) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("Error pushing entry {:?} Err {}", path, err)
                }
            }
            bar.inc(1);
        });

        bar.finish();

        Ok(())
    }

    fn hash_and_push(
        &self,
        committer: &Arc<Committer>,
        db: &Option<DBWithThreadMode<MultiThreaded>>,
        path: &Path,
    ) -> Result<(), OxenError> {
        // hash the file
        // find the entry in the history commit db
        // compare it to the last hash
        // TODO: if it is different, upload it, and mark it as being changed?
        //       maybe on the server we make a linked list of the changes with the commit id?
        // if it is the same, don't re-upload
        // Update the hash for this specific commit for this path
        if let Ok(hash) = util::hasher::hash_file_contents(path) {
            match util::fs::path_relative_to_dir(path, &self.repository.path) {
                Ok(path) => {
                    // Compare last hash to new one
                    let old_hash = committer.get_path_hash(db, &path).unwrap();
                    if old_hash == hash {
                        // we don't need to upload if hash is the same
                        // println!("Hash is the same! don't upload again {:?}", path);
                        return Ok(());
                    }

                    // Upload entry to server
                    let remote_repo = RemoteRepository::from_local(&self.repository)?;
                    match api::remote::entries::create(&remote_repo, &path, &hash) {
                        Ok(_entry) => {
                            // The last thing we do is update the hash in the local db
                            // after it has been posted to the server, so that even if the process
                            // is killed, and we don't get here, the worst thing that can happen
                            // is we re-upload it.
                            match committer.update_path_hash(db, &path, &hash) {
                                Ok(_) => {
                                    // println!("Updated hash! {:?} => {}", path, hash);
                                    Ok(())
                                }
                                Err(err) => {
                                    let err = format!(
                                        "Error updating hash path: {:?} Err: {}",
                                        path, err
                                    );
                                    Err(OxenError::basic_str(&err))
                                }
                            }
                        }
                        Err(err) => {
                            let err = format!("Error uploading {:?} {}", path, err);
                            Err(OxenError::basic_str(&err))
                        }
                    }
                }
                Err(err) => {
                    let err = format!("Could not get relative path... Err: {}", err);
                    Err(OxenError::basic_str(&err))
                }
            }
        } else {
            let err = format!("Error computing hash for path: {:?}", path);
            Err(OxenError::basic_str(&err))
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

    pub fn create_or_get_repo(&self) -> Result<(), OxenError> {
        // TODO move into another api class, and better error handling...just cranking this out
        let name = &self.repository.name;
        let url = "http://0.0.0.0:3000/repositories".to_string();
        let params = json!({ "name": name });

        let client = reqwest::blocking::Client::new();
        if let Ok(res) = client.post(url).json(&params).send() {
            let body = res.text()?;
            let response: Result<RepositoryResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(_) => Ok(()),
                Err(_) => Ok(()), // we are just assuming this error is already exists for now
            }
        } else {
            Err(OxenError::basic_str(
                "create_or_get_repo() Could not create repo",
            ))
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

    pub fn post_commit_to_server(&self, commit: &Commit) -> Result<(), OxenError> {
        // zip up the rocksdb in history dir, and post to server
        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
        let commit_dir = hidden_dir.join(&commit.id);
        let path_to_compress = format!("history/{}", commit.id);

        println!("Compressing commit {}...", commit.id);
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);

        tar.append_dir_all(path_to_compress, commit_dir)?;
        tar.finish()?;
        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        self.post_tarball_to_server(&buffer, commit)?;

        Ok(())
    }

    fn post_tarball_to_server(&self, buffer: &[u8], commit: &Commit) -> Result<(), OxenError> {
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
                Ok(_) => Ok(()),
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
    use crate::error::OxenError;
    // use crate::index::Indexer;
    // use crate::model::Repository;
    // use crate::test;
    // use crate::util;

    // const BASE_DIR: &str = "data/test/runs";

    #[test]
    fn test_indexer_push() -> Result<(), OxenError> {
        Ok(())
    }
}
