//! Caches the size of the repo to disk at the time of the commit, so that we can quickly query it

use fs_extra::dir::get_size;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::api;
use crate::constants::{CACHE_DIR, DIRS_DIR, HISTORY_DIR};
use crate::core::index::{CommitDirEntryReader, CommitEntryReader, CommitReader, ObjectDBReader};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;

pub fn repo_size_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("repo_size.txt")
}

pub fn dir_size_path(repo: &LocalRepository, commit: &Commit, dir: &Path) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join(DIRS_DIR)
        .join(dir)
        .join("size.txt")
}

pub fn dir_latest_commit_path(repo: &LocalRepository, commit: &Commit, dir: &Path) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join(DIRS_DIR)
        .join(dir)
        .join("latest_commit.txt")
}

// TODO: Refactor each one of these computes into a configurable cache
// 1) Compute the size of the repo at the time of the commit
// 2) Compute the size of each directory at time of commit
// 3) Compute the latest commit that modified each directory
pub fn compute(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!(
        "Running compute_repo_size on {:?} for commit {}",
        repo.path,
        commit.id
    );

    // List directories in the repo, and cache all of their entry sizes
    let reader = CommitEntryReader::new(repo, commit)?;
    let commit_reader = CommitReader::new(repo)?;
    let dirs = reader.list_dirs()?;
    log::debug!("Computing size of {} dirs", dirs.len());
    let object_reader = ObjectDBReader::new(repo)?;
    for dir in dirs {
        // log::debug!("REPO_SIZE PROCESSING DIR {dir:?}");

        // Start with the size of all the entries in this dir
        let dir_reader = CommitDirEntryReader::new(repo, &commit.id, &dir, &object_reader).unwrap();
        let entries = dir_reader.list_entries().unwrap();
        let mut total_size = api::local::entries::compute_entries_size(&entries).unwrap();

        // For each dir, find the latest commit that modified it
        let commits: HashMap<String, Commit> = HashMap::new();
        let mut latest_commit: Option<Commit> = None;

        // TODO: do not copy pasta this code
        for entry in entries {
            let commit = if commits.contains_key(&entry.commit_id) {
                Some(commits[&entry.commit_id].clone())
            } else {
                commit_reader.get_commit_by_id(&entry.commit_id)?
            };

            if latest_commit.is_none() {
                // log::debug!(
                //     "FOUND LATEST COMMIT PARENT EMPTY {:?} -> {:?}",
                //     entry.path,
                //     commit
                // );
                latest_commit = commit.clone();
            } else {
                // log::debug!(
                //     "CONSIDERING COMMIT PARENT TIMESTAMP {:?} {:?} < {:?}",
                //     entry.path,
                //     latest_commit.as_ref().unwrap().timestamp,
                //     commit.as_ref().unwrap().timestamp
                // );
                if latest_commit.as_ref().unwrap().timestamp < commit.as_ref().unwrap().timestamp {
                    // log::debug!(
                    //     "FOUND LATEST COMMIT PARENT TIMESTAMP {:?} -> {:?}",
                    //     entry.path,
                    //     commit
                    // );
                    latest_commit = commit.clone();
                }
            }
        }

        // Recursively compute the size of the directory children
        let children = reader.list_dir_children(&dir)?;
        let object_reader = ObjectDBReader::new(repo)?;
        for child in children {
            // log::debug!("REPO_SIZE PROCESSING CHILD {child:?}");

            let dir_reader = CommitDirEntryReader::new(repo, &commit.id, &child, &object_reader).unwrap();

            let entries = dir_reader.list_entries().unwrap();
            let size = api::local::entries::compute_entries_size(&entries).unwrap();

            total_size += size;

            for entry in entries {
                let commit = if commits.contains_key(&entry.commit_id) {
                    Some(commits[&entry.commit_id].clone())
                } else {
                    commit_reader.get_commit_by_id(&entry.commit_id)?
                };

                if latest_commit.is_none() {
                    // log::debug!("FOUND LATEST COMMIT CHILD EMPTY {:?} -> {:?}", entry.path, commit);
                    latest_commit = commit.clone();
                } else {
                    // log::debug!("CONSIDERING COMMIT PARENT TIMESTAMP {:?} {:?} < {:?}", entry.path, latest_commit.as_ref().unwrap().timestamp, commit.as_ref().unwrap().timestamp);
                    if latest_commit.as_ref().unwrap().timestamp
                        < commit.as_ref().unwrap().timestamp
                    {
                        // log::debug!("FOUND LATEST COMMIT PARENT TIMESTAMP {:?} -> {:?}", entry.path, commit);
                        latest_commit = commit.clone();
                    }
                }
            }
        }

        let size_str = total_size.to_string();
        let size_path = dir_size_path(repo, commit, &dir);
        log::debug!("Writing dir size {} to {:?}", size_str, size_path);
        // create parent directory if not exists
        if let Some(parent) = size_path.parent() {
            util::fs::create_dir_all(parent)?;
        }
        util::fs::write_to_path(&size_path, &size_str)?;

        let latest_commit_path = dir_latest_commit_path(repo, commit, &dir);
        if let Some(latest_commit) = latest_commit {
            log::debug!(
                "Writing latest commit {} to {:?}",
                latest_commit.id,
                latest_commit_path
            );
            // create parent directory if not exists
            if let Some(parent) = latest_commit_path.parent() {
                util::fs::create_dir_all(parent)?;
            }
            util::fs::write_to_path(&latest_commit_path, &latest_commit.id)?;
        }
    }

    // Cache the full size of the repo
    log::debug!("Computing size of repo {:?}", repo.path);
    match get_size(&repo.path) {
        Ok(size) => {
            log::debug!("Repo size for {:?} is {}", repo.path, size);
            write_repo_size(repo, commit, &size.to_string())?;
        }
        Err(e) => {
            // If we can't get the size, we'll just write an error message to the file
            // When we try to deserialize the file as a u64, we'll get an error and be able to return it
            let error_str = format!("Failed to get repo size: {}", e);
            write_repo_size(repo, commit, &error_str)?;
        }
    }

    Ok(())
}

fn write_repo_size(repo: &LocalRepository, commit: &Commit, val: &str) -> Result<(), OxenError> {
    let hash_file_path = repo_size_path(repo, commit);
    log::debug!("Writing repo size {} to {:?}", val, hash_file_path);
    util::fs::write_to_path(&hash_file_path, val)?;
    Ok(())
}
