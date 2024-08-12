use std::collections::HashSet;
use std::path::Path;

use std::path::PathBuf;
use std::str;
use crate::core::v0_10_0::index::RefReader;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::opts::PaginateOpts;
use crate::view::PaginatedCommits;

use super::index::merkle_tree::CommitMerkleTree;

pub fn commit(
    repo: &LocalRepository,
    message: impl AsRef<str>
) -> Result<Commit, OxenError> {
    super::index::commit_writer::commit(repo, message)
}

pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    todo!()
}

pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    match ref_reader.head_commit_id() {
        Ok(Some(commit_id)) => {
            let commit_id = u128::from_str_radix(&commit_id, 16).unwrap();
            let commit_data = CommitMerkleTree::read_node(repo, commit_id, false)?;
            let commit = commit_data.commit()?;
            Ok(commit.to_commit())
        }
        Ok(None) => Err(OxenError::head_not_found()),
        Err(err) => Err(err),
    }
}

pub fn root_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    todo!()
}

pub fn get_by_id(repo: &LocalRepository, commit_id: &str) -> Result<Option<Commit>, OxenError> {
    todo!()
}

/// List commits on the current branch from HEAD
pub fn list(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    todo!()
}

/// List commits for the repository in no particular order
pub fn list_all(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    todo!()
}

/// Get commit history given a revision (branch name or commit id)
pub fn list_from(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    todo!()
}

/// List all the commits that have missing entries
/// Useful for knowing which commits to resend
pub fn list_with_missing_entries(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    todo!()
}

/// Retrieve entries with filepaths matching a provided glob pattern
pub fn search_entries(
    repo: &LocalRepository,
    commit: &Commit,
    pattern: impl AsRef<str>,
) -> Result<HashSet<PathBuf>, OxenError> {
    todo!()
}

/// Get paginated list of commits by path (directory or file)
pub fn list_by_path_from_paginated(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    todo!()
}
