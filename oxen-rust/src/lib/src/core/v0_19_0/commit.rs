use std::collections::HashSet;
use std::path::Path;

use std::path::PathBuf;

use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::opts::PaginateOpts;
use crate::view::PaginatedCommits;

pub fn commit(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    todo!()
}

pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    todo!()
}

pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    todo!()
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
