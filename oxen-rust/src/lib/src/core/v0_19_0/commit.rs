use std::collections::HashSet;
use std::path::Path;

use crate::core::refs::RefReader;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::opts::PaginateOpts;
use crate::repositories;
use crate::view::PaginatedCommits;
use std::path::PathBuf;
use std::str;

use super::index::merkle_tree::CommitMerkleTree;

pub fn commit(repo: &LocalRepository, message: impl AsRef<str>) -> Result<Commit, OxenError> {
    super::index::commit_writer::commit(repo, message)
}

pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branches = ref_reader.list_branches()?;
    let mut latest_commit: Option<Commit> = None;
    for branch in branches {
        let commit = get_by_id(repo, &branch.commit_id)?;
        if let Some(commit) = commit {
            if latest_commit.is_some()
                && commit.timestamp < latest_commit.as_ref().unwrap().timestamp
            {
                latest_commit = Some(commit);
            } else if latest_commit.is_none() {
                latest_commit = Some(commit);
            }
        }
    }
    latest_commit.ok_or(OxenError::basic_str("No commits found".to_string()))
}

fn head_commit_id(repo: &LocalRepository) -> Result<u128, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    match ref_reader.head_commit_id() {
        Ok(Some(commit_id)) => uhash_from_str(&commit_id),
        Ok(None) => Err(OxenError::head_not_found()),
        Err(err) => Err(err),
    }
}

pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let head_commit_id = head_commit_id(repo)?;
    let commit_data = CommitMerkleTree::read_node(repo, head_commit_id, false)?;
    let commit = commit_data.commit()?;
    Ok(commit.to_commit())
}

pub fn root_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let commit_id = head_commit_id(repo)?;
    root_commit_recursive(repo, commit_id)
}

fn root_commit_recursive(repo: &LocalRepository, commit_id: u128) -> Result<Commit, OxenError> {
    if let Some(commit) = get_by_uid(repo, commit_id)? {
        if commit.parent_ids.is_empty() {
            return Ok(commit);
        }

        for parent_id in commit.parent_ids {
            let parent_id = uhash_from_str(&parent_id)?;
            root_commit_recursive(repo, parent_id)?;
        }
    }
    return Err(OxenError::basic_str("No root commit found".to_string()));
}

pub fn get_by_id(
    repo: &LocalRepository,
    commit_id_str: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let commit_id_str = commit_id_str.as_ref();
    let commit_id = uhash_from_str(commit_id_str)?;
    get_by_uid(repo, commit_id)
}

pub fn get_by_uid(repo: &LocalRepository, uid: u128) -> Result<Option<Commit>, OxenError> {
    let commit_data = CommitMerkleTree::read_node(repo, uid, false)
        .map_err(|_| OxenError::revision_not_found(str_from_uhash(uid).into()))?;
    let commit = commit_data.commit()?;
    Ok(Some(commit.to_commit()))
}

/// List commits on the current branch from HEAD
pub fn list(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    let mut results = vec![];
    let commit = head_commit(repo)?;
    list_recursive(repo, commit, &mut results)?;
    Ok(results)
}

fn list_recursive(
    repo: &LocalRepository,
    commit: Commit,
    results: &mut Vec<Commit>,
) -> Result<(), OxenError> {
    results.push(commit.clone());
    for parent_id in commit.parent_ids {
        let parent_id = uhash_from_str(&parent_id)?;
        if let Some(parent_commit) = get_by_uid(repo, parent_id)? {
            list_recursive(repo, parent_commit, results)?;
        }
    }
    Ok(())
}

/// List commits for the repository in no particular order
pub fn list_all(repo: &LocalRepository) -> Result<HashSet<Commit>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branches = ref_reader.list_branches()?;
    let mut commits = HashSet::new();
    for branch in branches {
        let commit = get_by_id(repo, &branch.commit_id)?;
        if let Some(commit) = commit {
            list_all_recursive(repo, commit, &mut commits)?;
        }
    }
    Ok(commits)
}

fn list_all_recursive(
    repo: &LocalRepository,
    commit: Commit,
    commits: &mut HashSet<Commit>,
) -> Result<(), OxenError> {
    commits.insert(commit.clone());
    for parent_id in commit.parent_ids {
        let parent_id = uhash_from_str(&parent_id)?;
        if let Some(parent_commit) = get_by_uid(repo, parent_id)? {
            list_all_recursive(repo, parent_commit, commits)?;
        }
    }
    Ok(())
}

/// Get commit history given a revision (branch name or commit id)
pub fn list_from(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    let mut results = vec![];
    let commit = repositories::revisions::get(repo, revision)?;
    if let Some(commit) = commit {
        list_recursive(repo, commit, &mut results)?;
    }
    Ok(results)
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

pub fn uhash_from_str(commit_id: &str) -> Result<u128, OxenError> {
    let commit_id = u128::from_str_radix(&commit_id, 16)
        .map_err(|_| OxenError::basic_str(format!("Invalid commit id: {}", commit_id)))?;
    Ok(commit_id)
}

pub fn str_from_uhash(commit_id: u128) -> String {
    format!("{:x}", commit_id)
}
