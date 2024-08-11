//! # Commits
//!
//! Create, read, and list commits
//!

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::opts::PaginateOpts;
use crate::util;
use crate::view::{PaginatedCommits, StatusMessage};

use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Commit the data that is staged in the repository
pub fn commit(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commit::commit(repo, message),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commit::commit(repo, message),
    }
}

/// Iterate over all commits and get the one with the latest timestamp
pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commit::latest_commit(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commit::latest_commit(repo),
    }
}

/// The current HEAD commit of the branch you currently have checked out
pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commit::head_commit(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commit::head_commit(repo),
    }
}

/// Get the root commit of a repository
pub fn root_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commit::root_commit(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commit::root_commit(repo),
    }
}

/// Get a commit by it's hash
pub fn get_by_id(repo: &LocalRepository, commit_id: &str) -> Result<Option<Commit>, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commit::get_by_id(repo, commit_id),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commit::get_by_id(repo, commit_id),
    }
}

/// List commits on the current branch from HEAD
pub fn list(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commit::list(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commit::list(repo),
    }
}

/// List commits for the repository in no particular order
pub fn list_all(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commit::list_all(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commit::list_all(repo),
    }
}

pub fn list_all_paginated(
    repo: &LocalRepository,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    let commits = list_all(repo)?;
    let (commits, pagination) = util::paginate(commits, pagination.page_num, pagination.page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}

/// List the history for a specific branch or commit (revision)
pub fn list_from(repo: &LocalRepository, revision: &str) -> Result<Vec<Commit>, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commit::list_from(repo, revision),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commit::list_from(repo, revision),
    }
}

/// Get a list commits by the commit message
pub fn get_by_message(
    repo: &LocalRepository,
    msg: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    let commits = list_all(repo)?;
    let filtered: Vec<Commit> = commits
        .into_iter()
        .filter(|commit| commit.message == msg.as_ref())
        .collect();
    Ok(filtered)
}

/// Get the most recent commit by the commit message, starting at the HEAD commit
pub fn first_by_message(
    repo: &LocalRepository,
    msg: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let commits = list(repo)?;
    Ok(commits
        .into_iter()
        .find(|commit| commit.message == msg.as_ref()))
}

/// List all the commits that have missing entries
/// Useful for knowing which commits to resend
pub fn list_with_missing_entries(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::commit::list_with_missing_entries(repo, commit_id)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::commit::list_with_missing_entries(repo, commit_id)
        }
    }
}

/// Retrieve entries with filepaths matching a provided glob pattern
pub fn search_entries(
    repo: &LocalRepository,
    commit: &Commit,
    pattern: &str,
) -> Result<HashSet<PathBuf>, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commit::search_entries(repo, commit, pattern),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commit::search_entries(repo, commit, pattern),
    }
}

/// List paginated commits starting from the given revision
pub fn list_from_paginated(
    repo: &LocalRepository,
    revision: &str,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    let commits = list_from(repo, revision)?;
    let (commits, pagination) = util::paginate(commits, pagination.page_num, pagination.page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}

/// List paginated commits by resource
pub fn list_by_path_from_paginated(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    match repo.version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::commit::list_by_path_from_paginated(repo, commit, path, pagination)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::commit::list_by_path_from_paginated(repo, commit, path, pagination)
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO: Test the commits modules...
}
