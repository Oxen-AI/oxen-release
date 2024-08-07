//! Revisions can either be commits by id or head commits on branches by name

use std::path::{Path, PathBuf};

use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_10_0::index::CommitDirEntryReader;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;
use crate::util;

/// Get a commit object from a commit id or branch name
/// Returns Ok(None) if the revision does not exist
pub fn get(repo: &LocalRepository, revision: impl AsRef<str>) -> Result<Option<Commit>, OxenError> {
    let revision = revision.as_ref();
    if repositories::branches::exists(repo, revision)? {
        let branch = repositories::branches::get_by_name(repo, revision)?;
        let branch = branch.ok_or(OxenError::local_branch_not_found(revision))?;
        let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?;
        Ok(commit)
    } else {
        let commit = repositories::commits::get_by_id(repo, revision)?;
        Ok(commit)
    }
}

/// Get the version file path from a commit id
pub fn get_version_file(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let commit_id = match get(repo, &revision)? {
        Some(commit) => commit.id,
        None => return Err(OxenError::commit_id_does_not_exist(revision.as_ref())),
    };

    get_version_file_from_commit_id(repo, commit_id, path)
}

/// Get the version file path from a commit id
pub fn get_version_file_from_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let commit_id = commit_id.as_ref();
    let path = path.as_ref();
    let parent = match path.parent() {
        Some(parent) => parent,
        None => return Err(OxenError::file_has_no_parent(path)),
    };

    let object_reader = get_object_reader(repo, commit_id)?;

    // Instantiate CommitDirEntryReader to fetch entry
    let relative_parent = util::fs::path_relative_to_dir(parent, &repo.path)?;
    let commit_entry_reader =
        CommitDirEntryReader::new(repo, commit_id, &relative_parent, object_reader)?;
    let file_name = match path.file_name() {
        Some(file_name) => file_name,
        None => return Err(OxenError::file_has_no_name(path)),
    };

    let entry = match commit_entry_reader.get_entry(file_name) {
        Ok(Some(entry)) => entry,
        _ => return Err(OxenError::entry_does_not_exist_in_commit(path, commit_id)),
    };

    Ok(util::fs::version_path(repo, &entry))
}
