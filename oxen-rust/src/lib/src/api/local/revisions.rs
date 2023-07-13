//! Revisions can either be commits by id or head commits on branches by name

use crate::api;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};

/// Get a commit object from a commit id or branch name
/// Returns Ok(None) if the revision does not exist
pub fn get(repo: &LocalRepository, revision: impl AsRef<str>) -> Result<Option<Commit>, OxenError> {
    let revision = revision.as_ref();
    if api::local::branches::exists(repo, revision)? {
        let branch = api::local::branches::get_by_name(repo, revision)?;
        let branch = branch.ok_or(OxenError::local_branch_not_found(revision))?;
        let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?;
        Ok(commit)
    } else {
        let commit = api::local::commits::get_by_id(repo, revision)?;
        Ok(commit)
    }
}
