use crate::error::OxenError;
use crate::model::{LocalRepository, Commit, CommitEntry};

use std::path::Path;

pub fn list_entry_versions_for_commit(
    local_repo: &LocalRepository,
    commit_id: &str,
    path: &Path,
) -> Result<Vec<(Commit, CommitEntry)>, OxenError> {
    todo!()
}

pub async fn checkout(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    todo!()
}

pub async fn checkout_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<(), OxenError> {
    todo!()
}