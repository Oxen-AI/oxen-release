use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{Commit, LocalRepository};

pub fn get_by_id(repo: &LocalRepository, commit_id: &str) -> Result<Option<Commit>, OxenError> {
    let committer = Committer::new(repo)?;
    let commit = committer.get_commit_by_id(commit_id)?;
    Ok(commit)
}
