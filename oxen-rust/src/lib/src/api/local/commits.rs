use crate::error::OxenError;
use crate::index::CommitReader;
use crate::model::{Commit, LocalRepository};

pub fn get_by_id(repo: &LocalRepository, commit_id: &str) -> Result<Option<Commit>, OxenError> {
    let reader = CommitReader::new(repo)?;
    reader.get_commit_by_id(commit_id)
}

pub fn get_head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let committer = CommitReader::new(repo)?;
    committer.head_commit()
}

pub fn get_parent(repo: &LocalRepository, commit: &Commit) -> Result<Option<Commit>, OxenError> {
    let committer = CommitReader::new(repo)?;
    if let Some(parent_id) = &commit.parent_id {
        committer.get_commit_by_id(parent_id)
    } else {
        Ok(None)
    }
}
