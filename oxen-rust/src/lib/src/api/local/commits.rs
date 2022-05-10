use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{Commit, LocalRepository};

pub fn get_by_id(repo: &LocalRepository, commit_id: &str) -> Result<Option<Commit>, OxenError> {
    let committer = Committer::new(repo)?;
    committer.get_commit_by_id(commit_id)
}

pub fn get_head_commit(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    let committer = Committer::new(repo)?;
    committer.get_head_commit()
}

pub fn get_parent(repo: &LocalRepository, commit: &Commit) -> Result<Option<Commit>, OxenError> {
    let committer = Committer::new(repo)?;
    if let Some(parent_id) = &commit.parent_id {
        committer.get_commit_by_id(parent_id)
    } else {
        Ok(None)
    }
}
