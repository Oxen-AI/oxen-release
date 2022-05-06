use crate::index::Committer;
use crate::model::{Commit, CommitEntry, LocalRepository};

use crate::error::OxenError;

pub fn list_all(repo: &LocalRepository, commit: &Commit) -> Result<Vec<CommitEntry>, OxenError> {
    let committer = Committer::new(repo)?;
    let entries = committer.list_entries_for_commit(commit)?;
    Ok(entries)
}
