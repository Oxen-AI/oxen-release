//! Computes metadata we can extract from the entry files

use crate::core::index::commit_metadata_db;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};

pub fn compute(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!("Running content_metadata");

    log::debug!("computing metadata {} -> {}", commit.id, commit.message);
    commit_metadata_db::index_commit(repo, commit)?;

    Ok(())
}

