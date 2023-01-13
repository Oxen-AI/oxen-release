//! entry_hash_cacher goes through the commit entry list and pre-computes the hash to verify everything is synced

use crate::constants::{HASH_FILE, HISTORY_DIR};
use crate::error::OxenError;
use crate::index::commit_validator;
use crate::model::{Commit, LocalRepository};
use crate::util;

pub fn compute_and_write_hash(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!("Running compute_and_write_hash");
    let hash = commit_validator::compute_commit_content_hash(repo, commit)?;
    write_hash(repo, commit, &hash)
}

pub fn write_hash(repo: &LocalRepository, commit: &Commit, val: &str) -> Result<(), OxenError> {
    let hash_file_path = util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(HASH_FILE);
    util::fs::write_to_path(&hash_file_path, val)?;
    Ok(())
}
