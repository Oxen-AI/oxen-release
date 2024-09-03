use std::path::Path;

use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, MerkleHash};
use crate::util;

// TODO: probably need to pass a data node here instead of a hash to get the metadata
pub fn restore_file(
    repo: &LocalRepository,
    hash: &MerkleHash,
    dst_path: &Path,
) -> Result<(), OxenError> {
    let version_path = util::fs::version_path_from_hash(repo, hash);
    if !version_path.exists() {
        return Err(OxenError::basic_str(&format!(
            "Source file not found in versions directory: {:?}",
            version_path
        )));
    }

    let working_path = repo.path.join(dst_path);
    if let Some(parent) = dst_path.parent() {
        util::fs::create_dir_all(parent)?;
    }

    util::fs::copy(version_path, working_path.clone())?;
    // TODO: set file metadata
    // Previous version used:
    // CommitEntryWriter::set_file_timestamps(repo, path, entry, files_db)?;
    Ok(())
}
