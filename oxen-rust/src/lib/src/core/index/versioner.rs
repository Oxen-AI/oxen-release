//! versioner is responsible for interacting with entries in the versioned directory
//!

use filetime::FileTime;
use std::path::Path;

use crate::current_function;
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository};
use crate::util;

use super::CommitDirEntryWriter;

pub fn backup_file(
    repository: &LocalRepository,
    committer: &CommitDirEntryWriter,
    entry: &CommitEntry,
    filepath: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let version_path = util::fs::version_path(repository, entry);
    let filepath = filepath.as_ref();
    if should_copy_entry(entry, &version_path) {
        log::debug!("{} unpack {:?}", current_function!(), entry.path);
        match util::fs::copy_mkdir(filepath, &version_path) {
            Ok(_) => {}
            Err(err) => {
                log::error!(
                    "Could not copy {:?} to {:?}: {}",
                    version_path,
                    filepath,
                    err
                );
            }
        }

        log::debug!(
            "{} updating timestamp for {:?}",
            current_function!(),
            filepath
        );

        match util::fs::metadata(filepath) {
            Ok(metadata) => {
                let mtime = FileTime::from_last_modification_time(&metadata);
                committer.set_file_timestamps(entry, &mtime)?;
            }
            Err(err) => {
                log::error!("Could not update timestamp for {:?}: {}", filepath, err);
            }
        }
    }
    Ok(())
}

fn should_copy_entry(entry: &CommitEntry, path: &Path) -> bool {
    !path.exists() || path_hash_is_different(entry, path)
}

fn path_hash_is_different(entry: &CommitEntry, path: &Path) -> bool {
    if let Ok(hash) = util::hasher::hash_file_contents(path) {
        return hash != entry.hash;
    }
    false
}
