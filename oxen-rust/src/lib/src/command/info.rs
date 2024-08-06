//! # oxen info
//!
//! Get information about a path in the oxen repository
//!

use crate::error::OxenError;
use crate::model::entries::metadata_entry::CLIMetadataEntry;
use crate::model::LocalRepository;
use crate::opts::InfoOpts;
use crate::{repositories, util};

/// # Get info about a file or directory
pub fn info(repository: &LocalRepository, opts: InfoOpts) -> Result<CLIMetadataEntry, OxenError> {
    let path = opts.path;

    if let Some(revision) = opts.revision {
        let commit = repositories::revisions::get(repository, &revision)?
            .ok_or(OxenError::revision_not_found(revision.to_owned().into()))?;

        if let Some(entry) = repositories::entries::get_commit_entry(repository, &commit, &path)? {
            let version_path = util::fs::version_path(repository, &entry);
            return repositories::metadata::get_cli(repository, path, version_path);
        } else {
            eprintln!(
                "Path does not exist in revision: {}:{}",
                revision,
                path.to_string_lossy()
            );
            return Err(OxenError::path_does_not_exist(path));
        }
    }

    // Fall back to look for the existing path
    if !path.exists() {
        eprintln!("Path does not exist: {:?}", path);
        return Err(OxenError::path_does_not_exist(path));
    }

    // get file metadata
    repositories::metadata::get_cli(repository, &path, &path)
}
