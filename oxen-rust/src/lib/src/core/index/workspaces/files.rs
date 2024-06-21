use std::path::{Path, PathBuf};

use crate::api;
use crate::core::index::commit_entry_reader::CommitEntryReader;
use crate::core::index::schema_reader::SchemaReader;
use crate::core::index::stager::Stager;
use crate::core::index::workspaces::workspace_dir;
use crate::error::OxenError;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::util;

// Stages a file in a specified directory
pub fn add(
    repo: &LocalRepository,
    workspace: &LocalRepository,
    commit: &Commit,
    identifier: &str,
    filepath: &Path,
) -> Result<PathBuf, OxenError> {
    let staging_dir = workspace_dir(repo, commit, identifier);
    log::debug!("remote stager before add... staging_dir {:?}", staging_dir);

    // Stager will be in the new repo workspace
    let stager = Stager::new(workspace)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(repo, &commit.id)?.unwrap();
    let reader = CommitEntryReader::new(repo, &commit)?;
    log::debug!("about to add file in the stager");
    // Add a schema_reader to stager.add_file for?

    let schema_reader = SchemaReader::new(repo, &commit.id)?;

    stager.add_file(filepath.as_ref(), &reader, &schema_reader)?;
    log::debug!("done adding file in the stager");

    let relative_path = util::fs::path_relative_to_dir(filepath, &staging_dir)?;
    Ok(relative_path)
}

pub fn has_file(workspace: &LocalRepository, filepath: &Path) -> Result<bool, OxenError> {
    // Stager will be in the new repo workspace
    let stager = Stager::new(workspace)?;
    stager.has_staged_file(filepath)
}

pub fn delete_file(workspace: &LocalRepository, filepath: &Path) -> Result<(), OxenError> {
    // Stager will be in the repo workspace
    let stager = Stager::new(workspace)?;
    stager.remove_staged_file(filepath)?;
    let full_path = workspace.path.join(filepath);
    match util::fs::remove_file(&full_path) {
        Ok(_) => Ok(()),
        Err(e) => {
            log::error!("Error deleting file {full_path:?} -> {e:?}");
            Err(OxenError::entry_does_not_exist(full_path))
        }
    }
}
