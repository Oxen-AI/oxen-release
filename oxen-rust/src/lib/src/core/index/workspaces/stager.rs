use rocksdb::{DBWithThreadMode, SingleThreaded};
use std::path::{Path, PathBuf};

use crate::constants::{FILES_DIR, MODS_DIR, OXEN_HIDDEN_DIR, WORKSPACES_DIR};
use crate::core::db;
use crate::core::db::str_json_db;
use crate::core::index::workspaces;
use crate::core::index::{CommitEntryReader, Stager};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, StagedData};
use crate::util;

pub fn mods_db_path(repo: &LocalRepository, workspace_id: &str, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());

    workspaces::workspace_dir(repo, workspace_id)
        .join(OXEN_HIDDEN_DIR)
        .join(WORKSPACES_DIR)
        .join(MODS_DIR)
        .join(MODS_DIR)
        .join(path_hash)
}

pub fn files_db_path(repo: &LocalRepository, workspace_id: &str) -> PathBuf {
    workspaces::workspace_dir(repo, workspace_id)
        .join(OXEN_HIDDEN_DIR)
        .join(WORKSPACES_DIR)
        .join(MODS_DIR)
        .join(FILES_DIR)
}

pub fn status(
    repo: &LocalRepository,
    workspace: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    directory: &Path,
) -> Result<StagedData, OxenError> {
    // Stager will be in the workspace repo
    let stager = Stager::new(workspace)?;
    // But we will read from the commit in the main repo
    log::debug!(
        "list_staged_data get commit by id {} -> {} -> {:?}",
        commit.message,
        commit.id,
        directory
    );

    let reader = CommitEntryReader::new(repo, commit)?;
    if Path::new(".") == directory {
        log::debug!("list_staged_data: status for root");
        let mut status = stager.status(&reader)?;
        list_staged_entries(repo, workspace_id, &mut status)?;
        Ok(status)
    } else {
        let mut status = stager.status_from_dir(&reader, directory)?;
        list_staged_entries(repo, workspace_id, &mut status)?;
        Ok(status)
    }
}

// Modifications to files are staged in a separate DB and applied on commit, so we fetch them from the mod_stager
fn list_staged_entries(
    repo: &LocalRepository,
    workspace_id: &str,
    status: &mut StagedData,
) -> Result<(), OxenError> {
    let mod_entries = list_files(repo, workspace_id)?;

    for path in mod_entries {
        status.modified_files.push(path.to_owned());
    }

    Ok(())
}

pub fn list_files(repo: &LocalRepository, workspace_id: &str) -> Result<Vec<PathBuf>, OxenError> {
    let db_path = files_db_path(repo, workspace_id);
    log::debug!("list_entries from files_db_path {db_path:?}");
    let opts = db::opts::default();
    let db: DBWithThreadMode<SingleThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::list_vals(&db)
}
