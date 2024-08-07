use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};

use std::path::{Path, PathBuf};

use crate::constants::{FILES_DIR, MODS_DIR};
use crate::core::db;
use crate::core::db::key_val::str_json_db;
use crate::core::v0_10_0::index::{CommitEntryReader, Stager};
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::model::StagedData;

fn files_db_path(workspace: &Workspace) -> PathBuf {
    workspace.dir().join(MODS_DIR).join(FILES_DIR)
}

pub fn status(workspace: &Workspace, directory: &Path) -> Result<StagedData, OxenError> {
    let repo = &workspace.base_repo;
    let workspace_repo = &workspace.workspace_repo;
    let commit = &workspace.commit;
    // Stager will be in the workspace repo
    let stager = Stager::new(workspace_repo)?;
    // But we will read from the commit in the main repo
    log::debug!(
        "list_staged_data get commit by id {} -> {} -> {:?}",
        commit.message,
        commit.id,
        directory
    );

    log::debug!("index::workspaces::stager::status for dir {:?}", directory);
    let reader = CommitEntryReader::new(repo, commit)?;
    if Path::new(".") == directory {
        let mut status = stager.status(&reader)?;
        list_staged_entries(workspace, &mut status)?;
        Ok(status)
    } else {
        let mut status = stager.status_from_dir(&reader, directory)?;
        list_staged_entries(workspace, &mut status)?;
        Ok(status)
    }
}

// Modifications to files are staged in a separate DB and applied on commit,
// so we fetch them from the mod_stager
fn list_staged_entries(workspace: &Workspace, status: &mut StagedData) -> Result<(), OxenError> {
    let mod_entries = list_files(workspace)?;

    for path in mod_entries {
        status.modified_files.push(path.to_owned());
    }

    Ok(())
}

pub fn list_files(workspace: &Workspace) -> Result<Vec<PathBuf>, OxenError> {
    let db_path = files_db_path(workspace);
    log::debug!("list_entries from files_db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<SingleThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::list_vals(&db)
}

pub fn add(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let db_path = files_db_path(workspace);
    log::debug!("workspaces::stager::add to db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<MultiThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let key = path.to_string_lossy();
    str_json_db::put(&db, &key, &key)
}

pub fn rm(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let opts = db::key_val::opts::default();
    let files_db_path = files_db_path(workspace);
    let files_db: DBWithThreadMode<MultiThreaded> =
        rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
    let key = path.as_ref().to_string_lossy();
    str_json_db::delete(&files_db, key)?;

    Ok(())
}
