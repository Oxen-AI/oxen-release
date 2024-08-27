use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::constants::{FILES_DIR, MODS_DIR, OXEN_HIDDEN_DIR, STAGED_DIR};
use crate::core::db;
use crate::core::db::key_val::str_json_db;
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::model::{StagedData, StagedEntry};

fn files_db_path(workspace: &Workspace) -> PathBuf {
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join(FILES_DIR)
}

fn staged_files_db_path(workspace: &Workspace, directory: impl AsRef<Path>) -> PathBuf {
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(FILES_DIR)
        .join(directory.as_ref())
}

pub fn status(workspace: &Workspace, directory: impl AsRef<Path>) -> Result<StagedData, OxenError> {
    log::debug!(
        "workspaces::stager::status for workspace {:?} and directory {:?}",
        workspace.id,
        directory.as_ref()
    );

    let mut status = StagedData::empty();
    list_staged_entries(workspace, directory.as_ref(), &mut status)?;
    Ok(status)
}

// Modifications to files are staged in a separate DB and applied on commit,
// so we fetch them from the mod_stager
fn list_staged_entries(
    workspace: &Workspace,
    directory: impl AsRef<Path>,
    status: &mut StagedData,
) -> Result<(), OxenError> {
    let directory = directory.as_ref();
    let mod_entries = list_files(workspace)?;

    for path in mod_entries {
        log::debug!(
            "list_staged_entries path: {:?} directory: {:?}",
            path,
            directory
        );
        if Path::new(".") == directory || path.starts_with(directory) {
            status.modified_files.push(path.to_owned());
        }
    }

    let staged_entries = list_staged_files(workspace, directory)?;
    for (path, entry) in staged_entries {
        status.staged_files.insert(PathBuf::from(path), entry);
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

fn list_staged_files(
    workspace: &Workspace,
    directory: impl AsRef<Path>,
) -> Result<HashMap<String, StagedEntry>, OxenError> {
    let directory = directory.as_ref();
    let db_path = staged_files_db_path(workspace, directory);
    log::debug!("list_staged_files from files_db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<SingleThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::hash_map(&db)
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
