use polars::datatypes::AnyValue;
use polars::frame::DataFrame;

use polars::prelude::NamedFrom;
use polars::series::Series;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use sql_query_builder::Select;

use crate::constants::{DIFF_STATUS_COL, OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME};
use crate::opts::DFOpts;

use crate::core::db::{self, df_db, staged_df_db, str_json_db};
use crate::core::df::tabular;

use crate::core::index::workspaces;
use crate::error::OxenError;
use crate::model::diff::DiffResult;
use crate::model::entry::mod_entry::NewMod;
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{Commit, CommitEntry, LocalRepository};
use crate::util;

use std::path::{Path, PathBuf};

// Get a single row by the _oxen_id val
pub fn get_row_by_id(
    repo: &LocalRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let row_id = row_id.as_ref();
    let db_path = workspaces::data_frames::mods_db_path(repo, workspace_id, path);
    log::debug!("get_row_by_id() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let schema = staged_df_db::full_staged_table_schema(&conn)?;

    let query = Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("{} = '{}'", OXEN_ID_COL, row_id));
    let data = df_db::select(&conn, &query, true, Some(&schema), None)?;
    log::debug!("get_row_by_id() got data: {:?}", data);
    Ok(data)
}

pub fn get_row_id(row_df: &DataFrame) -> Result<Option<String>, OxenError> {
    if row_df.height() == 1 && row_df.get_column_names().contains(&OXEN_ID_COL) {
        Ok(row_df
            .column(OXEN_ID_COL)
            .unwrap()
            .get(0)
            .map(|val| val.to_string().trim_matches('"').to_string())
            .ok())
    } else {
        Ok(None)
    }
}

pub fn get_row_status(row_df: &DataFrame) -> Result<Option<StagedRowStatus>, OxenError> {
    if row_df.height() == 1 && row_df.get_column_names().contains(&DIFF_STATUS_COL) {
        let anyval_status = row_df.column(DIFF_STATUS_COL).unwrap().get(0)?;
        let str_status = anyval_status
            .get_str()
            .ok_or_else(|| OxenError::basic_str("Row status not found"))?;
        let status = StagedRowStatus::from_string(str_status)?;
        Ok(Some(status))
    } else {
        Ok(None)
    }
}

pub fn add(
    repo: &LocalRepository,
    workspace_id: &str,
    new_mod: &NewMod,
) -> Result<DataFrame, OxenError> {
    let db_path = workspaces::data_frames::mods_db_path(repo, workspace_id, &new_mod.entry.path);
    log::debug!("add_row() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let df = tabular::parse_data_into_df(&new_mod.data, new_mod.content_type.to_owned())?;

    let result = staged_df_db::append_row(&conn, &df)?;

    track_commit_entry(repo, workspace_id, &new_mod.entry.path)?;

    Ok(result)
}

pub fn restore(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: impl AsRef<str>,
    entry: &CommitEntry,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let row_id = row_id.as_ref();
    let restored_row = restore_row_in_db(repo, workspace_id, entry, row_id)?;
    let diff = workspaces::data_frames::diff(repo, commit, workspace_id, &entry.path)?;

    if let DiffResult::Tabular(diff) = diff {
        if !diff.has_changes() {
            log::debug!("no changes, deleting file from staged db");
            // Restored to original state == delete file from staged db
            let opts = db::opts::default();
            let files_db_path = workspaces::stager::files_db_path(repo, workspace_id);
            let files_db: DBWithThreadMode<MultiThreaded> =
                rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
            let key = entry.path.to_string_lossy().to_string();
            str_json_db::delete(&files_db, key)?;
        }
    }

    Ok(restored_row)
}

pub fn restore_row_in_db(
    repo: &LocalRepository,
    workspace_id: impl AsRef<str>,
    entry: &CommitEntry,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let row_id = row_id.as_ref();
    let db_path = workspaces::data_frames::mods_db_path(repo, workspace_id, &entry.path);
    let conn = df_db::get_connection(db_path)?;

    // Get the row by id
    let row = get_row_by_id(repo, workspace_id, &entry.path, row_id)?;

    if row.height() == 0 {
        return Err(OxenError::resource_not_found(row_id));
    };

    let row_status =
        get_row_status(&row)?.ok_or_else(|| OxenError::basic_str("Row status not found"))?;

    let result_row = match row_status {
        StagedRowStatus::Added => {
            // Row is added, just delete it
            log::debug!("restore_row() row is added, deleting");
            staged_df_db::delete_row(&conn, row_id)?
        }
        StagedRowStatus::Modified | StagedRowStatus::Removed => {
            // Row is modified, just delete it
            log::debug!("restore_row() row is modified, deleting");
            let mut insert_row = prepare_modified_or_removed_row(repo, entry, &row)?;
            staged_df_db::modify_row(&conn, &mut insert_row, row_id)?
        }
        StagedRowStatus::Unchanged => {
            // Row is unchanged, just return it
            row
        }
    };

    log::debug!("we're returning this row: {:?}", result_row);

    Ok(result_row)
}

pub fn get_row_idx(row_df: &DataFrame) -> Result<Option<usize>, OxenError> {
    if row_df.height() == 1 && row_df.get_column_names().contains(&OXEN_ROW_ID_COL) {
        let row_df_anyval = row_df.column(OXEN_ROW_ID_COL).unwrap().get(0)?;
        match row_df_anyval {
            AnyValue::UInt16(val) => Ok(Some(val as usize)),
            AnyValue::UInt32(val) => Ok(Some(val as usize)),
            AnyValue::UInt64(val) => Ok(Some(val as usize)),
            AnyValue::Int16(val) => Ok(Some(val as usize)),
            AnyValue::Int32(val) => Ok(Some(val as usize)),
            AnyValue::Int64(val) => Ok(Some(val as usize)),
            val => {
                log::debug!("unrecognized row index type {:?}", val);
                Ok(None)
            }
        }
    } else {
        Ok(None)
    }
}

fn prepare_modified_or_removed_row(
    repo: &LocalRepository,
    entry: &CommitEntry,
    row_df: &DataFrame,
) -> Result<DataFrame, OxenError> {
    let row_idx =
        get_row_idx(row_df)?.ok_or_else(|| OxenError::basic_str("Row index not found"))?;
    let row_idx_og = (row_idx - 1) as i64;

    // let scan_rows = 10000 as usize;
    let committed_df_path = util::fs::version_path(repo, entry);

    // TODONOW should not be using all rows - just need to parse delim
    let lazy_df = tabular::read_df(committed_df_path, DFOpts::empty())?;

    // Get the row by index
    let mut row = lazy_df.slice(row_idx_og, 1_usize);

    // Added rows will error here on out of index, but we caught them earlier..
    // let mut row = row.collect()?;

    row.with_column(Series::new(
        DIFF_STATUS_COL,
        vec![StagedRowStatus::Unchanged.to_string()],
    ))?;

    Ok(row)
}

pub fn delete(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = workspaces::data_frames::mods_db_path(repo, workspace_id, path);
    let deleted_row = {
        let conn = df_db::get_connection(db_path)?;
        staged_df_db::delete_row(&conn, row_id)?
    };

    track_commit_entry(repo, workspace_id, path)?;

    // TODO: Better way of tracking when a file is restored to its original state without diffing

    let diff = workspaces::data_frames::diff(repo, commit, workspace_id, path)?;

    if let DiffResult::Tabular(diff) = diff {
        if !diff.has_changes() {
            log::debug!("no changes, deleting file from staged db");
            // Restored to original state == delete file from staged db
            let opts = db::opts::default();
            let files_db_path = workspaces::stager::files_db_path(repo, workspace_id);
            let files_db: DBWithThreadMode<MultiThreaded> =
                rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
            let key = path.to_string_lossy();
            str_json_db::delete(&files_db, key)?;
        }
    }
    Ok(deleted_row)
}

pub fn update(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    row_id: &str,
    new_mod: &NewMod,
) -> Result<DataFrame, OxenError> {
    let db_path = workspaces::data_frames::mods_db_path(repo, workspace_id, &new_mod.entry.path);
    let conn = df_db::get_connection(db_path)?;

    let mut df = tabular::parse_data_into_df(&new_mod.data, new_mod.content_type.to_owned())?;

    let result = staged_df_db::modify_row(&conn, &mut df, row_id)?;

    track_commit_entry(repo, workspace_id, &new_mod.entry.path)?;

    let diff = workspaces::data_frames::diff(
        repo,
        commit,
        workspace_id,
        PathBuf::from(&new_mod.entry.path),
    )?;

    if let DiffResult::Tabular(diff) = diff {
        if !diff.has_changes() {
            // Restored to original state == delete file from staged db
            let opts = db::opts::default();
            let files_db_path = workspaces::stager::files_db_path(repo, workspace_id);
            let files_db: DBWithThreadMode<MultiThreaded> =
                rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
            let key = new_mod.entry.path.to_string_lossy();
            str_json_db::delete(&files_db, key)?;
        }
    }

    Ok(result)
}

fn track_commit_entry(
    repo: &LocalRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let db_path = workspaces::stager::files_db_path(repo, workspace_id);
    log::debug!("track_commit_entry from files_db_path {db_path:?}");
    let opts = db::opts::default();
    let db: DBWithThreadMode<MultiThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let key = path.to_string_lossy();
    str_json_db::put(&db, &key, &key)
}
