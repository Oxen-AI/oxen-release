use duckdb::Connection;
use polars::datatypes::AnyValue;
use polars::frame::DataFrame;

use polars::prelude::NamedFrom;
use polars::series::Series;
use sql_query_builder::{Delete, Select};

use crate::api;
use crate::constants::{
    DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_COLS, OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME,
};
use crate::core::db::staged_df_db::select_cols_from_schema;
use crate::core::db::{df_db, staged_df_db};
use crate::core::df::{sql, tabular};
use crate::core::index::{mod_stager, remote_dir_stager};

use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{Branch, CommitEntry, LocalRepository};
use crate::opts::DFOpts;
use crate::{error::OxenError, util};
use std::path::{Path, PathBuf};

use super::{CommitEntryReader, CommitReader};

pub fn index_dataset(
    repo: &LocalRepository,
    branch: &Branch,
    path: &Path,
    identifier: &str,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    if !util::fs::is_tabular(path) {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.must be tabular.",
        ));
    }

    // need to init or get the remote staging env - for if this was called from API? todo
    let _branch_repo = remote_dir_stager::init_or_get(repo, branch, identifier)?;

    // Get the version path
    let commit_reader = CommitReader::new(repo)?;
    let commit = commit_reader.get_commit_by_id(&branch.commit_id)?;
    let commit = match commit {
        Some(commit) => commit,
        None => return Err(OxenError::resource_not_found(&branch.commit_id)),
    };

    let reader = CommitEntryReader::new(repo, &commit)?;
    let entry = reader.get_entry(path)?;
    let entry = match entry {
        Some(entry) => entry,
        None => return Err(OxenError::resource_not_found(path.to_string_lossy())),
    };

    let db_path = mod_stager::mods_df_db_path(repo, branch, identifier, &entry.path);

    if !db_path
        .parent()
        .expect("Failed to get parent directory")
        .exists()
    {
        std::fs::create_dir_all(db_path.parent().expect("Failed to get parent directory"))?;
    }

    let maybe_preview = copy_duckdb_if_already_indexed(repo, &entry, opts, &db_path)?;

    if let Some(preview) = maybe_preview {
        return Ok(preview);
    }

    let conn = df_db::get_connection(db_path)?;

    if df_db::table_exists(&conn, TABLE_NAME)? {
        df_db::drop_table(&conn, TABLE_NAME)?;
    }
    let version_path = util::fs::version_path(repo, &entry);

    log::debug!("index_dataset() got version path: {:?}", version_path);

    df_db::index_file_with_id(&version_path, &conn)?;

    add_row_status_cols(&conn)?;

    let preview = df_db::preview(&conn, TABLE_NAME)?;
    log::debug!("index_dataset() got preview: {:?}", preview);

    let commit_path = mod_stager::mods_commit_ref_path(repo, branch, identifier, &entry.path);
    std::fs::write(commit_path, branch.commit_id.as_str())?;

    let select = Select::new().select("*").from(TABLE_NAME);
    let preview = df_db::select(&conn, &select, true, None, Some(opts))?;

    Ok(preview)
}

pub fn unindex_df(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    let mods_df_db_path = mod_stager::mods_df_db_path(repo, branch, identity, path);
    let conn = df_db::get_connection(mods_df_db_path)?;
    df_db::drop_table(&conn, TABLE_NAME)?;

    Ok(())
}

pub fn dataset_is_indexed(
    repo: &LocalRepository,
    branch: &Branch,
    identifier: &str,
    path: &Path,
) -> Result<bool, OxenError> {
    log::debug!("checking dataset is indexed for {:?}", path);
    let db_path = mod_stager::mods_df_db_path(repo, branch, identifier, path);
    log::debug!("getting conn at path {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let table_exists = df_db::table_exists(&conn, TABLE_NAME)?;
    log::debug!("dataset_is_indexed() got table_exists: {:?}", table_exists);
    Ok(table_exists)
}

pub fn extract_dataset_to_versions_dir(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
    identity: &str,
) -> Result<(), OxenError> {
    let version_path = util::fs::version_path(repo, entry);
    let mods_df_db_path = mod_stager::mods_df_db_path(repo, branch, identity, entry.path.clone());
    let conn = df_db::get_connection(mods_df_db_path)?;

    log::debug!("extracting to versions path: {:?}", version_path);

    // Filter out any with removed status before extracting
    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    conn.execute(&delete.to_string(), [])?;

    let df_before = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_versions_dir() got df_before: {:?}",
        df_before
    );

    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") => export_csv(&version_path, &conn)?,
            Some("tsv") => export_tsv(&version_path, &conn)?,
            Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&version_path, &conn)?,
            Some("parquet") => export_parquet(&version_path, &conn)?,
            _ => {
                return Err(OxenError::basic_str(
                    "File format not supported, must be tabular.",
                ))
            }
        },
        None => {
            return Err(OxenError::basic_str(
                "File format not supported, must be tabular.",
            ))
        }
    }

    let df_after = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_versions_dir() got df_after: {:?}",
        df_after
    );

    Ok(())
}

// TODONOW combine with versions dir export fn and genericize on path
pub fn extract_dataset_to_working_dir(
    repo: &LocalRepository,
    branch_repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
    identity: &str,
) -> Result<PathBuf, OxenError> {
    let working_path = branch_repo.path.join(entry.path.clone());
    let mods_df_db_path = mod_stager::mods_df_db_path(repo, branch, identity, entry.path.clone());
    let conn = df_db::get_connection(mods_df_db_path)?;
    // Match on the extension

    if !working_path.exists() {
        std::fs::create_dir_all(
            working_path
                .parent()
                .expect("Failed to get parent directory"),
        )?;
    }

    log::debug!("created working path: {:?}", working_path);

    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    let res = conn.execute(&delete.to_string(), [])?;
    log::debug!("delete query result is: {:?}", res);

    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") => export_csv(&working_path, &conn)?,
            Some("tsv") => export_tsv(&working_path, &conn)?,
            Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&working_path, &conn)?,
            Some("parquet") => export_parquet(&working_path, &conn)?,
            _ => {
                return Err(OxenError::basic_str(
                    "File format not supported, must be tabular.",
                ))
            }
        },
        None => {
            return Err(OxenError::basic_str(
                "File format not supported, must be tabular.",
            ))
        }
    }

    let df_after = tabular::read_df(&working_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_working_dir() got df_after: {:?}",
        df_after
    );

    Ok(working_path)
}

// Get a single row by the _oxen_id val
pub fn get_row_by_id(
    repo: &LocalRepository,
    branch: &Branch,
    path: PathBuf,
    identifier: &str,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let db_path = mod_stager::mods_df_db_path(repo, branch, identifier, path);
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

pub fn query_staged_df(
    repo: &LocalRepository,
    entry: &CommitEntry,
    branch: &Branch,
    identifier: &str,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let db_path = mod_stager::mods_df_db_path(repo, branch, identifier, entry.path.clone());
    let conn = df_db::get_connection(db_path)?;

    // Get the schema of this commit entry
    let schema = api::local::schemas::get_by_path_from_ref(repo, &entry.commit_id, &entry.path)?
        .ok_or_else(|| OxenError::resource_not_found(entry.path.to_string_lossy()))?;

    // Enrich w/ oxen cols
    let full_schema = staged_df_db::enhance_schema_with_oxen_cols(&schema)?;

    let col_names = select_cols_from_schema(&schema)?;

    let select = Select::new().select(&col_names).from(TABLE_NAME);

    let df = df_db::select(&conn, &select, true, Some(&full_schema), Some(opts))?;

    Ok(df)
}

pub fn restore_row(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
    identifier: &str,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let db_path = mod_stager::mods_df_db_path(repo, branch, identifier, entry.path.clone());
    let conn = df_db::get_connection(db_path)?;

    // Get the row by id
    let row = get_row_by_id(repo, branch, entry.path.clone(), identifier, row_id)?;

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

pub fn count(
    repo: &LocalRepository,
    branch: &Branch,
    path: PathBuf,
    identifier: &str,
) -> Result<usize, OxenError> {
    let db_path = mod_stager::mods_df_db_path(repo, branch, identifier, path);
    let conn = df_db::get_connection(db_path)?;

    let count = df_db::count(&conn, TABLE_NAME)?;
    Ok(count)
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

fn add_row_status_cols(conn: &Connection) -> Result<(), OxenError> {
    let query_status = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT '{}'",
        TABLE_NAME,
        DIFF_STATUS_COL,
        StagedRowStatus::Unchanged
    );
    conn.execute(&query_status, [])?;

    let query_hash = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT NULL",
        TABLE_NAME, DIFF_HASH_COL
    );
    conn.execute(&query_hash, [])?;
    Ok(())
}

fn copy_duckdb_if_already_indexed(
    repo: &LocalRepository,
    entry: &CommitEntry,
    opts: &DFOpts,
    new_db_path: &Path,
) -> Result<Option<DataFrame>, OxenError> {
    let maybe_existing_db_path = sql::db_cache_path(repo, entry);
    let conn = df_db::get_connection(&maybe_existing_db_path)?;
    if df_db::table_exists(&conn, TABLE_NAME)? {
        std::fs::copy(&maybe_existing_db_path, new_db_path)?;
        let select = Select::new().select("*").from(TABLE_NAME);
        let preview = df_db::select(&conn, &select, true, None, Some(opts))?;
        return Ok(Some(preview));
    }
    Ok(None)
}

fn export_rest(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_rest()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}';",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    // let temp_select_query = Select::new().select("*").from(TABLE_NAME);
    // let temp_res = df_db::select(conn, &temp_select_query)?;
    // log::debug!("export_rest() got df: {:?}", temp_res);

    conn.execute(&query, [])?;
    Ok(())
}

fn export_csv(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_csv()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (HEADER, DELIMITER ',');",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    // let temp_select_query = Select::new().select("*").from(TABLE_NAME);

    // let temp_res = df_db::select(conn, &temp_select_query)?;
    // log::debug!("export_csv() got df: {:?}", temp_res);

    conn.execute(&query, [])?;

    Ok(())
}

fn export_tsv(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_tsv()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (HEADER, DELIMITER '\t');",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    conn.execute(&query, [])?;
    Ok(())
}

fn export_parquet(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_parquet()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");

    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (FORMAT PARQUET);",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );
    conn.execute(&query, [])?;

    Ok(())
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
