use duckdb::Connection;
use polars::frame::DataFrame;

use sql_query_builder::Select;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_ID_COL, TABLE_NAME};
use crate::core::db::df_db;
use crate::core::df::{sql, tabular};
use crate::core::index::{mod_stager, remote_dir_stager};

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
    let preview = df_db::select_with_opts(&conn, &select, opts)?;
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
    let db_path = mod_stager::mods_df_db_path(repo, branch, identifier, path);
    let conn = df_db::get_connection(db_path)?;
    let table_exists = df_db::table_exists(&conn, TABLE_NAME)?;
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
    // Match on the extension

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
    log::debug!("got working path as: {:?}", working_path);
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
        "extract_dataset_to_versions_dir() got df_after: {:?}",
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

    let query = Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("{} = '{}'", OXEN_ID_COL, row_id));
    let data = df_db::select(&conn, &query)?;
    log::debug!("get_row_by_id() got data: {:?}", data);
    Ok(data)
}

pub fn query_staged_df(
    repo: &LocalRepository,
    branch: &Branch,
    path: PathBuf,
    identifier: &str,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let db_path = mod_stager::mods_df_db_path(repo, branch, identifier, path);
    let conn = df_db::get_connection(db_path)?;

    let select = Select::new().select("*").from(TABLE_NAME);
    let df = df_db::select_with_opts(&conn, &select, opts)?;

    Ok(df)
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

fn add_row_status_cols(conn: &Connection) -> Result<(), OxenError> {
    let query_status = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT 'unchanged'",
        TABLE_NAME, DIFF_STATUS_COL
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
        let preview = df_db::select_with_opts(&conn, &select, opts)?;
        return Ok(Some(preview));
    }
    Ok(None)
}

fn export_rest(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_rest()");
    let query = format!(
        "COPY (SELECT * EXCLUDE {} FROM '{}') to '{}';",
        OXEN_ID_COL,
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
    let query = format!(
        "COPY (SELECT * EXCLUDE {} FROM '{}') to '{}' (HEADER, DELIMITER ',');",
        OXEN_ID_COL,
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
    let query = format!(
        "COPY (SELECT * EXCLUDE {} FROM '{}') to '{}' (HEADER, DELIMITER '\t');",
        OXEN_ID_COL,
        TABLE_NAME,
        path.to_string_lossy()
    );

    // let temp_select_query = Select::new().select("*").from(TABLE_NAME);

    // let temp_res = df_db::select(conn, &temp_select_query)?;
    // log::debug!("export_tsv() got df: {:?}", temp_res);

    conn.execute(&query, [])?;
    Ok(())
}

fn export_parquet(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_parquet()");

    let query = format!(
        "COPY (SELECT * EXCLUDE {} FROM '{}') to '{}' (FORMAT PARQUET);",
        OXEN_ID_COL,
        TABLE_NAME,
        path.to_string_lossy()
    );
    conn.execute(&query, [])?;

    Ok(())
}
