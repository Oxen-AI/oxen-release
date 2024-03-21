use duckdb::Connection;
use polars::frame::DataFrame;
use sql_query_builder::Select;

use crate::constants::{TABLE_NAME, OXEN_ID_COL};
use crate::core::db::df_db;
use crate::core::df::tabular;
use crate::core::index::mod_stager;
use crate::model::entry::commit_entry::Entry;
use crate::opts::DFOpts;
use crate::{error::OxenError, util};
use crate::model::{Branch, CommitEntry, LocalRepository};
use std::path::{Path, PathBuf};

use super::{CommitEntryReader, CommitReader};


pub fn index_dataset(
    repo: &LocalRepository,
    // branch_repo: &LocalRepository, 
    branch: &Branch, 
    path: &Path, 
    identifier: &str, 
) -> Result<(), OxenError> { // TODONOW: this should return a RemoteDataset struct
    
    if !util::fs::is_tabular(&path) {
        return Err(OxenError::basic_str("File format not supported, must be tabular.must be tabular."));
    }


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
        None => return Err(OxenError::resource_not_found(&path.to_string_lossy())),
    };

    // Get version path for entry 

    let db_path = mod_stager::mods_duckdb_path(repo, branch, identifier, &entry.path);


    let conn = df_db::get_connection(&db_path)?;
    
    df_db::drop_table(&conn, TABLE_NAME)?;
    let version_path = util::fs::version_path(&repo, &entry);

    log::debug!("index_dataset() got version path: {:?}", version_path);

    // TODO: Stubbed these out here because we will eventually want to parse the actual type, not just the extension. 
    // For v0, just treat the extension as gospel
    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") => index_csv(&version_path, &conn)?,
            Some("tsv") => index_tsv(&version_path, &conn)?,
            Some("json") | Some("jsonl") | Some("ndjson") => index_json(&version_path, &conn)?,
            Some("parquet") => index_parquet(&version_path, &conn)?,
            _ => return Err(OxenError::basic_str("File format not supported, must be tabular.")),
        },
        None => return Err(OxenError::basic_str("File format not supported, must be tabular.")),
    }

    Ok(())
}

pub fn extract_dataset_to_versions_dir(
    repo: &LocalRepository, 
    branch: &Branch, 
    entry: &CommitEntry, 
    identity: &str, 
) -> Result<(), OxenError> {
    let version_path = util::fs::version_path(repo, entry);
    let mods_duckdb_path = mod_stager::mods_duckdb_path(repo, branch, identity, entry.path.clone());
    let conn = df_db::get_connection(&mods_duckdb_path)?;
    // Match on the extension 

    let df_before = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!("extract_dataset_to_versions_dir() got df_before: {:?}", df_before);

    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") | Some("tsv") | Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&version_path, &conn)?,
            Some("parquet") => export_parquet(&version_path, &conn)?,
            _ => return Err(OxenError::basic_str("File format not supported, must be tabular.")),
        },
        None => return Err(OxenError::basic_str("File format not supported, must be tabular.")),
    }


    let df_after = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!("extract_dataset_to_versions_dir() got df_after: {:?}", df_after);

    Ok(())
}

pub fn extract_dataset_to_working_dir(
    repo: &LocalRepository, 
    branch_repo: &LocalRepository,
    branch: &Branch, 
    entry: &CommitEntry, 
    identity: &str, 
) -> Result<PathBuf, OxenError> {
    let working_path = branch_repo.path.join(entry.path.clone());
    log::debug!("got working path as: {:?}", working_path);
    let mods_duckdb_path = mod_stager::mods_duckdb_path(repo, branch, identity, entry.path.clone());
    let conn = df_db::get_connection(&mods_duckdb_path)?;
    // Match on the extension 

    if !working_path.exists() {
        std::fs::create_dir_all(&working_path.parent().expect("Failed to get parent directory"))?;
    }

    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") | Some("tsv") | Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&working_path, &conn)?,
            Some("parquet") => export_parquet(&working_path, &conn)?,
            _ => return Err(OxenError::basic_str("File format not supported, must be tabular.")),
        },
        None => return Err(OxenError::basic_str("File format not supported, must be tabular.")),
    }


    let df_after = tabular::read_df(&working_path, DFOpts::empty())?;
    log::debug!("extract_dataset_to_versions_dir() got df_after: {:?}", df_after);

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

    let db_path = mod_stager::mods_duckdb_path(repo, branch, identifier, &path);
    let conn = df_db::get_connection(&db_path)?;

    let query = Select::new().select("*").from(TABLE_NAME).where_clause(&format!("{} = '{}'", OXEN_ID_COL, row_id));
    let data = df_db::select(&conn, &query)?;
    log::debug!("get_row_by_id() got data: {:?}", data);
    Ok(data)

}



// TODONOW: we should manually pass column names from our schema. 
// their sniffer is worse than the one we're using and will give inconsistent results. 
fn index_csv(
    path: &Path,
    conn: &Connection,
) -> Result<(), OxenError> {
    let query = format!("CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM read_csv('{}', AUTO_DETECT=TRUE, header=True);", TABLE_NAME, OXEN_ID_COL, path.to_string_lossy());
    conn.execute(&query, [])?;

    let add_default_query = format!("ALTER TABLE {} ALTER COLUMN {} SET DEFAULT CAST(uuid() AS VARCHAR);", TABLE_NAME, OXEN_ID_COL);
    conn.execute(&add_default_query, [])?;

    Ok(())
}

// TODONOW: we should manually pass column names from our schema.
// TODONOW: consolidate these alter table statements too. and maybe bring the uuid logic to the oxen side.  
// their sniffer is worse than the one we're using and will give inconsistent results. 
fn index_tsv(
    path: &Path,
    conn: &Connection,
) -> Result<(), OxenError> {
    let query = format!("CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM read_csv('{}', AUTO_DETECT=TRUE, header=True);", TABLE_NAME, OXEN_ID_COL, path.to_string_lossy());
    conn.execute(&query, [])?;

    let add_default_query = format!("ALTER TABLE {} ALTER COLUMN {} SET DEFAULT CAST(uuid() AS VARCHAR);", TABLE_NAME, OXEN_ID_COL);
    conn.execute(&add_default_query, [])?;


    Ok(())
}

fn index_json(
    path: &Path, 
    conn: &Connection
) -> Result<(), OxenError> {
    let query = format!("CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM '{}';", TABLE_NAME, OXEN_ID_COL, path.to_string_lossy());
    conn.execute(&query, [])?;

    let add_default_query = format!("ALTER TABLE {} ALTER COLUMN {} SET DEFAULT CAST(uuid() AS VARCHAR);", TABLE_NAME, OXEN_ID_COL);
    conn.execute(&add_default_query, [])?;


    Ok(())
}

fn index_parquet(
    path: &Path, 
    conn: &Connection
) -> Result<(), OxenError> {
    let query = format!("CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM '{}';", TABLE_NAME, OXEN_ID_COL, path.to_string_lossy());
    conn.execute(&query, [])?;

    let add_default_query = format!("ALTER TABLE {} ALTER COLUMN {} SET DEFAULT CAST(uuid() AS VARCHAR);", TABLE_NAME, OXEN_ID_COL);
    conn.execute(&add_default_query, [])?;

    Ok(())
}   

fn export_rest(
    path: &Path, 
    conn: &Connection
) -> Result<(), OxenError> {
    let query = format!("COPY (SELECT * EXCLUDE {} FROM '{}') to '{}';", OXEN_ID_COL, TABLE_NAME, path.to_string_lossy());
    log::debug!("executing query: {}", query);
    conn.execute(&query, [])?;
    log::debug!("done query: {}", query);
    Ok(())
}

fn export_parquet(
    path: &Path, 
    conn: &Connection
) -> Result<(), OxenError> {
    let query = format!("COPY (SELECT * EXCLUDE {} FROM '{}') to '{}' (FORMAT PARQUET);", OXEN_ID_COL, TABLE_NAME, path.to_string_lossy());
    conn.execute(&query, [])?;
    Ok(())
}
