use duckdb::Connection;
use sql_query_builder::Select;

use crate::core::db::df_db;
use crate::core::index::mod_stager;
use crate::model::entry::commit_entry::Entry;
use crate::{error::OxenError, util};
use crate::model::{LocalRepository, Branch};
use std::path::Path;

use super::{CommitEntryReader, CommitReader};

const TABLE_NAME: &str = "STAGED_DATA";

pub fn index_dataset(
    repo: &LocalRepository,
    branch_repo: &LocalRepository, 
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



fn index_csv(
    path: &Path,
    conn: &Connection,
) -> Result<(), OxenError> {
    let query = format!("CREATE TABLE {} AS SELECT * FROM '{}';", TABLE_NAME, path.to_string_lossy());
    conn.execute(&query, [])?;

    // TODONOW delete 
    let select_all = Select::new().select("*").from(TABLE_NAME);
    let data = df_db::select(&conn, &select_all)?;
    log::debug!("index_csv() got data: {:?}", data);

    Ok(())
}

fn index_tsv(
    path: &Path,
    conn: &Connection,
) -> Result<(), OxenError> {
    let query = format!("CREATE TABLE {} AS SELECT * FROM '{}';", TABLE_NAME, path.to_string_lossy());
    conn.execute(&query, [])?;

    // TODONOW delete 
    let select_all = Select::new().select("*").from(TABLE_NAME);
    let data = df_db::select(&conn, &select_all)?;
    log::debug!("index_tsv() got data: {:?}", data);

    Ok(())
}

fn index_json(
    path: &Path, 
    conn: &Connection
) -> Result<(), OxenError> {
    let query = format!("CREATE TABLE {} AS SELECT * FROM '{}';", TABLE_NAME, path.to_string_lossy());
    conn.execute(&query, [])?;

    // TODONOW delete 
    let select_all = Select::new().select("*").from(TABLE_NAME);
    let data = df_db::select(&conn, &select_all)?;
    log::debug!("index_json() got data: {:?}", data);

    Ok(())
}

fn index_parquet(
    path: &Path, 
    conn: &Connection
) -> Result<(), OxenError> {
    let query = format!("CREATE TABLE {} AS SELECT * FROM '{}';", TABLE_NAME, path.to_string_lossy());
    conn.execute(&query, [])?;

    // TODONOW delete 
    let select_all = Select::new().select("*").from(TABLE_NAME);
    let data = df_db::select(&conn, &select_all)?;
    log::debug!("index_json() got data: {:?}", data);
    Ok(())
}   


