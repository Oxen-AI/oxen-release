use std::path::{Path, PathBuf};

use polars::frame::DataFrame;
use sql_query_builder::Select;

use crate::{
    constants::{CACHE_DIR, DUCKDB_CACHE_DIR},
    core::db::df_db,
    error::OxenError,
    model::{CommitEntry, LocalRepository}, util,
};

/// Module for handling the indexing of versioned dfs into duckdbs for SQL querying

pub fn db_cache_path(repo: LocalRepository, entry: CommitEntry) -> PathBuf {
    let hash_prefix = &entry.hash[0..2];
    let hash_suffix = &entry.hash[2..];
    let path = repo
        .path
        .join(CACHE_DIR)
        .join(DUCKDB_CACHE_DIR)
        .join(hash_prefix)
        .join(hash_suffix);
    path
}

pub fn index_df(repo: LocalRepository, entry: CommitEntry) -> Result<(), OxenError> {
    let duckdb_path = db_cache_path(repo, entry);

    if duckdb_path.exists() {
        return Ok(());
    }

    let parent = duckdb_path.parent().unwrap_or(&PathBuf::from(""));

    if !parent.exists() {
        util::fs::create_dir_all(&parent)?;
    }

    let conn = df_db::get_connection(&duckdb_path)?;
    
    let version_path = util::fs::version_path(&repo, &entry);

    index_file()

    Ok(df)
}

pub fn query_df(file_path: impl AsRef<Path>, sql: String) -> Result<DataFrame, OxenError> {
    let duckdb_path = file_path.as_ref().parent().unwrap().join("duckdb");
    let conn = df_db::get_connection(&duckdb_path)?;
    log::debug!("connection created");
    let from_clause = df_db::from_clause_from_disk_path(&file_path.as_ref())?;

    let select_all = Select::new().select("*").from(&from_clause);
    log::debug!("About to run select statement {}", select_all);
    let df = df_db::select(&conn, &select_all)?;
    Ok(df)
}
