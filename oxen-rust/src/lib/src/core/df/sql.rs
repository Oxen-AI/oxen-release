use polars::frame::DataFrame;
use crate::model::CommitEntry;
   
use crate::{core::db::df_db, error::OxenError};
use crate::model::LocalRepository;
use crate::constants::{CACHE_DIR, DUCKDB_CACHE_DIR};
use std::path::PathBuf;

pub fn db_cache_path(repo: &LocalRepository, entry: &CommitEntry) -> PathBuf {
    let hash_prefix = &entry.hash[0..2];
    let hash_suffix = &entry.hash[2..];

    repo.path
        .join(CACHE_DIR)
        .join(DUCKDB_CACHE_DIR)
        .join(hash_prefix)
        .join(hash_suffix)
}

pub fn get_conn(
    repo: &LocalRepository,
    entry: &CommitEntry,
) -> Result<duckdb::Connection, OxenError> {
    let duckdb_path = db_cache_path(repo, entry);
    let conn = df_db::get_connection(duckdb_path)?;
    Ok(conn)
}

pub fn query_df(sql: String, conn: &mut duckdb::Connection) -> Result<DataFrame, OxenError> {
    let df = df_db::select_str(conn, sql, false, None, None)?;

    Ok(df)
}
