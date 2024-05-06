use std::path::PathBuf;

use polars::frame::DataFrame;

use crate::{
    api,
    constants::{CACHE_DIR, DUCKDB_CACHE_DIR, DUCKDB_DF_TABLE_NAME},
    core::db::df_db,
    error::OxenError,
    model::{CommitEntry, LocalRepository, Schema},
    util,
};

use super::tabular;

/// Module for handling the indexing of versioned dfs into duckdbs for SQL querying

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

pub fn db_cache_dir(repo: &LocalRepository) -> PathBuf {
    repo.path.join(CACHE_DIR).join(DUCKDB_CACHE_DIR)
}

pub fn query_df(
    repo: &LocalRepository,
    entry: &CommitEntry,
    sql: String,
    conn: &mut duckdb::Connection,
) -> Result<DataFrame, OxenError> {
    let duckdb_path = db_cache_path(repo, entry);
    index_df(repo, entry, conn)?;

    let conn = df_db::get_connection(duckdb_path)?;
    log::debug!("connection created");

    let df = df_db::select_raw(&conn, &sql, None)?;
    log::debug!("got this query output");
    Ok(df)
}

pub fn text2sql_df(
    repo: &LocalRepository,
    entry: &CommitEntry,
    schema: &Schema,
    nlp: String,
    conn: &mut duckdb::Connection,
    host: String,
) -> Result<DataFrame, OxenError> {
    let sql = futures::executor::block_on(get_sql(schema, &nlp, host))?;
    println!("\n{}\n", sql);
    query_df(repo, entry, sql, conn)
}

pub fn clear_all_cached_dfs(repo: &LocalRepository) -> Result<(), OxenError> {
    let db_cache = db_cache_dir(repo);
    if db_cache.exists() {
        std::fs::remove_dir_all(&db_cache)?;
    }
    Ok(())
}

pub fn clear_cached_df(repo: &LocalRepository, entry: &CommitEntry) -> Result<(), OxenError> {
    let duckdb_path = db_cache_path(repo, entry);
    if duckdb_path.exists() {
        std::fs::remove_file(&duckdb_path)?;
    }
    Ok(())
}

pub fn index_df(
    repo: &LocalRepository,
    entry: &CommitEntry,
    conn: &mut duckdb::Connection,
) -> Result<(), OxenError> {
    log::debug!("indexing df");
    let duckdb_path = db_cache_path(repo, entry);
    let default_parent = PathBuf::from("");
    let parent = duckdb_path.parent().unwrap_or(&default_parent);

    if df_db::table_exists(conn, DUCKDB_DF_TABLE_NAME)? {
        log::warn!(
            "index_df() file is already indexed at path {:?}",
            duckdb_path
        );
        return Ok(());
    }

    if !parent.exists() {
        util::fs::create_dir_all(parent)?;
    }

    let version_path = util::fs::version_path(repo, entry);

    df_db::index_file_with_id(&version_path, conn)?;

    log::debug!("file successfully indexed");

    Ok(())
}

pub fn df_is_indexed(repo: &LocalRepository, entry: &CommitEntry) -> Result<bool, OxenError> {
    let duckdb_path = db_cache_path(repo, entry);

    if !duckdb_path.exists() {
        return Ok(false);
    }
    let conn = df_db::get_connection(duckdb_path)?;
    let is_indexed = df_db::table_exists(&conn, DUCKDB_DF_TABLE_NAME)?;
    Ok(is_indexed)
}

async fn get_sql(schema: &Schema, q: &str, host: String) -> Result<String, OxenError> {
    let polars_schema = schema.to_polars();
    let schema_str = tabular::polars_schema_to_flat_str(&polars_schema);

    api::remote::text2sql::convert(q, &schema_str, Some(host.to_string())).await
}
