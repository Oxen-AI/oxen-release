use polars::frame::DataFrame;
use crate::{core::db::df_db, error::OxenError};
use crate::model::LocalRepository;
use crate::core::index::CommitReader;
use crate::util::fs;
use crate::constants::{HISTORY_DIR, CACHE_DIR};


pub fn query_df_from_repo(
    sql: String,
    repo: &LocalRepository,
    // directory: impl AsRef<Path>
) -> Result<DataFrame, OxenError> {


    let commit_reader = CommitReader::new(&repo)?;
    let commit = commit_reader.head_commit()?;


    let path = fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("metadata")
        // .join(directory)
        .join("metadata.duckdb");


    let mut conn = df_db::get_connection(&path)?;
    
    Ok(query_df(sql, &mut conn)?)
}

pub fn query_df(sql: String, conn: &mut duckdb::Connection) -> Result<DataFrame, OxenError> {
    let df = df_db::select_str(conn, sql, false, None, None)?;

    Ok(df)
}
