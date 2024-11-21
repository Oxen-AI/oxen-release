use std::path::PathBuf;

use crate::model::LocalRepository;
use crate::repositories;
use crate::{core::db::data_frames::df_db, error::OxenError};
use polars::frame::DataFrame;
use uuid::Uuid;

pub fn query_df_from_repo(
    sql: String,
    repo: &LocalRepository,
    path: &PathBuf,
) -> Result<DataFrame, OxenError> {
    let commit = repositories::commits::head_commit(repo)?;

    if !repositories::workspaces::data_frames::is_queryable_data_frame_indexed(repo, path, &commit)?
    {
        // If not, proceed to create a new workspace and index the data frame.
        let workspace_id = Uuid::new_v4().to_string();
        let workspace = repositories::workspaces::create(repo, &commit, workspace_id, false)?;
        repositories::workspaces::data_frames::index(repo, &workspace, path)?;
    }

    let workspace =
        crate::core::v0_19_0::workspaces::data_frames::get_queryable_data_frame_workspace(
            repo, path, &commit,
        )?;

    let db_path = repositories::workspaces::data_frames::duckdb_path(&workspace, path);
    let mut conn = df_db::get_connection(db_path)?;
    query_df(sql, &mut conn)
}

pub fn query_df(sql: String, conn: &mut duckdb::Connection) -> Result<DataFrame, OxenError> {
    let df = df_db::select_str(conn, sql, false, None, None)?;

    Ok(df)
}
