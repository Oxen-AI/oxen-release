use crate::constants::TABLE_NAME;
use crate::core;
use crate::core::db::data_frames::df_db;
use crate::error::OxenError;
use crate::model::{Schema, Workspace};
use crate::repositories;

use std::path::Path;

pub fn get_by_path(workspace: &Workspace, path: impl AsRef<Path>) -> Result<Schema, OxenError> {
    let file_path = path.as_ref();
    let staged_db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let conn = df_db::get_connection(staged_db_path)?;
    let df_schema = df_db::get_schema(&conn, TABLE_NAME)?;
    Ok(df_schema)
}

pub fn update_schema(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    og_schema: &Schema,
    before_column: &str,
    after_column: &str,
) -> Result<(), OxenError> {
    core::v0_19_0::workspaces::data_frames::schemas::update_schema(
        workspace,
        path,
        og_schema,
        before_column,
        after_column,
    )
}
