use polars::frame::DataFrame;

use crate::core::db::data_frames::{columns, df_db};
use crate::core::df::tabular;
use crate::core::index::workspaces;
use crate::error::OxenError;
use crate::model::Workspace;
use crate::view::data_frames::columns::NewColumn;

use std::path::Path;

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path = workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("add_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    let result = columns::add_column(&conn, &new_column, &column_changes_path)?;
    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}
