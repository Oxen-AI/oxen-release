use polars::frame::DataFrame;
use rocksdb::{IteratorMode, DB};

use crate::constants::TABLE_NAME;
use crate::core::db;
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::core::db::data_frames::{columns, df_db};
use crate::core::v0_19_0::workspaces;
use crate::error::OxenError;
use crate::model::data_frame::schema::field::{Changes, PreviousField};
use crate::model::data_frame::schema::Field;
use crate::model::Workspace;
use crate::repositories;
use crate::view::data_frames::columns::{
    ColumnToDelete, ColumnToRestore, ColumnToUpdate, NewColumn,
};
use crate::view::data_frames::{ColumnChange, DataFrameColumnChange};
use crate::view::JsonDataFrameViews;

use std::path::Path;

use crate::core::db::data_frames::column_changes_db::get_all_data_frame_column_changes;

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("add_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;
    let result = columns::add_column(&conn, new_column)?;

    let column_after = ColumnChange {
        column_name: new_column.name.clone(),
        column_data_type: Some(new_column.data_type.to_owned()),
    };

    columns::record_column_change(
        &column_changes_path,
        "added".to_owned(),
        None,
        Some(column_after),
    )?;

    workspaces::files::add(workspace, file_path)?;

    Ok(result)
}
