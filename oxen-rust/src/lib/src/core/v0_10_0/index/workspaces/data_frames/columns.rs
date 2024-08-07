use polars::frame::DataFrame;
use rocksdb::DB;

use crate::constants::TABLE_NAME;
use crate::core::db;
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::core::db::data_frames::{columns, df_db};
use crate::core::v0_10_0::index::workspaces;
use crate::core::v0_10_0::index::workspaces::data_frames::column_changes_db;
use crate::error::OxenError;
use crate::model::schema::DataType;
use crate::model::Workspace;
use crate::view::data_frames::columns::{
    ColumnToDelete, ColumnToRestore, ColumnToUpdate, NewColumn,
};
use crate::view::data_frames::DataFrameColumnChange;

use std::path::Path;

use super::column_changes_db::get_all_data_frame_column_changes;

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

    let result = columns::add_column(&conn, new_column)?;

    columns::record_column_change(
        &column_changes_path,
        new_column.name.to_owned(),
        None,
        "added".to_owned(),
        None,
        None,
    )?;

    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}

pub fn delete(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path = workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("delete_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    let table_schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;
    let column_data_type = table_schema.get_field(&column_to_delete.name).unwrap();

    let result = columns::delete_column(&conn, column_to_delete)?;

    columns::record_column_change(
        &column_changes_path,
        column_to_delete.name.to_owned(),
        Some(column_data_type.dtype.clone()),
        "deleted".to_owned(),
        None,
        None,
    )?;

    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}

pub fn update(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path = workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("update_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    let table_schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;

    let result = columns::update_column(&conn, column_to_update, &table_schema)?;

    let column_data_type = table_schema.get_field(&column_to_update.name).unwrap();

    columns::record_column_change(
        &column_changes_path,
        column_to_update.name.to_owned(),
        Some(column_data_type.dtype.clone()),
        "modified".to_owned(),
        column_to_update.new_name.clone(),
        column_to_update.new_data_type.clone(),
    )?;

    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}

pub fn restore(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_restore: &ColumnToRestore,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path = workspaces::data_frames::column_changes_path(workspace, file_path);

    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(&column_changes_path))?;

    log::debug!("restore_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    if let Some(change) =
        column_changes_db::get_data_frame_column_change(&db, &column_to_restore.name)?
    {
        match change.operation.as_str() {
            "added" => {
                log::debug!("restore_column() column is added, deleting");
                let column_to_delete = ColumnToDelete {
                    name: change.column_name.clone(),
                };
                let result = columns::delete_column(&conn, &column_to_delete)?;
                columns::revert_column_changes(db, change.column_name.clone())?;
                workspaces::stager::add(workspace, file_path)?;
                Ok(result)
            }
            "deleted" => {
                log::debug!("restore_column() column was removed, adding it back");
                let new_column = NewColumn {
                    name: change.column_name.clone(),
                    data_type: change
                        .column_data_type
                        .clone()
                        .expect("Column data type is required but was None"),
                };
                let result = columns::add_column(&conn, &new_column)?;
                columns::revert_column_changes(db, change.column_name.clone())?;
                workspaces::stager::add(workspace, file_path)?;
                Ok(result)
            }
            "modified" => {
                log::debug!("restore_column() column was modified, reverting changes");
                let new_data_type = DataType::from_string(
                    change
                        .column_data_type
                        .expect("column_data_type is None, cannot unwrap"),
                )
                .to_sql();
                let column_to_update = ColumnToUpdate {
                    name: change
                        .new_name
                        .clone()
                        .expect("New name is required but was None"),
                    new_data_type: Some(new_data_type.to_owned()),
                    new_name: Some(change.column_name.clone()),
                };
                let table_schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;
                let result = columns::update_column(&conn, &column_to_update, &table_schema)?;
                columns::revert_column_changes(db, change.column_name.clone())?;
                workspaces::stager::add(workspace, file_path)?;
                Ok(result)
            }
            _ => Err(OxenError::UnsupportedOperation(
                change.operation.clone().into(),
            )),
        }
    } else {
        Err(OxenError::ColumnNameNotFound(
            column_to_restore.name.clone().into(),
        ))
    }
}

pub fn get_column_diff(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
) -> Result<Vec<DataFrameColumnChange>, OxenError> {
    let column_changes_path = workspaces::data_frames::column_changes_path(workspace, file_path);
    let opts = db::key_val::opts::default();
    let db = DB::open_for_read_only(&opts, dunce::simplified(&column_changes_path), false)?;
    get_all_data_frame_column_changes(&db)
}
