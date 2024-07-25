use polars::frame::DataFrame;

use crate::core::db::data_frames::{columns, df_db};
use crate::core::df::tabular;
use crate::core::index::workspaces;
use crate::core::index::workspaces::data_frames::data_frame_column_changes_db;
use crate::error::OxenError;
use crate::model::{CommitEntry, Workspace};
use crate::view::data_frames::columns::{
    ColumnToDelete, ColumnToRestore, ColumnToUpdate, NewColumn,
};

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

    let result = columns::delete_column(&conn, &column_to_delete, &column_changes_path)?;
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

    let result = columns::update_column(&conn, &column_to_update, &column_changes_path)?;
    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}

// pub fn restore(
//     workspace: &Workspace,
//     entry: &CommitEntry,
//     column_to_restore: &ColumnToRestore,
// ) -> Result<DataFrame, OxenError> {
//     let file_path = file_path.as_ref();
//     let db_path = workspaces::data_frames::duckdb_path(workspace, file_path);
//     let column_changes_path = workspaces::data_frames::column_changes_path(workspace, file_path);
//     log::debug!("update_column() got db_path: {:?}", db_path);
//     let conn = df_db::get_connection(&db_path)?;

//     let column_changes = data_frame_column_changes_db::get_data_frame_column_change(&conn, )?;

//     let restored_row = restore_row_in_db(workspace, entry, row_id)?;
//     let diff = workspaces::data_frames::diff(workspace, &entry.path)?;

//     if let DiffResult::Tabular(diff) = diff {
//         if !diff.has_changes() {
//             log::debug!("no changes, deleting file from staged db");
//             // Restored to original state == delete file from staged db
//             workspaces::stager::rm(workspace, &entry.path)?;
//         }
//     }

//     Ok(restored_row)
// }

// pub fn restore_row_in_db(
//     workspace: &Workspace,
//     entry: &CommitEntry,
//     row_id: impl AsRef<str>,
// ) -> Result<DataFrame, OxenError> {
//     let row_id = row_id.as_ref();
//     let db_path = workspaces::data_frames::duckdb_path(workspace, &entry.path);
//     let conn = df_db::get_connection(db_path)?;

//     // Get the row by id
//     let row = get_by_id(workspace, &entry.path, row_id)?;

//     if row.height() == 0 {
//         return Err(OxenError::resource_not_found(row_id));
//     };

//     let row_status =
//         get_row_status(&row)?.ok_or_else(|| OxenError::basic_str("Row status not found"))?;

//     let result_row = match row_status {
//         StagedRowStatus::Added => {
//             // Row is added, just delete it
//             log::debug!("restore_row() row is added, deleting");
//             rows::delete_row(&conn, row_id)?
//         }
//         StagedRowStatus::Modified | StagedRowStatus::Removed => {
//             // Row is modified, just delete it
//             log::debug!("restore_row() row is modified, deleting");
//             let mut insert_row =
//                 prepare_modified_or_removed_row(&workspace.base_repo, entry, &row)?;
//             rows::modify_row(&conn, &mut insert_row, row_id)?
//         }
//         StagedRowStatus::Unchanged => {
//             // Row is unchanged, just return it
//             row
//         }
//     };

//     log::debug!("we're returning this row: {:?}", result_row);

//     Ok(result_row)
// }

// pub fn get_by_name(
//     workspace: &Workspace,
//     path: impl AsRef<Path>,
//     name: impl AsRef<str>,
// ) -> Result<DataFrame, OxenError> {
//     let path = path.as_ref();
//     let db_path = workspaces::data_frames::duckdb_path(workspace, path);
//     log::debug!("get_row_by_id() got db_path: {:?}", db_path);
//     let conn = df_db::get_connection(db_path)?;

//     let schema = workspace_df_db::full_staged_table_schema(&conn)?;

//     let query = Select::new()
//         .select("*")
//         .from(TABLE_NAME)
//         .where_clause(&format!("{} = '{}'", OXEN_ID_COL, row_id));
//     let data = df_db::select(&conn, &query, true, Some(&schema), None)?;
//     log::debug!("get_row_by_id() got data: {:?}", data);
//     Ok(data)
// }
