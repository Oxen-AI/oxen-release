use crate::error::OxenError;
use crate::model::Workspace;
use crate::view::data_frames::columns::{
    ColumnToDelete, ColumnToRestore, ColumnToUpdate, NewColumn,
};
use crate::view::data_frames::DataFrameColumnChange;
use crate::view::JsonDataFrameViews;

use polars::frame::DataFrame;
use std::path::Path;

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    todo!()
}

pub fn update(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, OxenError> {
    todo!()
}

pub fn delete(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, OxenError> {
    todo!()
}

pub fn restore(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_restore: &ColumnToRestore,
) -> Result<DataFrame, OxenError> {
    todo!()
}

pub fn get_column_diff(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
) -> Result<Vec<DataFrameColumnChange>, OxenError> {
    todo!()
}

pub fn decorate_fields_with_column_diffs(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    df_views: &mut JsonDataFrameViews,
) -> Result<(), OxenError> {
    todo!()
}
