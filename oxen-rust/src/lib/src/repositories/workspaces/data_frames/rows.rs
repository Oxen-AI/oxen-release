use crate::error::OxenError;
use crate::model::data_frame::update_result::UpdateResult;
use crate::model::Workspace;
use crate::view::data_frames::DataFrameRowChange;

use polars::frame::DataFrame;
use std::path::Path;

pub fn get_by_id(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    todo!()
}

pub fn get_row_idx(row_df: &DataFrame) -> Result<Option<usize>, OxenError> {
    todo!()
}

pub fn get_row_id(row_df: &DataFrame) -> Result<Option<String>, OxenError> {
    todo!()
}

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    todo!()
}

pub fn get_row_diff(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
) -> Result<Vec<DataFrameRowChange>, OxenError> {
    todo!()
}

pub fn update(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    todo!()
}

pub fn batch_update(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<Vec<UpdateResult>, OxenError> {
    todo!()
}

pub fn delete(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    todo!()
}

pub fn restore(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    todo!()
}
