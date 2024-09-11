use polars::datatypes::AnyValue;
use polars::frame::DataFrame;

use polars::prelude::NamedFrom;
use polars::series::Series;
use rocksdb::DB;
use serde_json::Value;
use sql_query_builder::Select;

use crate::constants::{DIFF_STATUS_COL, OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME};
use crate::core::db;
use crate::opts::DFOpts;

use crate::core::db::data_frames::{df_db, rows, workspace_df_db};
use crate::core::df::tabular;
use crate::core::v0_19_0::workspaces;
use crate::error::OxenError;
use crate::model::data_frame::update_result::UpdateResult;
use crate::model::diff::DiffResult;
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{CommitEntry, LocalRepository, Workspace};
use crate::repositories;
use crate::util;
use crate::view::data_frames::DataFrameRowChange;
use crate::view::JsonDataFrameView;

use std::path::Path;

use crate::core::db::data_frames::row_changes_db::get_all_data_frame_row_changes;

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let row_changes_path =
        repositories::workspaces::data_frames::row_changes_path(workspace, file_path);

    log::debug!("add_row() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let df = tabular::parse_json_to_df(data)?;

    let mut result = rows::append_row(&conn, &df)?;

    let oxen_id_col = result
        .column("_oxen_id")
        .expect("Column _oxen_id not found");

    let last_idx = oxen_id_col.len() - 1;
    let last_value = oxen_id_col.get(last_idx)?;

    let row_id = last_value.to_string().trim_matches('"').to_string();

    let row = JsonDataFrameView::json_from_df(&mut result);

    rows::record_row_change(&row_changes_path, row_id, "added".to_owned(), row, None)?;

    workspaces::files::add(workspace, file_path)?;

    Ok(result)
}
