use polars::datatypes::AnyValue;
use polars::frame::DataFrame;

use polars::prelude::NamedFrom;
use polars::series::Series;
use sql_query_builder::Select;

use crate::constants::{DIFF_STATUS_COL, OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME};
use crate::opts::DFOpts;

use crate::core::db::data_frames::{df_db, workspace_df_db};
use crate::core::df::tabular;
use crate::core::index::workspaces;
use crate::error::OxenError;
use crate::model::diff::DiffResult;
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{CommitEntry, LocalRepository, Workspace};
use crate::util;

use std::path::Path;

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = workspaces::data_frames::duckdb_path(workspace, file_path);
    log::debug!("add_row() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let df = tabular::parse_json_to_df(data)?;

    let result = workspace_df_db::append_row(&conn, &df)?;
    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}
