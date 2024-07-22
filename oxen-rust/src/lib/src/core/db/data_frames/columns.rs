use std::path::PathBuf;

use duckdb::arrow::array::RecordBatch;
use polars::frame::DataFrame;
use rocksdb::DB;

use crate::core::db;
use crate::core::db::data_frames::workspace_df_db::{
    full_staged_table_schema, schema_without_oxen_cols,
};
use crate::core::index::workspaces::data_frames::data_frame_column_changes_db;
use crate::view::data_frames::columns::NewColumn;
use crate::view::data_frames::DataFrameColumnChange;
use crate::{constants::TABLE_NAME, error::OxenError};

use super::df_db;
pub fn add_column(
    conn: &duckdb::Connection,
    new_column: &NewColumn,
    column_changes_path: &PathBuf,
) -> Result<DataFrame, OxenError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    if table_schema.has_column(&new_column.name) {
        return Err(OxenError::column_name_already_exists(&new_column.name));
    }

    record_column_change(new_column, column_changes_path)?;

    let inserted_df = insert_column(conn, TABLE_NAME, new_column)?;
    Ok(inserted_df)
}

fn record_column_change(
    new_column: &NewColumn,
    column_changes_path: &PathBuf,
) -> Result<(), OxenError> {
    let change = DataFrameColumnChange {
        column_name: new_column.name.clone(),
        operation: "added".to_string(),
        new_name: "".to_string(),
    };

    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(column_changes_path))?;

    data_frame_column_changes_db::write_data_frame_column_change(&change, &db)
}

pub fn insert_column(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    let table_name = table_name.as_ref();
    let sql = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        table_name, new_column.name, new_column.data_type
    );
    conn.execute(&sql, [])?;

    let sql_query = format!("SELECT * FROM {}", table_name);
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    let table_schema = full_staged_table_schema(conn)?;

    df_db::record_batches_to_polars_df_explicit_nulls(result_set, &table_schema)
}
