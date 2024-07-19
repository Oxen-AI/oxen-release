use std::path::PathBuf;

use duckdb::arrow::array::RecordBatch;
use duckdb::ToSql;
use polars::frame::DataFrame;
use serde_json::Value;
use sql::Select;

// use sql::Select;
use sql_query_builder as sql;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_COLS, OXEN_ID_COL};

use crate::core::db::data_frames::workspace_df_db::{
    full_staged_table_schema, schema_without_oxen_cols,
};
use crate::core::df::tabular;
use crate::core::index::workspaces::data_frames::data_frame_column_changes_db;
use crate::model::schema::Schema;
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::LocalRepository;
use crate::view::data_frames::columns::NewColumn;
use crate::view::data_frames::DataFrameColumnChange;
use crate::{constants::TABLE_NAME, error::OxenError};
use polars::prelude::*; // or use polars::lazy::*; if you're working in a lazy context

use super::df_db;

pub fn add_column(
    conn: &duckdb::Connection,
    new_column: &NewColumn,
    column_changes_path: &PathBuf,
) -> Result<DataFrame, OxenError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    println!("==================================");
    println!("table_schema: {:?}", table_schema);
    println!("db_path: {:?}", column_changes_path);
    println!("==================================");

    if table_schema.has_column(&new_column.name) {
        return Err(OxenError::column_name_already_exists(&new_column.name));
    }

    let change = DataFrameColumnChange {
        column_name: new_column.name.clone(),
        operation: "added".to_string(),
        new_name: "".to_string(), // Assuming you want an empty string here instead of nil
    };

    // Assuming this function exists and expecting it returns a Result<(), SomeErrorType>
    match data_frame_column_changes_db::write_data_frame_column_change(&change, column_changes_path)
    {
        Ok(_) => Ok(()),  // Successfully wrote the change
        Err(e) => Err(e), // Propagate the error
    };

    // // Handle initialization for completely null {} create objects coming over from the hub
    // let df = if df.height() == 0 {
    //     let added_column = Series::new(DIFF_STATUS_COL, vec![StagedRowStatus::Added.to_string()]);
    //     DataFrame::new(vec![added_column])?
    // } else {
    //     df
    // };

    let schema = full_staged_table_schema(conn)?;
    let inserted_df = insert_polars_df(conn, TABLE_NAME, &new_column, &schema)?;

    // log::debug!("staged_df_db::append_row() inserted_df: {:?}", inserted_df);

    // Ok(inserted_df)

    let s0 = Series::new("days", [0, 1, 2].as_ref());
    let s1 = Series::new("temp", [22.1, 19.9, 7.].as_ref());

    let df = DataFrame::new(vec![s0, s1]).unwrap();

    Ok(df)

    // Proceed with appending `new_df` to the database
}

pub fn insert_polars_df(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    new_column: &NewColumn,
    out_schema: &Schema,
) -> Result<DataFrame, OxenError> {
    let table_name = table_name.as_ref();

    let sql = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        table_name, new_column.name, new_column.data_type
    );

    conn.execute(&sql, [])?;

    // Fetch the entire table data
    let sql_query = format!("SELECT * FROM {}", table_name);
    let mut stmt = conn.prepare(&sql_query)?;
    let result_set: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;


    let df: DataFrame = df_db::record_batches_to_polars_df_explicit_nulls(result_set, &table_schema)?;

    println!("----------------------------------");
    println!("df: {:?}", df);
    println!("----------------------------------");
    // Use Polars to convert the result set into a DataFrame

    Ok(df)
}
