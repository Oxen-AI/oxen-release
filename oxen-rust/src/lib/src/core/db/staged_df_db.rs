use polars::frame::DataFrame;
// use sql::Select;
use sql_query_builder as sql;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_ID_COL};

use crate::model::staged_row_status::StagedRowStatus;
use crate::model::Schema;
use crate::{constants::TABLE_NAME, error::OxenError};

use super::df_db;
use polars::prelude::*; // or use polars::lazy::*; if you're working in a lazy context

/// Builds on df_db, but for specific use cases involving remote staging -
/// i.e., handling additional virtual columns beyond the formal schema, table names, etc.

pub fn append_row(conn: &duckdb::Connection, df: &DataFrame) -> Result<DataFrame, OxenError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let df_schema = df.schema();

    if !table_schema.has_same_field_names(&df_schema) {
        return Err(OxenError::incompatible_schemas(
            &df_schema
                .iter_fields()
                .map(|f| f.name.to_string())
                .collect::<Vec<String>>(),
            table_schema,
        ));
    }

    // Add a column DIFF_STATUS_COL with the value "added"
    let added_column = Series::new(
        DIFF_STATUS_COL,
        vec![StagedRowStatus::Added.to_string(); df.height()],
    );
    let df = df.hstack(&[added_column])?;

    let inserted_df = df_db::insert_polars_df(conn, TABLE_NAME, &df)?;

    log::debug!("staged_df_db::append_row() inserted_df: {:?}", inserted_df);

    Ok(inserted_df)

    // Proceed with appending `new_df` to the database
}

pub fn delete_row(conn: &duckdb::Connection, uuid: &str) -> Result<DataFrame, OxenError> {
    let stmt = sql::Update::new()
        .update(TABLE_NAME)
        .set(&format!(
            "\"{}\" = '{}'",
            DIFF_STATUS_COL,
            StagedRowStatus::Deleted.to_string()
        ))
        .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));

    let select_stmt = sql::Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));

    log::debug!("staged_df_db::delete_row() sql: {:?}", stmt);
    conn.execute(&stmt.to_string(), [])?;
    let maybe_res = df_db::select(conn, &select_stmt)?;

    log::debug!("got this deleted observation: {:?}", maybe_res);

    if maybe_res.height() == 0 {
        return Err(OxenError::resource_not_found(uuid));
    }
    Ok(maybe_res)
}

// pub fn delete_row(conn: &duckdb::Connection, uuid: &str) -> Result<DataFrame, OxenError> {
//     let stmt = sql::Delete::new()
//         .delete_from(TABLE_NAME)
//         .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));

//     let select_stmt = sql::Select::new()
//         .select("*")
//         .from(TABLE_NAME)
//         .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));

//     // Select first - duckdb does't support DELETE RETURNING
//     let maybe_row = df_db::select(conn, &select_stmt)?;

//     if maybe_row.height() == 0 {
//         return Err(OxenError::resource_not_found(uuid));
//     }

//     log::debug!("staged_df_db::delete_row() sql: {:?}", stmt);
//     conn.execute(&stmt.to_string(), [])?;
//     Ok(maybe_row)
// }

pub fn schema_without_oxen_cols(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
) -> Result<Schema, OxenError> {
    let oxen_cols = vec![OXEN_ID_COL, DIFF_HASH_COL, DIFF_STATUS_COL];
    let table_schema = df_db::get_schema_excluding_cols(conn, table_name, &oxen_cols)?;
    Ok(table_schema)
}

pub fn df_diff(conn: &duckdb::Connection) -> Result<DataFrame, OxenError> {
    let select = sql::Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!(
            "\"{}\" != '{}'",
            DIFF_STATUS_COL,
            StagedRowStatus::Unchanged.to_string()
        ));

    let res = df_db::select(conn, &select)?;

    Ok(res)
}
