use std::path::PathBuf;

use duckdb::ToSql;
use polars::frame::DataFrame;
use polars::prelude::NamedFrom;
use sql::Select;
use sql_query_builder as sql;

use crate::constants::OXEN_ID_COL;

use crate::{
    constants::TABLE_NAME,
    error::OxenError,
    model::{
        entry::mod_entry::ModType,
        schema::{DataType, Field},
        Schema,
    },
};

use super::df_db;

/// Builds on df_db, but for specific use cases involving remote staging -
/// i.e., handling additional virtual columns beyond the formal schema, table names, etc.

pub fn append_row(
    conn: &duckdb::Connection,
    df: &polars::frame::DataFrame,
) -> Result<DataFrame, OxenError> {
    let select = Select::new().select("*").from(TABLE_NAME);
    let temp_df = df_db::select(&conn, &select)?;
    log::debug!("temp df from append_row: {:?}", temp_df);
    let testing_schema = df_db::get_schema(&conn, TABLE_NAME)?;

    log::debug!("Testing schema: {:?}", testing_schema);
    log::debug!("called from this conn: {:?}", conn);
    let table_schema = df_db::get_schema_without_id(&conn, TABLE_NAME)?;
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

    let inserted_df = df_db::insert_polars_df(conn, TABLE_NAME, &df)?;

    // Print the db
    let stmt = sql::Select::new()
        .select(&format!("* EXCLUDE {}", OXEN_ID_COL))
        .from(&TABLE_NAME);

    let res = df_db::select(conn, &stmt)?;

    log::debug!("res df: {:?}", res);

    Ok(inserted_df)

    // Proceed with appending `new_df` to the database
}

pub fn delete_row(conn: &duckdb::Connection, uuid: &str) -> Result<DataFrame, OxenError> {
    let stmt = sql::Delete::new()
        .delete_from(&TABLE_NAME)
        .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));

    let select_stmt = sql::Select::new()
        .select("*")
        .from(&TABLE_NAME)
        .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));

    // Select first - duckdb does't support DELETE RETURNING
    let maybe_row = df_db::select(conn, &select_stmt)?;

    if maybe_row.height() == 0 {
        return Err(OxenError::resource_not_found(uuid));
    }

    log::debug!("staged_df_db::delete_row() sql: {:?}", stmt);
    conn.execute(&stmt.to_string(), [])?;
    Ok(maybe_row)
}
