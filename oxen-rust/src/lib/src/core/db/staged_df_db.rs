use polars::frame::DataFrame;
use sql::Select;
// use sql::Select;
use sql_query_builder as sql;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_ID_COL};

use crate::core::df::tabular;
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

    if !table_schema.has_field_names(&df_schema.get_names()) {
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

    let df = if df.height() == 0 {
        let added_column = Series::new(DIFF_STATUS_COL, vec![StagedRowStatus::Added.to_string()]);
        let df = DataFrame::new(vec![added_column])?;
        df
    } else {
        df
    };

    let inserted_df = df_db::insert_polars_df(conn, TABLE_NAME, &df)?;

    log::debug!("staged_df_db::append_row() inserted_df: {:?}", inserted_df);

    Ok(inserted_df)

    // Proceed with appending `new_df` to the database
}

pub fn modify_row(
    conn: &duckdb::Connection,
    df: &DataFrame,
    uuid: &str,
) -> Result<DataFrame, OxenError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let df_schema = df.schema();

    if !table_schema.has_field_names(&df_schema.get_names()) {
        return Err(OxenError::incompatible_schemas(
            &df_schema
                .iter_fields()
                .map(|f| f.name.to_string())
                .collect::<Vec<String>>(),
            table_schema,
        ));
    }

    if df.height() != 1 {
        return Err(OxenError::basic_str(
            "Modify row requires exactly one row".to_owned(),
        ));
    }

    // Determine the modification status

    // get existing hash from db
    let select_hash = Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("\"{}\" = '{}'", OXEN_ID_COL, uuid));

    let maybe_db_data = df_db::select(conn, &select_hash)?;
    let col_names = maybe_db_data
        .get_column_names()
        .iter()
        .map(|f| f.to_string())
        .collect::<Vec<String>>();

    let new_status = if maybe_db_data.height() == 0 {
        // Not hashed yet, aka this is first modification. Mark modified and calculate original hash
        // Then add it to the df we're sending over for insert
        let original_data_hash =
            tabular::df_hash_rows_on_cols(maybe_db_data.clone(), &col_names, DIFF_HASH_COL)?;
        let diff_status_col = Series::new(DIFF_STATUS_COL, vec![original_data_hash.to_string()]);
        let df = df.hstack(&[diff_status_col])?;
        StagedRowStatus::Modified.to_string()
    } else {
        StagedRowStatus::Modified.to_string()
    };
    // Add it to the df
    let diff_status_col = Series::new(DIFF_STATUS_COL, vec![new_status]);
    let df = df.hstack(&[diff_status_col])?;

    // TODO: add hash info to the df;

    let result = df_db::modify_row_with_polars_df(conn, TABLE_NAME, &uuid, &df)?;

    log::debug!("got this modified observation: {:?}", result);

    if result.height() == 0 {
        return Err(OxenError::resource_not_found(uuid));
    }
    Ok(result)
}

pub fn delete_row(conn: &duckdb::Connection, uuid: &str) -> Result<DataFrame, OxenError> {
    let select_stmt = sql::Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));

    let row_to_delete = df_db::select(conn, &select_stmt)?;

    if row_to_delete.height() == 0 {
        return Err(OxenError::resource_not_found(uuid));
    }

    // If it's newly added, delete it. Otherwise, set it to removed

    let status = row_to_delete.column(DIFF_STATUS_COL)?.get(0)?;
    let status_str = status.get_str();

    let status = match status_str {
        Some(status) => status,
        None => {
            return Err(OxenError::basic_str(
                "Diff status column is not a string".to_owned(),
            ))
        }
    };
    log::debug!("status is: {}", status);

    if status == StagedRowStatus::Added.to_string() {
        log::debug!("staged_df_db::delete_row() deleting row");
        let stmt = sql::Delete::new()
            .delete_from(TABLE_NAME)
            .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));
        conn.execute(&stmt.to_string(), [])?;
    } else {
        log::debug!("staged_df_db::delete_row() updating row to indicate deletion");
        let stmt = sql::Update::new()
            .update(TABLE_NAME)
            .set(&format!(
                "\"{}\" = '{}'",
                DIFF_STATUS_COL,
                StagedRowStatus::Removed.to_string()
            ))
            .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));
        conn.execute(&stmt.to_string(), [])?;
    };

    Ok(row_to_delete)
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
