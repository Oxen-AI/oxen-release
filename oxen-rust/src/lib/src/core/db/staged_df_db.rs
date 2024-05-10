use polars::frame::DataFrame;
use sql::Select;
// use sql::Select;
use sql_query_builder as sql;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_COLS, OXEN_ID_COL, OXEN_ROW_ID_COL};

use crate::core::df::tabular;
use crate::model::schema::Field;
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::Schema;
use crate::{constants::TABLE_NAME, error::OxenError};
use polars::prelude::*; // or use polars::lazy::*; if you're working in a lazy context

use super::df_db;

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

    let added_column = Series::new(
        DIFF_STATUS_COL,
        vec![StagedRowStatus::Added.to_string(); df.height()],
    );
    let df = df.hstack(&[added_column])?;

    // Handle initialization for completely null {} create objects coming over from the hub
    let df = if df.height() == 0 {
        let added_column = Series::new(DIFF_STATUS_COL, vec![StagedRowStatus::Added.to_string()]);
        DataFrame::new(vec![added_column])?
    } else {
        df
    };

    let schema = full_staged_table_schema(conn)?;
    let inserted_df = df_db::insert_polars_df(conn, TABLE_NAME, &df, &schema)?;

    log::debug!("staged_df_db::append_row() inserted_df: {:?}", inserted_df);

    Ok(inserted_df)

    // Proceed with appending `new_df` to the database
}

pub fn modify_row(
    conn: &duckdb::Connection,
    df: &mut DataFrame,
    uuid: &str,
) -> Result<DataFrame, OxenError> {
    if df.height() != 1 {
        return Err(OxenError::basic_str("Modify row requires exactly one row"));
    }

    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let out_schema = full_staged_table_schema(conn)?;

    // Filter it down to exclude any of the OXEN_COLS, we don't want to modify these but hub sends them over
    let schema = df.schema();
    let df_cols = schema.get_names();
    let df_cols: Vec<&str> = df_cols
        .iter()
        .filter(|col| !OXEN_COLS.contains(col))
        .copied()
        .collect();
    let df = df.select(df_cols)?;

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

    // get existing hash and status from db
    let select_hash = Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("\"{}\" = '{}'", OXEN_ID_COL, uuid));

    let maybe_db_data = df_db::select(conn, &select_hash, true, None, None)?;

    let mut new_row = maybe_db_data.clone().to_owned();
    for col in df.get_columns() {
        // Replace that column in the existing df if it exists
        let col_name = col.name();
        let new_val = df.column(col_name)?.get(0)?;
        new_row.with_column(Series::new(col_name, vec![new_val]))?;
    }

    // TODO could use a struct to return these more safely
    let (insert_hash, updated_status) =
        get_hash_and_status_for_modification(conn, &maybe_db_data, &new_row)?;

    // Update with latest values pre insert
    // TODO: Find a better way to do this than overwriting the entire column here.
    new_row.with_column(Series::new(DIFF_STATUS_COL, vec![updated_status]))?;
    new_row.with_column(Series::new(DIFF_HASH_COL, vec![insert_hash]))?;

    let result = df_db::modify_row_with_polars_df(conn, TABLE_NAME, uuid, &new_row, &out_schema)?;

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

    let row_to_delete = df_db::select(conn, &select_stmt, true, None, None)?;

    if row_to_delete.height() == 0 {
        return Err(OxenError::resource_not_found(uuid));
    }

    // If it's newly added, delete it. Otherwise, set it to removed
    let status = row_to_delete.column(DIFF_STATUS_COL)?.get(0)?;
    let status_str = status.get_str();

    let status = match status_str {
        Some(status) => status,
        None => return Err(OxenError::basic_str("Diff status column is not a string")),
    };
    log::debug!("status is: {}", status);

    // Rows that weren't in previous commits are just removed from the staging df, rows in previous commits are tombstoned as "Removed"
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
                StagedRowStatus::Removed
            ))
            .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));
        conn.execute(&stmt.to_string(), [])?;
    };

    Ok(row_to_delete)
}

pub fn select_cols_from_schema(schema: &Schema) -> Result<String, OxenError> {
    let all_col_names = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .chain(schema.fields.iter().map(|col| format!("\"{}\"", col.name)))
        .collect::<Vec<String>>()
        .join(", ");

    Ok(all_col_names)
}

// Returns the schema of the underlying table with the oxen cols prepended in a predictable
// order expected by the UI / API
pub fn full_staged_table_schema(conn: &duckdb::Connection) -> Result<Schema, OxenError> {
    let schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    enhance_schema_with_oxen_cols(&schema)
}

pub fn schema_without_oxen_cols(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
) -> Result<Schema, OxenError> {
    let table_schema = df_db::get_schema_excluding_cols(conn, table_name, &OXEN_COLS)?;
    Ok(table_schema)
}

pub fn enhance_schema_with_oxen_cols(schema: &Schema) -> Result<Schema, OxenError> {
    let mut schema = schema.clone();

    let oxen_fields: Vec<Field> = OXEN_COLS
        .iter()
        .map(|col| Field {
            name: col.to_string(),
            dtype: if col == &OXEN_ROW_ID_COL {
                DataType::Int32.to_string()
            } else {
                DataType::String.to_string()
            },
            metadata: None,
        })
        .collect();

    schema.fields = oxen_fields
        .iter()
        .chain(schema.fields.iter())
        .cloned()
        .collect();

    Ok(schema)
}

pub fn df_diff(conn: &duckdb::Connection) -> Result<DataFrame, OxenError> {
    let select = sql::Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!(
            "\"{}\" != '{}'",
            DIFF_STATUS_COL,
            StagedRowStatus::Unchanged
        ));

    let schema = full_staged_table_schema(conn)?;

    let res = df_db::select(conn, &select, true, Some(&schema), None)?;

    Ok(res)
}

fn get_hash_and_status_for_modification(
    conn: &duckdb::Connection,
    old_row: &DataFrame,
    new_row: &DataFrame,
) -> Result<(String, String), OxenError> {
    let schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let col_names = schema.fields_names();

    let old_status = old_row.column(DIFF_STATUS_COL)?.get(0)?;
    let old_status = old_status
        .get_str()
        .ok_or_else(|| OxenError::basic_str("Diff status column is not a string"))?;

    let old_hash = old_row.column(DIFF_HASH_COL)?.get(0)?;

    let new_hash_df = tabular::df_hash_rows_on_cols(new_row.clone(), &col_names, "_temp_hash")?;
    let new_hash = new_hash_df.column("_temp_hash")?.get(0)?;
    let new_hash = new_hash
        .get_str()
        .ok_or_else(|| OxenError::basic_str("Diff hash column is not a string"))?;

    // We need to calculate the original hash for the row
    // Use a temp hash column to avoid collision with the column that's already there.
    let insert_hash = if old_hash.is_null() {
        let original_data_hash =
            tabular::df_hash_rows_on_cols(old_row.clone(), &col_names, "_temp_hash")?;
        let original_data_hash = original_data_hash.column("_temp_hash")?.get(0)?;
        original_data_hash
            .get_str()
            .ok_or_else(|| OxenError::basic_str("Diff hash column is not a string"))?
            .to_owned()
    } else {
        old_hash
            .get_str()
            .ok_or_else(|| OxenError::basic_str("Diff hash column is not a string"))?
            .to_owned()
    };

    // Anything previously added must stay added regardless of any further modifications.
    // Modifying back to original state changes it to unchanged
    // If we have no prior hash info on the original hash state, it is now modified (this is the first modification)

    let new_status = if old_status == StagedRowStatus::Added.to_string() {
        StagedRowStatus::Added.to_string()
    } else if old_status == StagedRowStatus::Removed.to_string() {
        if insert_hash == new_hash {
            StagedRowStatus::Unchanged.to_string()
        } else {
            StagedRowStatus::Modified.to_string()
        }
    } else if old_hash.is_null() {
        StagedRowStatus::Modified.to_string()
    } else if new_hash == insert_hash {
        StagedRowStatus::Unchanged.to_string()
    } else {
        StagedRowStatus::Modified.to_string()
    };

    Ok((insert_hash.to_string(), new_status))
}
