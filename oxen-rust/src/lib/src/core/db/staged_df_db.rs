use polars::frame::DataFrame;
use sql::Select;
// use sql::Select;
use sql_query_builder as sql;

use crate::api::remote::df;
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

    // TODONOW this is very ugly
    let schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let full_schema = schema_with_oxen_cols(&schema)?;
    let inserted_df = df_db::insert_polars_df(conn, TABLE_NAME, &df, &full_schema)?;

    log::debug!("staged_df_db::append_row() inserted_df: {:?}", inserted_df);

    Ok(inserted_df)

    // Proceed with appending `new_df` to the database
}

pub fn modify_row(
    conn: &duckdb::Connection,
    df: &mut DataFrame,
    uuid: &str,
) -> Result<DataFrame, OxenError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let out_schema = schema_with_oxen_cols(&table_schema)?;

    // Filter it down to exclude any of the OXEN_COLS
    // TODONOW messy
    log::debug!("df before: {:?}", df);
    let schema = df.schema();
    let df_cols = schema.get_names();
    let df_cols: Vec<&str> = df_cols
        .iter()
        .filter(|col| !OXEN_COLS.contains(col))
        .map(|&col| col) // Add this line to dereference &&str to &str
        .collect();
    log::debug!("df cols: {:?}", df_cols);
    let df = df.select(df_cols)?;
    log::debug!("df after: {:?}", df);

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

    // get existing hash and status from db
    let select_hash = Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("\"{}\" = '{}'", OXEN_ID_COL, uuid));

    let mut maybe_db_data = df_db::select(conn, &select_hash)?;

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

    log::debug!("here is our hash: {}", insert_hash);
    log::debug!("here is our status: {}", updated_status);

    log::debug!("our new df looks like this: {:?}", new_row);
    log::debug!("our old df looks like this: {:?}", maybe_db_data);

    // Update with latest values pre insert
    // TODO this should be able to just be overwritten with one mutable var but polars doesn't like it...
    new_row.with_column(Series::new(DIFF_STATUS_COL, vec![updated_status]))?;
    new_row.with_column(Series::new(DIFF_HASH_COL, vec![insert_hash]))?;

    // Iterate over the values of the first row in df and update the corresponding column in the df

    log::debug!("df ready for insert insert: {:?}", df);

    let result = df_db::modify_row_with_polars_df(conn, TABLE_NAME, &uuid, &new_row, &out_schema)?;

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

pub fn select_cols_from_schema(schema: &Schema) -> Result<String, OxenError> {
    let all_col_names = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .chain(schema.fields.iter().map(|col| format!("\"{}\"", col.name)))
        .collect::<Vec<String>>()
        .join(", ");

    Ok(all_col_names)
}

pub fn schema_with_oxen_cols(schema: &Schema) -> Result<Schema, OxenError> {
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
    let table_schema = df_db::get_schema_excluding_cols(conn, table_name, &OXEN_COLS)?;
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

    let schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let full_schema = schema_with_oxen_cols(&schema)?;

    // Could do opts here for speed
    let res = df_db::select_with_schema(conn, &select, &full_schema)?;

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
        .ok_or_else(|| OxenError::basic_str("Diff status column is not a string".to_owned()))?;

    if old_status == StagedRowStatus::Removed.to_string() {
        return Err(OxenError::basic_str(
            "Cannot modify a deleted row".to_owned(),
        ));
    }

    let old_hash = old_row.column(DIFF_HASH_COL)?.get(0)?;

    log::debug!("hashing on these col_names: {:?}", col_names);
    let new_hash_df = tabular::df_hash_rows_on_cols(new_row.clone(), &col_names, "_temp_hash")?;
    log::debug!("here's our new_hash_df: {:?}", new_hash_df);
    let new_hash = new_hash_df.column("_temp_hash")?.get(0)?;
    let new_hash = new_hash
        .get_str()
        .ok_or_else(|| OxenError::basic_str("Diff hash column is not a string".to_owned()))?;

    log::debug!("got new_hash: {}", new_hash);

    // We need to calculate the original hash for the row
    // Use a temp hash column to avoid collision with the column that's already there.
    let insert_hash = if old_hash.is_null() {
        let original_data_hash =
            tabular::df_hash_rows_on_cols(old_row.clone(), &col_names, "_temp_hash")?;
        let original_data_hash = original_data_hash.column("_temp_hash")?.get(0)?;
        let original_data_hash = original_data_hash
            .get_str()
            .ok_or_else(|| OxenError::basic_str("Diff hash column is not a string".to_owned()))?
            .to_owned();
        original_data_hash
    } else {
        old_hash
            .get_str()
            .ok_or_else(|| OxenError::basic_str("Diff hash column is not a string".to_owned()))?
            .to_owned()
    };
    log::debug!("got old_hash: {}", old_hash);
    log::debug!("got insert_hash: {}", insert_hash);
    log::debug!("got new hash: {}", new_hash);

    let new_status = if old_status == StagedRowStatus::Added.to_string() {
        StagedRowStatus::Added.to_string()
    } else if old_hash.is_null() {
        StagedRowStatus::Modified.to_string()
    } else {
        if new_hash == insert_hash {
            StagedRowStatus::Unchanged.to_string()
        } else {
            StagedRowStatus::Modified.to_string()
        }
    };

    Ok((insert_hash.to_string(), new_status))
}
