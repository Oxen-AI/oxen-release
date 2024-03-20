use std::path::PathBuf;

use duckdb::ToSql;
use polars::frame::DataFrame;
use polars::prelude::NamedFrom;
use sql_query_builder as sql;

use crate::constants::OXEN_ID_COL;

use crate::{
    constants::TABLE_NAME, error::OxenError, model::{
        entry::mod_entry::ModType,
        schema::{DataType, Field},
        Schema,
    }
};

use super::df_db;

/// Builds on df_db, but for specific use cases involving remote staging -
/// i.e., handling additional virtual columns beyond what's in the formal schema

// TODO: how to protect these more...
pub const OXEN_MOD_STATUS_COL: &str = "oxen_mod_status";
pub const OXEN_ROW_INDEX_COL: &str = "oxen_row_index";

// TODO: this is slightly duplicative with df_db but TBD if i want to pollute the schema upfront here.
// not returning it so seems fine to do...okay i'm sold
pub fn create_staged_table_if_not_exists(
    schema: &Schema,
    db_path: PathBuf,
) -> Result<String, OxenError> {
    let conn = df_db::get_connection(db_path)?;
    let mut columns: Vec<String> = schema.fields.iter().map(|f| f.to_sql()).collect();
    let mod_status_field = Field {
        name: OXEN_MOD_STATUS_COL.to_owned(),
        dtype: DataType::String.to_string(),
        metadata: None,
    };

    let row_index_field = Field {
        name: OXEN_ROW_INDEX_COL.to_owned(),
        dtype: DataType::UInt64.to_string(),
        metadata: None,
    };

    let schema_with_virts = Schema {
        name: Some(TABLE_NAME.to_owned()),
        fields: vec![mod_status_field, row_index_field]
            .into_iter()
            .chain(schema.fields.iter().cloned())
            .collect(),
        hash: "".to_string(),
        metadata: None,
    };

    let table_name = df_db::create_table_if_not_exists(&conn, &schema_with_virts)?;
    Ok(table_name.to_owned())
}


// pub fn add_row(
//     conn: &duckdb::Connection,
//     remote_dataset: RemoteDataset,
// )
// pub fn append_row(
//     conn: &duckdb::Connection,
//     df: &polars::frame::DataFrame,
// ) -> Result<(), OxenError> {
//     let mod_type_series = polars::prelude::Series::new(
//         OXEN_MOD_STATUS_COL,
//         vec![ModType::Append.to_string(); df.height()],
//     );
//     let row_idx_series = polars::prelude::Series::new(OXEN_ROW_INDEX_COL, vec![0; df.height()]);

//     let new_df = polars::prelude::DataFrame::new(vec![mod_type_series, row_idx_series])
//         .and_then(|df_new| df_new.hstack(&df.get_columns()))?;

//     df_db::insert_polars_df(conn, TABLE_NAME, &new_df)?;

//     // Print the db

//     let stmt = sql::Select::new().select(&format!("*")).from(&TABLE_NAME);

//     let res = df_db::select(conn, &stmt)?;

//     log::debug!("res df: {:?}", res);

//     Ok(())

//     // Proceed with appending `new_df` to the database
// }

pub fn append_row(
    conn: &duckdb::Connection,
    df: &polars::frame::DataFrame,
) -> Result<DataFrame, OxenError> {

    // Add in a check that the df has the same schema as the table
    
    let table_schema = df_db::get_schema_without_id(&conn, TABLE_NAME)?;
    let df_schema = df.schema();

    if !table_schema.has_same_field_names(&df_schema) {
        return Err(OxenError::incompatible_schemas(
            &df_schema.iter_fields().map(|f| f.name.to_string()).collect::<Vec<String>>(),
            table_schema
        ));
    }

    let inserted_df = df_db::insert_polars_df(conn, TABLE_NAME, &df)?;

    // Print the db

    let stmt = sql::Select::new().select(&format!("* EXCLUDE {}", OXEN_ID_COL)).from(&TABLE_NAME);

    let res = df_db::select(conn, &stmt)?;

    log::debug!("res df: {:?}", res);

    Ok(inserted_df)

    // Proceed with appending `new_df` to the database
}

