use polars::frame::DataFrame;

use crate::{core::db::data_frames::df_db, error::OxenError};

pub fn query_df(sql: String, conn: &mut duckdb::Connection) -> Result<DataFrame, OxenError> {
    let df = df_db::select_str(conn, sql, false, None, None)?;

    Ok(df)
}
