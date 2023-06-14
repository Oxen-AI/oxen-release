//! Abstraction over DuckDB database to write and read dataframes from disk.
//!

use crate::error::OxenError;

use duckdb::arrow::record_batch::RecordBatch;
use polars::prelude::*;
use std::io::Cursor;
use std::path::Path;

/// Get a connection to a duckdb database.
pub fn get_connection(path: impl AsRef<Path>) -> Result<duckdb::Connection, OxenError> {
    let path = path.as_ref();
    let conn = duckdb::Connection::open(path)?;
    Ok(conn)
}

/// Query number of rows in a table.
pub fn count(conn: &duckdb::Connection, table: &str) -> Result<usize, OxenError> {
    let sql = format!("SELECT count(*) FROM {}", table);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let size: usize = row.get(0)?;
        Ok(size)
    } else {
        Err(OxenError::basic_str(format!("No rows in table {}", table)))
    }
}

/// Select fields from a table.
pub fn select(
    conn: &duckdb::Connection,
    table: &str,
    fields: &[&str],
    limit: usize,
) -> Result<DataFrame, OxenError> {
    let fields = fields.join(", ");
    let sql = format!("SELECT {} FROM {} LIMIT {}", fields, table, limit);
    log::debug!("select sql: {}", sql);
    let mut stmt = conn.prepare(&sql)?;
    let records: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

    // Hacky to convert to json and then to polars...but the results from these queries should be small, and
    // if they are bigger, need to look into converting directly from arrow to polars.
    let json = arrow_json::writer::record_batches_to_json_rows(&records[..]).unwrap();
    let json_str = serde_json::to_string(&json).unwrap();

    let content = Cursor::new(json_str.as_bytes());
    let df = JsonReader::new(content).finish().unwrap();
    log::debug!("result df: {:?}", df);

    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_df_db_count() -> Result<(), OxenError> {
        let db_file = Path::new("data")
            .join("test")
            .join("db")
            .join("metadata.db");
        let conn = get_connection(&db_file)?;

        let count = count(&conn, "metadata")?;

        assert_eq!(count, 16);

        Ok(())
    }

    #[test]
    fn test_df_db_select() -> Result<(), OxenError> {
        let db_file = Path::new("data")
            .join("test")
            .join("db")
            .join("metadata.db");
        let conn = get_connection(&db_file)?;

        let limit = 7;
        let fields = vec!["filename", "data_type"];
        let df = select(&conn, "metadata", &fields, limit)?;

        assert!(df.width() == fields.len());
        assert!(df.height() == limit);

        Ok(())
    }
}
