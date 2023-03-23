use polars::prelude::DataFrame;
use std::fmt;

pub struct DataFrameDiff {
    pub added_rows: Option<DataFrame>,
    pub removed_rows: Option<DataFrame>,
    pub added_cols: Option<DataFrame>,
    pub removed_cols: Option<DataFrame>,
}

impl fmt::Display for DataFrameDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut results: Vec<String> = vec![];
        if let Some(rows) = &self.added_rows {
            results.push(format!("Added Rows\n\n{rows}\n\n"));
        }

        if let Some(rows) = &self.removed_rows {
            results.push(format!("Removed Rows\n\n{rows}\n\n"));
        }

        if let Some(cols) = &self.added_cols {
            results.push(format!("Added Columns\n\n{cols}\n\n"));
        }

        if let Some(cols) = &self.removed_cols {
            results.push(format!("Removed Columns\n\n{cols}\n\n"));
        }
        write!(f, "{}", results.join("\n"))
    }
}
