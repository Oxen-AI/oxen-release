use polars::prelude::DataFrame;

pub struct DataFrameDiff {
    pub added_rows: Option<DataFrame>,
    pub removed_rows: Option<DataFrame>,
    pub added_cols: Option<DataFrame>,
    pub removed_cols: Option<DataFrame>,
}
