use polars::prelude::DataFrame;

pub struct DataFrameDiff {
    pub added: DataFrame,
    pub removed: DataFrame,
}
