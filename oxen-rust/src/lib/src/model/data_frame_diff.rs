use datafusion::dataframe::DataFrame;
use std::sync::Arc;

pub struct DataFrameDiff {
    pub added: Arc<DataFrame>,
    pub removed: Arc<DataFrame>,
}
