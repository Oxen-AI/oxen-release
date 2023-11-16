use serde::{Deserialize, Serialize};

use crate::view::{schema::SchemaWithPath, JsonDataFrame};

use super::tabular_compare_summary::TabularCompareSummary;

#[derive(Deserialize, Serialize, Debug)]
pub struct TabularCompare {
    pub summary: TabularCompareSummary,

    pub schema_left: Option<SchemaWithPath>,
    pub schema_right: Option<SchemaWithPath>,

    pub keys: Vec<String>,
    pub targets: Vec<String>,
    pub match_rows: Option<JsonDataFrame>,
    // pub match_rows_view: Option<JsonDataFrameView>,
    pub diff_rows: Option<JsonDataFrame>,
    // pub diff_rows_view: Option<JsonDataFrameView>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareBody {
    pub left_resource: String,
    pub right_resource: String,
    pub keys: Vec<String>,
    pub targets: Vec<String>,
}
