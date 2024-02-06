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
    pub diff_rows: Option<JsonDataFrame>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareBody {
    pub compare_id: String,
    pub left: TabularCompareResourceBody,
    pub right: TabularCompareResourceBody,
    pub keys: Vec<TabularCompareFieldBody>,
    pub compare: Vec<TabularCompareFieldBody>,
    pub display: Vec<TabularCompareDisplayBody>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareResourceBody {
    pub path: String,
    pub version: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareFieldBody {
    pub left: String,
    pub right: String,
    pub alias: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareDisplayBody {
    pub left: Option<String>,
    pub right: Option<String>,
    pub compare_method: Option<String>,
}
