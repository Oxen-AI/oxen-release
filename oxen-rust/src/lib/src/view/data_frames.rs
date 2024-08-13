use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod columns;

#[derive(Deserialize, Serialize, Debug)]
pub struct DataFramePayload {
    pub is_indexed: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataFrameColumnChange {
    pub column_name: String,
    pub column_data_type: Option<String>,
    pub operation: String,
    pub new_name: Option<String>,
    pub new_data_type: Option<String>,
}
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataFrameRowChange {
    pub row_id: String,
    pub operation: String,
    pub value: Value,
    pub new_value: Option<String>,
}
