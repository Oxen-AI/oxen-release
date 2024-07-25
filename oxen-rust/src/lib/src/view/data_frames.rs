use serde::{Deserialize, Serialize};

pub mod columns;

#[derive(Deserialize, Serialize, Debug)]
pub struct DataFramePayload {
    pub is_indexed: bool,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DataFrameColumnChange {
    pub column_name: String,
    pub column_data_type: Option<String>,
    pub operation: String,
    pub new_name: Option<String>,
    pub new_data_type: Option<String>,
}
