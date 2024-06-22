use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct DataFramePayload {
    pub is_indexed: bool,
}
