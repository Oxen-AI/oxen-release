use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataTypeCount {
    pub count: usize,
    pub data_type: String,
}
