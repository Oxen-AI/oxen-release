use crate::model::EntryDataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EntryMetaData {
    pub hash: u128,
    pub num_bytes: u64,
    pub data_type: EntryDataType,
}

impl Default for EntryMetaData {
    fn default() -> Self {
        EntryMetaData {
            hash: 0,
            num_bytes: 0,
            data_type: EntryDataType::Binary,
        }
    }
}
