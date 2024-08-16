use crate::model::EntryDataType;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

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

impl Display for EntryMetaData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EntryMetaData {{ hash: {:x}, num_bytes: {}, data_type: {:?} }}",
            self.hash, self.num_bytes, self.data_type
        )
    }
}
