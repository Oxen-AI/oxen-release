use crate::model::{EntryDataType, MerkleHash, StagedEntryStatus};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EntryMetaData {
    pub hash: MerkleHash,
    pub num_bytes: u64,
    pub data_type: EntryDataType,
    pub status: StagedEntryStatus,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
}

impl Default for EntryMetaData {
    fn default() -> Self {
        EntryMetaData {
            hash: MerkleHash::new(0),
            num_bytes: 0,
            data_type: EntryDataType::Binary,
            status: StagedEntryStatus::Unmodified,
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
        }
    }
}

impl Display for EntryMetaData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EntryMetaData {{ hash: {}, num_bytes: {}, data_type: {:?}, status: {:?} }}",
            self.hash, self.num_bytes, self.data_type, self.status
        )
    }
}
