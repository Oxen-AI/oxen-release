use crate::model::ContentHashable;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub enum StagedEntryStatus {
    Added,
    Modified,
    Removed,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub enum StagedEntryType {
    Regular, // any old file
    Tabular, // file we want to track row level changes
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StagedEntry {
    pub hash: String,
    pub status: StagedEntryStatus,
    pub entry_type: StagedEntryType,
}

impl StagedEntry {
    pub fn empty_status(status: StagedEntryStatus) -> StagedEntry {
        StagedEntry {
            hash: String::from(""),
            status: status,
            entry_type: StagedEntryType::Regular,
        }
    }
}

impl ContentHashable for StagedEntry {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}
