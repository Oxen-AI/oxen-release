use crate::model::ContentHashable;
use crate::model::EntryType;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub enum StagedEntryStatus {
    Added,
    Modified,
    Removed,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StagedEntry {
    pub hash: String,
    pub status: StagedEntryStatus,
    pub entry_type: EntryType,
}

impl StagedEntry {
    pub fn empty_status(status: StagedEntryStatus) -> StagedEntry {
        StagedEntry {
            hash: String::from(""),
            status,
            entry_type: EntryType::Regular,
        }
    }
}

impl ContentHashable for StagedEntry {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}
