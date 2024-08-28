use crate::model::ContentHashable;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub enum StagedEntryStatus {
    Added,
    Modified,
    Removed,
    Unmodified,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StagedEntry {
    pub hash: String,
    pub status: StagedEntryStatus,
}

impl StagedEntry {
    pub fn empty_status(status: StagedEntryStatus) -> StagedEntry {
        StagedEntry {
            hash: String::from(""),
            status,
        }
    }
}

impl ContentHashable for StagedEntry {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}
