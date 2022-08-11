use crate::model::ContentHashable;
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
}

impl ContentHashable for StagedEntry {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}
