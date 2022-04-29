use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CommitEntry {
    pub id: String,
    pub is_synced: bool,
    pub hash: String,
    pub commit_id: String,
    pub extension: String, // file extension
}

impl CommitEntry {
    pub fn filename(&self) -> PathBuf {
        PathBuf::from(format!("{}.{}", self.commit_id, self.extension))
    }
}
