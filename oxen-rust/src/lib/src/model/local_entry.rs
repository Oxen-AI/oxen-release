
use crate::model::Commit;

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LocalEntry {
    pub id: String,
    pub is_synced: bool,
    pub hash: String,
    pub extension: String, // file extension
}

impl LocalEntry {
    pub fn file_from_commit(&self, commit: &Commit) -> PathBuf {
        PathBuf::from(format!("{}.{}", commit.id, self.extension))
    }
}