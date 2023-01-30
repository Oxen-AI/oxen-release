use std::path::PathBuf;

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
    // Not sure this is the best solution...but works for now
    // virtual tmp file name that we use to stage tmp data that isn't in the correct file path
    // ex) server driven commits
    pub tmp_file: Option<PathBuf>
}

impl StagedEntry {
    pub fn empty_status(status: StagedEntryStatus) -> StagedEntry {
        StagedEntry {
            hash: String::from(""),
            status,
            tmp_file: None
        }
    }
}

impl ContentHashable for StagedEntry {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}
