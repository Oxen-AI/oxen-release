use crate::model::CommitEntry;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoteEntry {
    pub filename: String,
    pub hash: String,
}

impl RemoteEntry {
    pub fn from_commit_entry(entry: &CommitEntry) -> RemoteEntry {
        RemoteEntry {
            filename: String::from(entry.path.to_str().unwrap()),
            hash: entry.hash.to_owned(),
        }
    }
}
