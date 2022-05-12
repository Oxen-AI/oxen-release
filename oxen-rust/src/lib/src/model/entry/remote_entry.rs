use crate::model::CommitEntry;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoteEntry {
    pub id: String,
    pub filename: String,
    pub hash: String,
}

impl RemoteEntry {
    pub fn from_commit_entry(entry: &CommitEntry) -> RemoteEntry {
        RemoteEntry {
            id: entry.id.to_owned(),
            filename: String::from(entry.path.to_str().unwrap()),
            hash: entry.hash.to_owned(),
        }
    }
}
