use crate::model::RemoteEntry;

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CommitEntry {
    pub id: String,
    pub path: PathBuf,
    pub is_synced: bool,
    pub hash: String,
    pub commit_id: String,
    pub extension: String,
}

impl CommitEntry {
    pub fn from_remote_and_commit_id(remote: &RemoteEntry, commit_id: &str) -> CommitEntry {
        let path = PathBuf::from(remote.filename.to_owned());
        // assuming extension is valid if we got it from remote
        let extension = path.extension().unwrap().to_str().unwrap();
        CommitEntry {
            id: remote.id.to_owned(),
            path: path.to_owned(),
            is_synced: true,
            hash: remote.hash.to_owned(),
            commit_id: commit_id.to_string(),
            extension: extension.to_string(),
        }
    }

    pub fn filename(&self) -> PathBuf {
        PathBuf::from(format!("{}.{}", self.commit_id, self.extension))
    }

    pub fn to_synced(&self) -> CommitEntry {
        CommitEntry {
            id: self.id.to_owned(),
            path: self.path.to_owned(),
            is_synced: true,
            hash: self.hash.to_owned(),
            commit_id: self.commit_id.to_owned(),
            extension: self.extension.to_owned(),
        }
    }

    pub fn to_remote(&self) -> RemoteEntry {
        RemoteEntry {
            id: self.id.to_owned(),
            filename: self.path.to_str().unwrap_or("").to_string(),
            hash: self.hash.to_owned(),
        }
    }

    pub fn to_uri_encoded(&self) -> String {
        serde_url_params::to_string(&self).unwrap()
    }
}
