use crate::model::{LocalRepository, RemoteEntry};
use crate::util;

use filetime::FileTime;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::{PathBuf, Path};
use std::hash::{Hash, Hasher};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CommitEntry {
    pub id: String,
    pub commit_id: String, // need commit_id to restore
    pub path: PathBuf,
    pub is_synced: bool,
    pub hash: String,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
}

// Hash on the path field so we can quickly look up
impl PartialEq for CommitEntry {
    fn eq(&self, other: &CommitEntry) -> bool {
        self.path == other.path
    }
}
impl Eq for CommitEntry {}
impl Hash for CommitEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl CommitEntry {
    // For HashSet search purposes
    pub fn from_path<T: AsRef<Path>>(path: T) -> CommitEntry {
        CommitEntry {
            id: String::from(""),
            commit_id: String::from(""),
            path: path.as_ref().to_path_buf(),
            is_synced: false,
            hash: String::from(""),
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
        }
    }

    pub fn version_file(&self) -> PathBuf {
        let current_dir = env::current_dir().unwrap();
        let repo_dir = util::fs::get_repo_root(&current_dir).expect("Oxen repo not found.");
        let repo = LocalRepository::from_dir(&repo_dir).unwrap();
        util::fs::version_path(&repo, self)
    }

    pub fn filename(&self) -> PathBuf {
        PathBuf::from(format!("{}.{}", self.commit_id, self.extension()))
    }

    pub fn filename_from_commit_id(&self, commit_id: &str) -> PathBuf {
        PathBuf::from(format!("{}.{}", commit_id, self.extension()))
    }

    pub fn extension(&self) -> String {
        String::from(self.path.extension().unwrap().to_str().unwrap())
    }

    pub fn to_synced(&self) -> CommitEntry {
        CommitEntry {
            id: self.id.to_owned(),
            commit_id: self.commit_id.to_owned(),
            path: self.path.to_owned(),
            is_synced: true,
            hash: self.hash.to_owned(),
            last_modified_seconds: self.last_modified_seconds,
            last_modified_nanoseconds: self.last_modified_nanoseconds,
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

    pub fn has_different_modification_time(&self, time: &FileTime) -> bool {
        self.last_modified_nanoseconds != time.nanoseconds()
            || self.last_modified_seconds != time.unix_seconds()
    }
}
