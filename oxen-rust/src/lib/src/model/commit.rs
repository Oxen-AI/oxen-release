use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NewCommit {
    pub parent_ids: Vec<String>,
    pub message: String,
    pub author: String,
    pub email: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
}

impl NewCommit {
    pub fn from_commit(commit: &Commit) -> NewCommit {
        NewCommit {
            parent_ids: commit.parent_ids.to_owned(),
            message: commit.message.to_owned(),
            author: commit.author.to_owned(),
            email: commit.email.to_owned(),
            timestamp: commit.timestamp.to_owned(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Commit {
    pub id: String,
    pub parent_ids: Vec<String>,
    pub message: String,
    pub author: String,
    pub email: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
}

// Hash on the id field so we can quickly look up
impl PartialEq for Commit {
    fn eq(&self, other: &Commit) -> bool {
        self.id == other.id
    }
}
impl Eq for Commit {}
impl Hash for Commit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Commit {
    pub fn from_new_and_id(new_commit: &NewCommit, id: String) -> Commit {
        Commit {
            id,
            parent_ids: new_commit.parent_ids.to_owned(),
            message: new_commit.message.to_owned(),
            author: new_commit.author.to_owned(),
            email: new_commit.email.to_owned(),
            timestamp: new_commit.timestamp.to_owned(),
        }
    }

    pub fn to_uri_encoded(&self) -> String {
        serde_url_params::to_string(&self).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommitStats {
    pub commit: Commit,
    pub num_entries: usize, // this is how many entries are in our commit db
    pub num_synced_files: usize, // this is how many files are actually synced (in case we killed)
}

impl CommitStats {
    pub fn is_synced(&self) -> bool {
        self.num_entries == self.num_synced_files
    }
}
