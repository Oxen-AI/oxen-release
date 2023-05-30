use crate::model::{Commit, CommitStats};
use serde::{Deserialize, Serialize};

use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct CommitResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub commit: Commit,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CommitStatsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub stats: CommitStats,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListCommitResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub commits: Vec<Commit>,
}

impl ListCommitResponse {
    pub fn success(commits: Vec<Commit>) -> ListCommitResponse {
        ListCommitResponse {
            status: StatusMessage::resource_found(),
            commits,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct PaginatedCommits {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entries: Vec<Commit>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}
