use crate::model::{Commit, CommitStats};
use crate::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct CommitResponse {
    pub status: String,
    pub status_message: String,
    pub commit: Commit,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CommitParentsResponse {
    pub status: String,
    pub status_message: String,
    pub parents: Vec<Commit>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CommitStatsResponse {
    pub status: String,
    pub status_message: String,
    pub stats: CommitStats,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListCommitResponse {
    pub status: String,
    pub status_message: String,
    pub commits: Vec<Commit>,
}

impl ListCommitResponse {
    pub fn success(commits: Vec<Commit>) -> ListCommitResponse {
        ListCommitResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_FOUND),
            commits,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct PaginatedCommits {
    pub entries: Vec<Commit>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}
