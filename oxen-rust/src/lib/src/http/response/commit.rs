
use crate::model::CommitMsg;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct CommitMsgResponse {
    pub status: String,
    pub status_message: String,
    pub commit: CommitMsg,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListCommitMsgResponse {
    pub status: String,
    pub status_message: String,
    pub commits: Vec<CommitMsg>,
}

#[derive(Deserialize, Debug)]
pub struct PaginatedCommitMsgs {
    pub entries: Vec<CommitMsg>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}