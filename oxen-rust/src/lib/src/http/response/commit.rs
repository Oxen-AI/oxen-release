use crate::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
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

impl ListCommitMsgResponse {
    pub fn success(commits: Vec<CommitMsg>) -> ListCommitMsgResponse {
        ListCommitMsgResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_FOUND),
            commits: commits,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct PaginatedCommitMsgs {
    pub entries: Vec<CommitMsg>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}
