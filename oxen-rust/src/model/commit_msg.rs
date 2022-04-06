use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct CommitMsg {
    pub id: String,
    pub message: String,
}

#[derive(Deserialize, Debug)]
pub struct CommitMsgResponse {
    pub commit: CommitMsg,
}

#[derive(Deserialize, Debug)]
pub struct PaginatedCommitMsgs {
    pub entries: Vec<CommitMsg>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}
