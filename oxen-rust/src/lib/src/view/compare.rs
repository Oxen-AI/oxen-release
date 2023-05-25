use serde::{Deserialize, Serialize};

use crate::model::{Commit, DiffEntry};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub base_commit: Commit,
    pub head_commit: Commit,
    pub entries: Vec<DiffEntry>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}
