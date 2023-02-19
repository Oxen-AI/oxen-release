use serde::{Deserialize, Serialize};

use crate::model::{Commit, DiffEntry};

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareResponse {
    pub status: String,
    pub status_message: String,
    pub base_commit: Commit,
    pub head_commit: Commit,
    pub entries: Vec<DiffEntry>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}
