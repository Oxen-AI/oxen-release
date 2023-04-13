use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeConflictFile {
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeableResponse {
    pub status: String,
    pub status_message: String,
    pub is_mergeable: bool,
    pub conflicts: Vec<MergeConflictFile>,
}
