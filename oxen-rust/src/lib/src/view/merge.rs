use serde::{Deserialize, Serialize};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeConflictFile {
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeableResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub is_mergeable: bool,
    pub conflicts: Vec<MergeConflictFile>,
}
