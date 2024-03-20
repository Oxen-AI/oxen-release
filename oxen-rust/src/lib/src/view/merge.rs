use serde::{Deserialize, Serialize};

use crate::model::Commit;

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeConflictFile {
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mergeable {
    pub is_mergeable: bool,
    pub conflicts: Vec<MergeConflictFile>,
    pub commits: Vec<Commit>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeableResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub mergeable: Mergeable,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeSuccessResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub head_commit: String,
    pub base_commit: String,
}
