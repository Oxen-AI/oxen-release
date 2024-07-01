use serde::{Deserialize, Serialize};

use time::OffsetDateTime;

use super::StatusMessage;
use crate::model::Commit;

#[derive(Deserialize, Serialize, Debug)]
pub struct NewWorkspace {
    pub workspace_id: String,
    pub branch_name: String,
    pub resource_path: Option<String>,
    pub entity_type: Option<String>,
}

// HACK to get this to work with our hub where we don't keep parent_ids 🤦‍♂️
// TODO: it should just be a Commit object
#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceCommit {
    pub id: String,
    pub message: String,
    pub author: String,
    pub email: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
}

impl From<WorkspaceCommit> for Commit {
    fn from(val: WorkspaceCommit) -> Self {
        Commit {
            id: val.id,
            message: val.message,
            author: val.author,
            email: val.email,
            timestamp: val.timestamp,
            parent_ids: vec![],
            root_hash: None,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceResponse {
    pub id: String,
    pub commit: WorkspaceCommit,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceResponseView {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub workspace: WorkspaceResponse,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListWorkspaceResponseView {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub workspaces: Vec<WorkspaceResponse>,
}
