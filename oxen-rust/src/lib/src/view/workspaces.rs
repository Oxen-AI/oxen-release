use serde::{Deserialize, Serialize};

use crate::model::Commit;

use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct NewWorkspace {
    pub workspace_id: String,
    pub branch_name: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceResponse {
    pub workspace_id: String,
    pub branch_name: String,
    pub commit: Commit,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceResponseView {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub workspace: WorkspaceResponse,
}
