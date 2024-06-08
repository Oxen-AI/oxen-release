use serde::{Deserialize, Serialize};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct ParseResourceResponse {
    pub status: StatusMessage,
    pub commit_id: String,
    pub branch_name: String,
    pub resource: String,
}
