use serde::{Deserialize, Serialize};

use crate::model::{Commit, Schema};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct ListSchemaResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub schemas: Vec<Schema>,
    pub commit: Option<Commit>,
}
