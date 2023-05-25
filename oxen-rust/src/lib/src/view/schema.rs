use serde::{Deserialize, Serialize};

use crate::model::Schema;

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub schema: Schema,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListSchemaResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub schemas: Vec<Schema>,
}
