use serde::{Deserialize, Serialize};

use crate::model::Schema;

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaResponse {
    pub status: String,
    pub status_message: String,
    pub schema: Schema,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListSchemaResponse {
    pub status: String,
    pub status_message: String,
    pub schemas: Vec<Schema>,
}
