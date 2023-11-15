use serde::{Deserialize, Serialize};

use super::entry::ResourceVersion;
use crate::model::{Commit, Schema};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaWithPath {
    pub path: String,
    #[serde(flatten)]
    pub schema: Schema,
}

impl SchemaWithPath {
    pub fn new(path: String, schema: Schema) -> SchemaWithPath {
        SchemaWithPath { path, schema }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListSchemaResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub schemas: Vec<SchemaWithPath>,
    pub commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
}
