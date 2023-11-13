use crate::model::ContentHashable;
use serde::{Deserialize, Serialize};

use super::Schema;

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub enum StagedSchemaStatus {
    Added,
    Modified,
    Removed,
}

// TODONOW: Do we need to include actual schema information on here
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StagedSchema {
    pub schema: Schema,
    pub status: StagedSchemaStatus,
}

impl ContentHashable for StagedSchema {
    fn content_hash(&self) -> String {
        self.schema.hash.clone()
    }
}
