use crate::model::{ContentHashable, StagedEntryStatus};
use serde::{Deserialize, Serialize};

use super::Schema;

// TODONOW: maybe come back to this 
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
    pub status: StagedEntryStatus,
}

impl ContentHashable for StagedSchema {
    fn content_hash(&self) -> String {
        self.schema.hash.clone()
    }
}
