use serde::{Deserialize, Serialize};

use crate::model::MetadataEntry;

use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct MetadataEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: MetadataEntry,
}
