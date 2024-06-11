use serde::{Deserialize, Serialize};

use crate::model::{MetadataEntry, ParsedResource};

use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct MetadataEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub resource: ParsedResource,
    pub entry: MetadataEntry,
}
