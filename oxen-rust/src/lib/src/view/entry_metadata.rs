use serde::{Deserialize, Serialize};

use super::StatusMessage;
use crate::model::entry::metadata_entry::MetadataEntry;
use crate::view::entries::EMetadataEntry;

#[derive(Deserialize, Serialize, Debug)]
pub struct MetadataEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: MetadataEntry,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EMetadataEntryResponseView {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: EMetadataEntry,
}
