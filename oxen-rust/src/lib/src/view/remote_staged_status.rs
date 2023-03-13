use serde::{Deserialize, Serialize};

use crate::model::ModEntry;

use super::{JsonDataFrame, PaginatedDirEntries};

#[derive(Deserialize, Serialize, Debug)]
pub struct StagedFileModResponse {
    pub status: String,
    pub status_message: String,
    pub modification: ModEntry,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListStagedFileModResponseRaw {
    pub status: String,
    pub status_message: String,
    pub modifications: Vec<ModEntry>,
    pub page_number: usize,
    pub page_size: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RemoteStagedStatus {
    pub added_files: PaginatedDirEntries,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RemoteStagedStatusResponse {
    pub status: String,
    pub status_message: String,
    pub staged: RemoteStagedStatus,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct StagedDFModifications {
    pub added: Option<JsonDataFrame>,
    // TODO: add other types
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListStagedFileModResponseDF {
    pub status: String,
    pub status_message: String,
    pub modifications: StagedDFModifications,
}
