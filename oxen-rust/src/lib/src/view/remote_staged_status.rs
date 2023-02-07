use serde::{Deserialize, Serialize};

use crate::model::ModEntry;

use super::PaginatedDirEntries;

#[derive(Deserialize, Serialize, Debug)]
pub struct StagedFileModResponse {
    pub status: String,
    pub status_message: String,
    pub modification: ModEntry,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListStagedFileModResponse {
    pub status: String,
    pub status_message: String,
    pub modifications: Vec<ModEntry>,
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
