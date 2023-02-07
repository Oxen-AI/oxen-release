use serde::{Deserialize, Serialize};

use crate::model::AppendEntry;

use super::PaginatedDirEntries;

#[derive(Deserialize, Serialize, Debug)]
pub struct StagedFileAppendResponse {
    pub status: String,
    pub status_message: String,
    pub append: AppendEntry,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListStagedFileAppendResponse {
    pub status: String,
    pub status_message: String,
    pub appends: Vec<AppendEntry>,
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
