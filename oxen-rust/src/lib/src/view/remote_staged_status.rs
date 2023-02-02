use serde::{Deserialize, Serialize};

use super::PaginatedDirEntries;

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
