use super::StatusMessage;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionFile {
    pub hash: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionFileResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub version: VersionFile,
}
