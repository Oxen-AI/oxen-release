use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{entry::ResourceVersion, StatusMessage};

#[derive(Deserialize, Serialize, Debug)]
pub struct FileMetaData {
    pub size: u64,
    pub data_type: String,
    pub resource: ResourceVersion,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FileMetaDataResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub meta: FileMetaData,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FilePathsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub paths: Vec<PathBuf>,
}
