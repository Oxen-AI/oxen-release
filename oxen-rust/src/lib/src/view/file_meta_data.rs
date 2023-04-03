use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::entry::ResourceVersion;

#[derive(Deserialize, Serialize, Debug)]
pub struct FileMetaData {
    pub size: u64,
    pub data_type: String,
    pub resource: ResourceVersion,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FileMetaDataResponse {
    pub status: String,
    pub status_message: String,
    pub meta: FileMetaData,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FilePathsResponse {
    pub status: String,
    pub status_message: String,
    pub paths: Vec<PathBuf>,
}
