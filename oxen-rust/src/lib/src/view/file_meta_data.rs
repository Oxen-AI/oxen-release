use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct FileMetaData {
    pub size: u64,
    pub data_type: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FileMetaDataResponse {
    pub status: String,
    pub status_message: String,
    pub meta: FileMetaData,
}
