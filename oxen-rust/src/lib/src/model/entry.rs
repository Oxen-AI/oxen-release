use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Entry {
    pub id: String,
    pub data_type: String,
    pub url: String,
    pub filename: String,
    pub hash: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LocalEntry {
    pub id: String,
    pub is_synced: bool,
    pub hash: String,
    pub extension: String, // file extension
}
