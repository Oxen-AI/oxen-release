use serde::{Deserialize, Serialize};

use crate::model::DirEntry;

#[derive(Deserialize, Serialize, Debug)]
pub struct EntryMetaDataResponse {
    pub status: String,
    pub status_message: String,
    pub entry: DirEntry,
}
