use serde::{Deserialize, Serialize};

use crate::model::DirEntry;
use crate::view::entry::ResourceVersion;

#[derive(Deserialize, Serialize, Debug)]
pub struct EntryMetaDataResponse {
    pub status: String,
    pub status_message: String,
    pub entry: DirEntry,
    pub resource: ResourceVersion,
}
