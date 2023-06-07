use serde::{Deserialize, Serialize};

use crate::model::{Commit, EntryDataType};
use crate::view::entry::ResourceVersion;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StatsEntry {
    pub filename: String,
    pub is_dir: bool,
    pub latest_commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
    // size of the file in bytes
    pub size: u64,
    // high level type of "image", "text", "video", "audio", "tabular"
    pub data_type: EntryDataType,
    // auto detected mime type of the file (e.g. "image/png")
    pub mime_type: String,
    // auto detected extension of the file
    pub extension: String,
    // type specific meta data
    pub meta: MetaData,
}
