use serde::{Deserialize, Serialize};

use crate::api;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{Commit, CommitEntry, EntryDataType, LocalRepository};
use crate::view::entry::ResourceVersion;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CLIMetadataEntry {
    pub filename: String,
    pub last_updated: Option<Commit>,
    // Hash of the file
    pub hash: String,
    // size of the file in bytes
    pub size: u64,
    // high level type of "image", "text", "video", "audio", "tabular"
    pub data_type: EntryDataType,
    // auto detected mime type of the file (e.g. "image/png")
    pub mime_type: String,
    // auto detected extension of the file
    pub extension: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataEntry {
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
    // metadata per data tyoe
    pub metadata: Option<GenericMetadata>,
    // If it's a tabular file, is it indexed for querying?
    pub is_queryable: Option<bool>,
}

impl MetadataEntry {
    pub fn from_commit_entry(
        repo: &LocalRepository,
        entry: Option<CommitEntry>,
        commit: &Commit,
    ) -> Option<MetadataEntry> {
        entry.as_ref()?;
        match api::local::metadata::from_commit_entry(repo, &entry.unwrap(), commit) {
            Ok(metadata) => Some(metadata),
            Err(_) => None,
        }
    }
}
