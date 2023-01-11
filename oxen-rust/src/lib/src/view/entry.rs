use crate::model::{CommitEntry, DirEntry, RemoteEntry};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct EntryResponse {
    pub status: String,
    pub status_message: String,
    pub entry: CommitEntry,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RemoteEntryResponse {
    pub status: String,
    pub status_message: String,
    pub entry: RemoteEntry,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ResourceVersion {
    pub path: String,
    pub version: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PaginatedEntries {
    pub status: String,
    pub status_message: String,
    pub entries: Vec<RemoteEntry>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PaginatedDirEntries {
    pub status: String,
    pub status_message: String,
    pub entries: Vec<DirEntry>,
    pub resource: ResourceVersion,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}
