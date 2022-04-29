use crate::model::{CommitEntry, RemoteEntry};
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

#[derive(Deserialize, Debug)]
pub struct PaginatedEntries {
    pub entries: Vec<CommitEntry>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}
