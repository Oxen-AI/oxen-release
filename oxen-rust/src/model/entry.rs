use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Entry {
    pub id: String,
    pub data_type: String,
    pub url: String,
    pub filename: String,
    pub hash: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EntryResponse {
    pub status: String,
    pub status_message: String,
    pub entry: Entry,
}

#[derive(Deserialize, Debug)]
pub struct PaginatedEntries {
    pub entries: Vec<Entry>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}
