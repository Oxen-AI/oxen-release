use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Entry {
    pub id: String,
    pub data_type: String,
    pub url: String,
    pub hash: String,
}

#[derive(Deserialize, Debug)]
pub struct EntryResponse {
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

