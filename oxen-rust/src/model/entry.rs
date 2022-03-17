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
