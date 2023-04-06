use crate::{
    model::{CommitEntry, DirEntry, RemoteEntry},
    util,
};
use serde::{Deserialize, Serialize};

use super::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};

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
    pub entries: Vec<DirEntry>,
    pub resource: Option<ResourceVersion>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

impl PaginatedDirEntries {
    pub fn from_entries(
        entries: Vec<DirEntry>,
        resource: Option<ResourceVersion>,
        page_num: usize,
        page_size: usize,
        total: usize,
    ) -> PaginatedDirEntries {
        log::debug!(
            "PaginatedDirEntries::from_entries entries.len() {} page_num {} page_size {} total {} ",
            entries.len(),
            page_num,
            page_size,
            total
        );
        let total_entries = total;
        let total_pages = (total_entries as f64 / page_size as f64).ceil() as u64;
        let paginated = util::paginate(entries, page_num, page_size);
        PaginatedDirEntries {
            entries: paginated,
            resource,
            page_size,
            page_number: page_num,
            total_pages: total_pages as usize,
            total_entries: total,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PaginatedDirEntriesResponse {
    pub status: String,
    pub status_message: String,
    pub entries: Vec<DirEntry>,
    pub resource: Option<ResourceVersion>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

impl PaginatedDirEntriesResponse {
    pub fn ok_from(paginated: PaginatedDirEntries) -> Self {
        Self {
            status: STATUS_SUCCESS.to_string(),
            status_message: MSG_RESOURCE_FOUND.to_string(),
            entries: paginated.entries,
            resource: paginated.resource,
            page_size: paginated.page_size,
            page_number: paginated.page_number,
            total_pages: paginated.total_pages,
            total_entries: paginated.total_entries,
        }
    }
}
