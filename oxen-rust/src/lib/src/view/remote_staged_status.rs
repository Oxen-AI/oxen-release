use serde::{Deserialize, Serialize};

use crate::{
    model::{DirEntry, LocalRepository, ModEntry, StagedData, SummarizedStagedDirStats},
    util,
};

use super::{JsonDataFrame, PaginatedDirEntries};

#[derive(Deserialize, Serialize, Debug)]
pub struct StagedFileModResponse {
    pub status: String,
    pub status_message: String,
    pub modification: ModEntry,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListStagedFileModResponseRaw {
    pub status: String,
    pub status_message: String,
    pub modifications: Vec<ModEntry>,
    pub page_number: usize,
    pub page_size: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RemoteStagedStatus {
    pub added_dirs: SummarizedStagedDirStats,
    pub added_files: PaginatedDirEntries,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RemoteStagedStatusResponse {
    pub status: String,
    pub status_message: String,
    pub staged: RemoteStagedStatus,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct StagedDFModifications {
    pub added: Option<JsonDataFrame>,
    // TODO: add other types
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListStagedFileModResponseDF {
    pub status: String,
    pub status_message: String,
    pub modifications: StagedDFModifications,
}

impl RemoteStagedStatus {
    pub fn from_staged(
        repo: &LocalRepository,
        staged: &StagedData,
        page_num: usize,
        page_size: usize,
    ) -> RemoteStagedStatus {
        let entries: Vec<DirEntry> = staged
            .added_files
            .keys()
            .map(|path| {
                let full_path = repo.path.join(path);
                let meta = std::fs::metadata(&full_path).unwrap();
                let path_str = path.to_string_lossy().to_string();

                DirEntry {
                    filename: path_str,
                    is_dir: false,
                    size: meta.len(),
                    latest_commit: None,
                    datatype: util::fs::file_datatype(&full_path),
                    resource: None, // not committed so does not have a resource
                }
            })
            .collect();

        let added_paginated = RemoteStagedStatus::paginate_entries(entries, page_num, page_size);

        RemoteStagedStatus {
            added_dirs: staged.added_dirs.to_owned(),
            added_files: added_paginated,
        }
    }

    fn paginate_entries(
        entries: Vec<DirEntry>,
        page_number: usize,
        page_size: usize,
    ) -> PaginatedDirEntries {
        let total_entries = entries.len();
        let total_pages = (total_entries as f64 / page_size as f64).ceil() as usize;
        let paginated = util::paginate(entries, page_number, page_size);

        PaginatedDirEntries {
            entries: paginated,
            page_number,
            page_size,
            total_pages,
            total_entries,
            resource: None,
        }
    }
}
