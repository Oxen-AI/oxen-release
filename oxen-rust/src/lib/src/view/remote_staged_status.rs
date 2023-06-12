use std::{collections::HashMap, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    model::{
        entry::metadata_entry::MetaData, LocalRepository, MetaDataEntry, ModEntry, StagedData,
        StagedEntry, SummarizedStagedDirStats,
    },
    util,
};

use super::{JsonDataFrame, PaginatedDirEntries, StatusMessage};

#[derive(Deserialize, Serialize, Debug)]
pub struct StagedFileModResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub modification: ModEntry,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListStagedFileModResponseRaw {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub data_type: String,
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
    pub modified_files: PaginatedDirEntries,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RemoteStagedStatusResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub staged: RemoteStagedStatus,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct StagedDFModifications {
    pub added_rows: Option<JsonDataFrame>,
    // TODO: add other types
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListStagedFileModResponseDF {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub data_type: String,
    pub modifications: StagedDFModifications,
}

impl RemoteStagedStatus {
    pub fn from_staged(
        repo: &LocalRepository,
        staged: &StagedData,
        page_num: usize,
        page_size: usize,
    ) -> RemoteStagedStatus {
        let added_entries: Vec<MetaDataEntry> =
            RemoteStagedStatus::added_to_meta_entry(repo, &staged.added_files);
        let modified_entries: Vec<MetaDataEntry> =
            RemoteStagedStatus::modified_to_meta_entry(repo, &staged.modified_files);

        let added_paginated =
            RemoteStagedStatus::paginate_entries(added_entries, page_num, page_size);
        let modified_paginated =
            RemoteStagedStatus::paginate_entries(modified_entries, page_num, page_size);

        RemoteStagedStatus {
            added_dirs: staged.added_dirs.to_owned(),
            added_files: added_paginated,
            modified_files: modified_paginated,
        }
    }

    fn added_to_meta_entry(
        repo: &LocalRepository,
        entries: &HashMap<PathBuf, StagedEntry>,
    ) -> Vec<MetaDataEntry> {
        RemoteStagedStatus::iter_to_meta_entry(repo, entries.keys())
    }

    fn modified_to_meta_entry(repo: &LocalRepository, entries: &[PathBuf]) -> Vec<MetaDataEntry> {
        RemoteStagedStatus::iter_to_meta_entry(repo, entries.iter())
    }

    fn iter_to_meta_entry<'a, I: Iterator<Item = &'a PathBuf>>(
        repo: &LocalRepository,
        entries: I,
    ) -> Vec<MetaDataEntry> {
        entries
            .map(|path| {
                let full_path = repo.path.join(path);
                let len = match util::fs::metadata(&full_path) {
                    Ok(m) => m.len(),
                    Err(_) => 0,
                };
                let path_str = path.to_string_lossy().to_string();

                MetaDataEntry {
                    filename: path_str,
                    is_dir: false,
                    size: len,
                    latest_commit: None,
                    data_type: util::fs::file_data_type(&full_path),
                    mime_type: util::fs::file_mime_type(&full_path),
                    extension: util::fs::file_extension(&full_path),
                    resource: None, // not committed so does not have a resource
                    meta: MetaData {
                        text: None,
                        image: None,
                        video: None,
                        audio: None,
                        tabular: None,
                    },
                }
            })
            .collect()
    }

    fn paginate_entries(
        entries: Vec<MetaDataEntry>,
        page_number: usize,
        page_size: usize,
    ) -> PaginatedDirEntries {
        let total_entries = entries.len();
        let (paginated, total_pages) = util::paginate(entries, page_number, page_size);

        PaginatedDirEntries {
            status: StatusMessage::resource_found(),
            entries: paginated,
            page_number,
            page_size,
            total_pages,
            total_entries,
            resource: None,
        }
    }
}
