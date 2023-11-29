use std::collections::HashMap;
use std::path::PathBuf;

use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

use crate::model::{Commit, CommitEntry, DataFrameSize, DiffEntry, Schema};
use crate::view::Pagination;

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AddRemoveModifyCounts {
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareCommits {
    pub base_commit: Commit,
    pub head_commit: Commit,
    pub commits: Vec<Commit>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareCommitsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub pagination: Pagination,
    // Wrap everything else in a compare object
    pub compare: CompareCommits,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareEntries {
    pub base_commit: Commit,
    pub head_commit: Commit,
    pub counts: AddRemoveModifyCounts,
    pub entries: Vec<DiffEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub compare: DiffEntry,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareEntriesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub pagination: Pagination,
    pub compare: CompareEntries,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareTabularResponse {
    pub dfs: CompareTabular,
    pub status: StatusMessage,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareTabular {
    pub source: HashMap<String, CompareSourceDF>,
    pub derived: HashMap<String, CompareDerivedDF>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareSourceDF {
    pub name: String,
    pub path: PathBuf,
    pub version: String, // Commit id or branch name
    pub schema: Schema,
    pub size: DataFrameSize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareVirtualResource {
    // TODO: Maybe this should be common to all v resource types - diffs, queries, etc.
    pub url: String,
    pub base: String,
    pub head: String,
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareDerivedDF {
    pub name: String,
    pub size: DataFrameSize,
    pub schema: Schema,
    pub version: String,
    pub resource: Option<CompareVirtualResource>, // None for direct CLI compare creation
}

impl CompareSourceDF {
    pub fn from_name_df_entry_schema(
        name: &str,
        df: DataFrame,
        entry: &CommitEntry,
        schema: Schema,
    ) -> CompareSourceDF {
        CompareSourceDF {
            name: name.to_owned(),
            path: entry.path.clone(),
            version: entry.commit_id.clone(),
            schema,
            size: DataFrameSize {
                height: df.height(),
                width: df.width(),
            },
        }
    }
}

impl CompareDerivedDF {
    pub fn from_compare_info(
        name: &str,
        compare_id: Option<&str>,
        left_commit_id: &str,
        right_commit_id: &str,
        df: DataFrame,
        schema: Schema,
    ) -> CompareDerivedDF {
        let resource = compare_id.map(|compare_id| CompareVirtualResource {
            url: format!("/compare/{}/", compare_id),
            base: left_commit_id.to_owned(),
            head: right_commit_id.to_owned(),
            path: name.to_owned(),
        });

        CompareDerivedDF {
            name: name.to_owned(),
            size: DataFrameSize {
                height: df.height(),
                width: df.width(),
            },
            schema,
            version: "derived".to_owned(),
            resource,
        }
    }
}
