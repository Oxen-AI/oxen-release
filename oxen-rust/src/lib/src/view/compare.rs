use std::collections::HashMap;
use std::path::PathBuf;

use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

use crate::constants::DIFF_STATUS_COL;
use crate::error::OxenError;
use crate::message::{MessageLevel, OxenMessage};
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
    #[serde(flatten)]
    pub status: StatusMessage,
    pub messages: Vec<OxenMessage>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareTabular {
    pub source: HashMap<String, CompareSourceDF>,
    pub derived: HashMap<String, CompareDerivedDF>,
    pub dupes: CompareDupes,
    pub schema_diff: Option<CompareSchemaDiff>,
    pub summary: Option<CompareSummary>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareSchemaDiff {
    pub added_cols: Vec<CompareSchemaColumn>,
    pub removed_cols: Vec<CompareSchemaColumn>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareSummary {
    pub modifications: CompareTabularMods,
    pub schema: Schema,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareTabularMods {
    pub added_rows: usize,
    pub removed_rows: usize,
    pub modified_rows: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareDisplayFields {
    pub left: Vec<String>,
    pub right: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareDupes {
    pub left: u64,
    pub right: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareSourceDF {
    pub name: String,
    pub path: PathBuf,
    pub version: String, // Commit id or branch name
    pub schema: Schema,
    pub size: DataFrameSize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareSchemaColumn {
    pub name: String,
    pub key: String,
    pub dtype: String,
}

#[derive(Debug)]
pub enum CompareResult {
    Tabular((CompareTabular, DataFrame)),
    Text(String),
}

pub struct CompareTabularRaw {
    pub diff_df: DataFrame,
    pub dupes: CompareDupes,
    pub compare_summary: Option<CompareSummary>,
    pub schema_diff: Option<CompareSchemaDiff>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareVirtualResource {
    // TODO: Maybe this should be common to all v resource types - diffs, queries, etc.
    pub path: String,
    pub base: String,
    pub head: String,
    pub resource: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareDerivedDF {
    pub name: String,
    pub size: DataFrameSize,
    pub schema: Schema,
    pub resource: Option<CompareVirtualResource>, // None for direct CLI compare creation
}

impl CompareDupes {
    pub fn empty() -> CompareDupes {
        CompareDupes { left: 0, right: 0 }
    }

    pub fn to_message(&self) -> OxenMessage {
        OxenMessage {
            level: MessageLevel::Warning,
            message: format!("This compare contains rows with duplicate keys. Results may be unexpected if keys are intended to be unique.\nLeft df duplicates: {}\nRight df duplicates: {}\n", self.left, self.right),
        }
    }
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
        left_commit_entry: Option<&CommitEntry>,
        right_commit_entry: Option<&CommitEntry>,
        df: DataFrame,
        schema: Schema,
    ) -> CompareDerivedDF {
        let resource = match (compare_id, left_commit_entry, right_commit_entry) {
            (Some(compare_id), Some(left_commit_entry), Some(right_commit_entry)) => {
                Some(CompareVirtualResource {
                    path: format!(
                        "/compare/data_frame/{}/{}/{}..{}",
                        compare_id, name, left_commit_entry.commit_id, right_commit_entry.commit_id
                    ),
                    base: left_commit_entry.commit_id.to_owned(),
                    head: right_commit_entry.commit_id.to_owned(),
                    resource: format!("{}/{}", compare_id, name),
                })
            }
            _ => None,
        };

        CompareDerivedDF {
            name: name.to_owned(),
            size: DataFrameSize {
                height: df.height(),
                width: df.width(),
            },
            schema,
            resource,
        }
    }
}

impl CompareSummary {
    pub fn from_diff_df(df: &DataFrame) -> Result<CompareSummary, OxenError> {
        // TODO optimization: can this be done in one pass?
        let added_rows = df
            .column(DIFF_STATUS_COL)?
            .utf8()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "added").unwrap_or(false))
            .count();

        let removed_rows = df
            .column(DIFF_STATUS_COL)?
            .utf8()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "removed").unwrap_or(false))
            .count();

        let modified_rows = df
            .column(DIFF_STATUS_COL)?
            .utf8()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "modified").unwrap_or(false))
            .count();

        Ok(CompareSummary {
            modifications: CompareTabularMods {
                added_rows,
                removed_rows,
                modified_rows,
            },
            schema: Schema::from_polars(&df.schema()),
        })
    }
}

impl CompareSchemaDiff {
    pub fn from_dfs(df1: &DataFrame, df2: &DataFrame) -> Result<CompareSchemaDiff, OxenError> {
        // Assuming CompareSchemaColumn and CompareSchemaDiff are defined elsewhere
        // and OxenError is a placeholder for error handling in your application.

        // Get added columns
        let added_cols = df2
            .schema()
            .iter_fields()
            .filter(|field| !df1.schema().contains(field.name()))
            .map(|field| {
                Ok(CompareSchemaColumn {
                    name: field.name().to_owned().to_string(),
                    key: format!("{}.right", field.name()),
                    dtype: format!("{:?}", field.data_type()),
                })
            })
            .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

        // Get removed columns
        let removed_cols = df1
            .schema()
            .iter_fields()
            .filter(|field| !df2.schema().contains(field.name()))
            .map(|field| {
                Ok(CompareSchemaColumn {
                    name: field.name().to_string(),
                    key: format!("{}.left", field.name()),
                    dtype: format!("{:?}", field.data_type()),
                })
            })
            .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

        Ok(CompareSchemaDiff {
            added_cols,
            removed_cols,
        })
    }

    pub fn from_schemas(s1: &Schema, s2: &Schema) -> Result<CompareSchemaDiff, OxenError> {
        let added_cols = s2
            .fields
            .iter()
            .filter(|field| !s1.fields.contains(field))
            .map(|field| {
                Ok(CompareSchemaColumn {
                    name: field.name.to_owned().to_string(),
                    key: format!("{}.right", field.name),
                    dtype: field.dtype.to_owned(),
                })
            })
            .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

        let removed_cols = s1
            .fields
            .iter()
            .filter(|field| !s2.fields.contains(field))
            .map(|field| {
                Ok(CompareSchemaColumn {
                    name: field.name.to_string(),
                    key: format!("{}.left", field.name),
                    dtype: field.dtype.to_string(),
                })
            })
            .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

        Ok(CompareSchemaDiff {
            added_cols,
            removed_cols,
        })
    }
}
