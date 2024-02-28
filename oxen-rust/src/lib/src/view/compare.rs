use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

use crate::constants::DIFF_STATUS_COL;
use crate::error::OxenError;
use crate::message::{MessageLevel, OxenMessage};
use crate::model::compare::tabular_compare::{TabularCompareFieldBody, TabularCompareTargetBody};
use crate::model::diff::text_diff::TextDiff;
use crate::model::diff::AddRemoveModifyCounts;
use crate::model::{Commit, DiffEntry, Schema};
use crate::view::Pagination;

use super::StatusMessage;

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

#[derive(Debug)]
pub struct CompareTabularWithDF {
    pub diff_df: DataFrame,
    pub dupes: CompareDupes,
    pub schema_diff: Option<CompareSchemaDiff>,
    pub summary: Option<CompareSummary>,
    pub keys: Vec<TabularCompareFieldBody>,
    pub targets: Vec<TabularCompareTargetBody>,
    pub display: Vec<TabularCompareTargetBody>,
    pub source_schemas: CompareSourceSchemas,
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

// #[derive(Serialize, Deserialize, Debug)]
// pub struct CompareSourceDF {
//     pub name: String,
//     pub path: PathBuf,
//     pub version: String, // Commit id or branch name
//     pub schema: Schema,
//     pub size: DataFrameSize,
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareSchemaColumn {
    pub name: String,
    pub key: String,
    pub dtype: String,
}

// TODONOW these should maybe be moved to a model

#[derive(Debug)]
pub enum CompareResult {
    Tabular((CompareTabular, DataFrame)),
    Text(TextDiff),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareTabular {
    pub dupes: CompareDupes,
    pub summary: Option<CompareSummary>,
    pub schema_diff: Option<CompareSchemaDiff>,
    pub source_schemas: CompareSourceSchemas,
    pub keys: Option<Vec<TabularCompareFieldBody>>,
    pub targets: Option<Vec<TabularCompareTargetBody>>,
    pub display: Option<Vec<TabularCompareTargetBody>>,
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
pub struct CompareSourceDFs {
    pub left: CompareSourceDF,
    pub right: CompareSourceDF,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct CompareSourceDF {
    pub path: String,
    pub version: Option<String>, // Needs to be option for py / CLI
    pub schema: Schema,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareSourceSchemas {
    pub left: Schema,
    pub right: Schema,
}

impl CompareDupes {
    pub fn empty() -> CompareDupes {
        CompareDupes { left: 0, right: 0 }
    }

    pub fn to_message(&self) -> OxenMessage {
        OxenMessage {
            level: MessageLevel::Warning,
            title: "Duplicate keys".to_owned(),
            description: format!("This compare contains rows with duplicate keys. Results may be unexpected if keys are intended to be unique.\nLeft df duplicates: {}\nRight df duplicates: {}\n", self.left, self.right),
        }
    }
}

// impl CompareSourceDF {
//     pub fn from_name_df_entry_schema(
//         name: &str,
//         df: DataFrame,
//         entry: &CommitEntry,
//         schema: Schema,
//     ) -> CompareSourceDF {
//         CompareSourceDF {
//             name: name.to_owned(),
//             path: entry.path.clone(),
//             version: entry.commit_id.clone(),
//             schema,
//             size: DataFrameSize {
//                 height: df.height(),
//                 width: df.width(),
//             },
//         }
//     }
// }

impl CompareSummary {
    pub fn from_diff_df(df: &DataFrame) -> Result<CompareSummary, OxenError> {
        // TODO optimization: can this be done in one pass?
        let added_rows = df
            .column(DIFF_STATUS_COL)?
            .str()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "added").unwrap_or(false))
            .count();

        let removed_rows = df
            .column(DIFF_STATUS_COL)?
            .str()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "removed").unwrap_or(false))
            .count();

        let modified_rows = df
            .column(DIFF_STATUS_COL)?
            .str()?
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

impl CompareTabular {
    pub fn from_with_df_and_source_schemas(
        with_df: &CompareTabularWithDF,
        source_schemas: CompareSourceSchemas,
    ) -> CompareTabular {
        CompareTabular {
            dupes: with_df.dupes.clone(),
            schema_diff: with_df.schema_diff.clone(),
            summary: with_df.summary.clone(),
            keys: Some(with_df.keys.clone()),
            targets: Some(with_df.targets.clone()),
            display: Some(with_df.display.clone()),
            source_schemas,
        }
    }
    pub fn from_with_df(with_df: &CompareTabularWithDF) -> CompareTabular {
        CompareTabular {
            dupes: with_df.dupes.clone(),
            schema_diff: with_df.schema_diff.clone(),
            summary: with_df.summary.clone(),
            keys: Some(with_df.keys.clone()),
            targets: Some(with_df.targets.clone()),
            display: Some(with_df.display.clone()),
            source_schemas: with_df.source_schemas.clone(),
        }
    }
}
