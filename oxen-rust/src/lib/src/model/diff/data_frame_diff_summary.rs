use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::{Commit, EntryDataType, MetadataEntry};

// KNOCK OUT WHAT A SUMMARY WOULD LOOK LIKE, AND THEN ADD THE FULL DIFF

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CountChange {
    pub added: usize,
    pub removed: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataFrameDiffSummary {
    pub row_counts: CountChange,
    pub column_counts: CountChange,
    pub has_schema_change: bool,
}
