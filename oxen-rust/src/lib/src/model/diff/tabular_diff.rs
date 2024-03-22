use crate::model::schema::{Field, Schema};
use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

use super::AddRemoveModifyCounts;

#[derive(Debug, Clone)]
pub struct TabularSchemaDiff {
    pub added: Vec<Field>,
    pub removed: Vec<Field>,
}

#[derive(Debug, Clone)]
pub struct TabularDiffMods {
    pub row_counts: AddRemoveModifyCounts,
    pub col_changes: TabularSchemaDiff,
}

#[derive(Debug, Clone)]
pub struct TabularDiffSummary {
    pub modifications: TabularDiffMods,
    pub schema: Schema,
    pub dupes: TabularDiffDupes,
}

#[derive(Debug, Clone)]
pub struct TabularDiff {
    pub summary: TabularDiffSummary,
    pub parameters: TabularDiffParameters,
    pub contents: DataFrame,
}

#[derive(Debug, Clone)]
pub struct TabularDiffParameters {
    pub keys: Vec<String>,
    pub targets: Vec<String>,
    pub display: Vec<String>,
}

// Need to serialize here because we directly write this to disk to cache compares
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularDiffDupes {
    pub left: u64,
    pub right: u64,
}



impl Default for TabularSchemaDiff {
    fn default() -> Self {
        TabularSchemaDiff {
            added: Vec::new(),
            removed: Vec::new(),
        }
    }
}