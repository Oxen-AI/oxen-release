use crate::model::schema::{Field, Schema};
use polars::frame::DataFrame;

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
}

#[derive(Debug, Clone)]
pub struct TabularDiff {
    pub summary: TabularDiffSummary,
    pub contents: DataFrame,
}
