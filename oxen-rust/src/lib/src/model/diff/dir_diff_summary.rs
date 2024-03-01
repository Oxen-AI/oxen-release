use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    model::{diff::AddRemoveModifyCounts, EntryDataType},
    view::DataTypeCount,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirDiffSummary {
    pub dir: DirDiffSummaryImpl,
}

// Impl is so that we can wrap the json response in the "dir" field to make summaries easier to distinguish
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirDiffSummaryImpl {
    pub file_counts: AddRemoveModifyCounts,
    pub data_type_counts: Option<AddRemoveDataTypeCounts>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AddRemoveDataTypeCounts {
    pub added: Vec<DataTypeCount>,
    pub removed: Vec<DataTypeCount>,
}
