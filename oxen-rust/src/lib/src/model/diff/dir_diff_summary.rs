use serde::{Deserialize, Serialize};

use crate::view::compare::AddRemoveModifyCounts;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirDiffSummary {
    pub dir: DirDiffSummaryImpl,
}

// Impl is so that we can wrap the json response in the "dir" field to make summaries easier to distinguish
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirDiffSummaryImpl {
    pub file_counts: AddRemoveModifyCounts,
}
