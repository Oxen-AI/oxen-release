
use serde::{Deserialize, Serialize};

use crate::view::compare::AddRemoveModifyCounts;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirDiffSummary {
    pub file_counts: AddRemoveModifyCounts,
}
