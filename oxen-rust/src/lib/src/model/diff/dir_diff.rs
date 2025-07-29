use serde::{Deserialize, Serialize};

use super::dir_diff_summary::DirDiffSummary;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirDiff {
    #[serde(flatten)]
    pub summary: DirDiffSummary,
}
