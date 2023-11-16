use serde::{Deserialize, Serialize};
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareSummary {
    pub num_left_only_rows: usize,
    pub num_right_only_rows: usize,
    pub num_diff_rows: usize,
    pub num_match_rows: usize,
}
