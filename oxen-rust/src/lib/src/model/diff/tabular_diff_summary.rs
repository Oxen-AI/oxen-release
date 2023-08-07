
use serde::{Deserialize, Serialize};

use crate::model::DataFrameSize;


#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffSummary {
    pub base_size: DataFrameSize,
    pub head_size: DataFrameSize,
    pub schema_has_changed: bool,
}
