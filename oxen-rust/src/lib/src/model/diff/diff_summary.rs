use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::{Commit, EntryDataType, MetadataEntry};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DiffSummary {
    pub data_type: EntryDataType,
    pub data_frame: Option<DataFrameDiffSummary>,
}
