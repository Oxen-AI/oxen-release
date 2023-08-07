use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::{Commit, EntryDataType, MetadataEntry};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Diff {
    pub data_type: EntryDataType,
    pub summary: DiffSummary,
}
