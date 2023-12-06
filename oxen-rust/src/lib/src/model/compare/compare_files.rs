use crate::model::{EntryDataType, MetadataEntry};
use crate::view::entry::ResourceVersion;
use serde::{Deserialize, Serialize};

use super::tabular_compare::TabularCompare;
use super::tabular_compare_summary::TabularCompareSummary;
#[derive(Deserialize, Serialize, Debug)]

pub struct CompareFiles {
    pub status: String,
    pub data_type: EntryDataType,
    pub filename_1: String,
    pub filename_2: String,

    pub resource_1: Option<ResourceVersion>,
    pub resource_2: Option<ResourceVersion>,

    pub entry_1: Option<MetadataEntry>,
    pub entry_2: Option<MetadataEntry>,

    pub compare_summary: Option<TabularCompareSummary>,

    // Full Diff - can be a ton of data
    pub compare: Option<TabularCompare>,
}
