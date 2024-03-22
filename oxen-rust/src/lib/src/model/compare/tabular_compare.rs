use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{error::OxenError, model::diff::tabular_diff::TabularSchemaDiff, view::{schema::SchemaWithPath, JsonDataFrame}};

use super::tabular_compare_summary::TabularCompareSummary;

#[derive(Deserialize, Serialize, Debug)]
pub struct TabularCompare {
    pub summary: TabularCompareSummary,

    pub schema_left: Option<SchemaWithPath>,
    pub schema_right: Option<SchemaWithPath>,

    pub keys: Vec<String>,
    pub targets: Vec<String>,
    pub match_rows: Option<JsonDataFrame>,
    pub diff_rows: Option<JsonDataFrame>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareBody {
    pub compare_id: String,
    pub left: TabularCompareResourceBody,
    pub right: TabularCompareResourceBody,
    pub keys: Vec<TabularCompareFieldBody>,
    pub compare: Vec<TabularCompareTargetBody>,
    pub display: Vec<TabularCompareTargetBody>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareResourceBody {
    pub path: String,
    pub version: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareFieldBody {
    pub left: String,
    pub right: String,
    pub alias_as: Option<String>,
    pub compare_method: Option<String>,
}

impl TabularCompareFieldBody {
    pub fn to_string(&self) -> String {
        self.left.clone()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareFields {
    pub keys: Vec<TabularCompareFieldBody>,
    pub targets: Vec<TabularCompareTargetBody>,
    pub display: Vec<TabularCompareTargetBody>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareTargetBody {
    pub left: Option<String>,
    pub right: Option<String>,
    pub compare_method: Option<String>,
}
impl TabularCompareTargetBody {
    pub fn to_string(&self) -> Result<String, OxenError> {
        self.left.clone()
            .or_else(|| self.right.clone())
            .ok_or(OxenError::basic_str("Both 'left' and 'right' fields are None"))
    }
}

impl TabularCompareFields {
    pub fn from_lists_and_schema_diff(
        schema_diff: &TabularSchemaDiff,
        keys: Vec<&str>,
        targets: Vec<&str>,
        display: Vec<&str>,
    ) -> Self {
        let res_keys = keys
        .iter()
        .map(|key| TabularCompareFieldBody {
            left: key.to_string(),
            right: key.to_string(),
            alias_as: None,
            compare_method: None,
        })
        .collect::<Vec<TabularCompareFieldBody>>();

    let mut res_targets: Vec<TabularCompareTargetBody> = vec![];

    // Get added and removed as sets of strings
    let added_set: HashSet<String> = schema_diff.added.iter().map(|f| f.name.clone()).collect();
    let removed_set: HashSet<String> = schema_diff.removed.iter().map(|f| f.name.clone()).collect();

    for target in targets.iter() {
        if added_set.contains(&target.to_string()) {
            res_targets.push(TabularCompareTargetBody {
                left: None,
                right: Some(target.to_string()),
                compare_method: None,
            });
        } else if removed_set.contains(&target.to_string()) {
            res_targets.push(TabularCompareTargetBody {
                left: Some(target.to_string()),
                right: None,
                compare_method: None,
            });
        } else {
            res_targets.push(TabularCompareTargetBody {
                left: Some(target.to_string()),
                right: Some(target.to_string()),
                compare_method: None,
            });
        }
    }

    let mut res_display: Vec<TabularCompareTargetBody> = vec![];
    for disp in display.iter() {
        if added_set.contains(&disp.to_string()) {
            res_display.push(TabularCompareTargetBody {
                left: None,
                right: Some(disp.to_string()),
                compare_method: None,
            });
        } else if removed_set.contains(&disp.to_string()) {
            res_display.push(TabularCompareTargetBody {
                left: Some(disp.to_string()),
                right: None,
                compare_method: None,
            });
        } else {
            res_display.push(TabularCompareTargetBody {
                left: Some(disp.to_string()),
                right: Some(disp.to_string()),
                compare_method: None,
            });
        }
    }

    TabularCompareFields {
        keys: res_keys,
        targets: res_targets,
        display: res_display,

    }
}
}
