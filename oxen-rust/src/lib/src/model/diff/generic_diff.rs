use serde::{Deserialize, Serialize};

use crate::model::diff::dir_diff::DirDiff;
use crate::model::diff::tabular_diff::TabularDiff;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum GenericDiff {
    DirDiff(DirDiff),
    TabularDiff(TabularDiff),
}
