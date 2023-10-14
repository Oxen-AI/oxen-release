use serde::{Deserialize, Serialize};

use crate::model::diff::dir_diff::DirDiff;
use crate::model::diff::tabular_diff::TabularDiff;
use crate::model::diff::text_diff::TextDiff;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum GenericDiff {
    DirDiff(DirDiff),
    TabularDiff(TabularDiff),
    TextDiff(TextDiff),
}
