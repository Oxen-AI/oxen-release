use crate::model::diff::change_type::ChangeType;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LineDiff {
    pub modification: ChangeType,
    pub text: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TextDiff {
    pub lines: Vec<LineDiff>,
}
