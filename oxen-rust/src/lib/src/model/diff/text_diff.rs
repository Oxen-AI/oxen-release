use std::path::{Path, PathBuf};

use crate::model::diff::change_type::ChangeType;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LineDiff {
    pub modification: ChangeType,
    pub text: String,
}

#[derive(Default, Deserialize, Serialize, Debug, Clone)]
pub struct TextDiff {
    pub lines: Vec<LineDiff>,
    pub filename1: Option<PathBuf>,
    pub filename2: Option<PathBuf>,
}
