use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TextDiff {
    pub added_lines: Vec<String>,
    pub removed_lines: Vec<String>,
    pub modified_lines: Vec<String>,
    pub unchanged_lines: Vec<String>,
}
