use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirEntry {
    pub filename: String,
    pub is_dir: bool,
}
