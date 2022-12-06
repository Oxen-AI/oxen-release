use serde::{Deserialize, Serialize};

use crate::model::Commit;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirEntry {
    pub filename: String,
    pub is_dir: bool,
    pub size: u64,
    pub latest_commit: Option<Commit>,
}
