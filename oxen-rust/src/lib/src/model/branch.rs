use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Branch {
    pub name: String,
    pub commit_id: String,
    pub is_head: bool,
}
