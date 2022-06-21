use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BranchName {
    pub branch: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Branch {
    pub name: String,
    pub commit_id: String,
    pub is_head: bool,
}
