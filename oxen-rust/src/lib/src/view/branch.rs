use crate::model::Branch;
use serde::{Deserialize, Serialize};

use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct BranchResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub branch: Branch,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct BranchNew {
    pub name: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct BranchName {
    pub branch_name: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct BranchNewFromExisting {
    pub new_name: String,
    pub from_name: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct BranchUpdate {
    pub commit_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListBranchesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub branches: Vec<Branch>,
}
