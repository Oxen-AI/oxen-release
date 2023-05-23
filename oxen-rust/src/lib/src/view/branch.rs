use crate::model::Branch;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct BranchResponse {
    pub status: String,
    pub status_message: String,
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
    pub status: String,
    pub status_message: String,
    pub branches: Vec<Branch>,
}
