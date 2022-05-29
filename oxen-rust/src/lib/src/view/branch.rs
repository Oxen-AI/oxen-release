
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
    pub name: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListBranchesResponse {
    pub status: String,
    pub status_message: String,
    pub branches: Vec<Branch>,
}
