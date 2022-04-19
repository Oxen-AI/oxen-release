
use crate::model::{Repository, CommitHead};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct RepositoryResponse {
    pub status: String,
    pub status_message: String,
    pub repository: Repository,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepositoryHeadResponse {
    pub status: String,
    pub status_message: String,
    pub repository: Repository,
    pub head: Option<CommitHead>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListRepositoriesResponse {
    pub status: String,
    pub status_message: String,
    pub repositories: Vec<Repository>,
}
