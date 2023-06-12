use crate::model::{EntryDataType, RemoteRepository};
use serde::{Deserialize, Serialize};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepositoryView {
    pub namespace: String,
    pub name: String,
    // pub api_url: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepositoryResponse {
    pub status: String,
    pub status_message: String,
    pub repository: RepositoryView,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepositoryStatsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub repository: RepositoryStatsView,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataTypeView {
    pub data_type: EntryDataType,
    pub data_size: u64,
    pub file_count: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepositoryStatsView {
    pub data_size: u64,
    pub data_types: Vec<DataTypeView>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListRepositoryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub repositories: Vec<RepositoryView>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepositoryResolveResponse {
    pub status: String,
    pub status_message: String,
    pub repository_api_url: String,
}

impl RepositoryView {
    pub fn from_remote(repository: RemoteRepository) -> RepositoryView {
        RepositoryView {
            namespace: repository.namespace.clone(),
            name: repository.name,
        }
    }
}
