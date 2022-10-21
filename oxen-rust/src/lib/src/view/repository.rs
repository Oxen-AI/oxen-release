use crate::model::RemoteRepository;
use serde::{Deserialize, Serialize};

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
pub struct ListRepositoryResponse {
    pub status: String,
    pub status_message: String,
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
