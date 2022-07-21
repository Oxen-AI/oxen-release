use crate::model::{LocalRepository, RemoteRepository};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepositoryView {
    pub id: String,
    pub name: String,
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

impl RepositoryView {
    pub fn from_local(repository: LocalRepository) -> RepositoryView {
        RepositoryView {
            id: repository.id.clone(),
            name: repository.name,
        }
    }

    pub fn from_remote(repository: RemoteRepository) -> RepositoryView {
        RepositoryView {
            id: repository.id.clone(),
            name: repository.name,
        }
    }
}
