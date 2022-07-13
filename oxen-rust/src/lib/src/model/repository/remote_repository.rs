use crate::api;
use crate::model::LocalRepository;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoteRepository {
    pub id: String,
    pub name: String,
}

impl RemoteRepository {
    pub fn from_local(repository: &LocalRepository) -> RemoteRepository {
        RemoteRepository {
            id: repository.id.clone(),
            name: repository.name.clone(),
        }
    }

    pub fn url(&self) -> String {
        api::endpoint::repo_url(self)
    }
}
