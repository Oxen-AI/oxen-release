use crate::constants::{DEFAULT_ORIGIN_HOST, DEFAULT_REMOTE_NAME};
use crate::model::{LocalRepository, Remote};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoteRepository {
    pub id: String,
    pub name: String,
    pub url: String,
}

impl RemoteRepository {
    pub fn from_local(repository: &LocalRepository) -> RemoteRepository {
        RemoteRepository {
            id: repository.id.clone(),
            name: repository.name.clone(),
            url: repository
                .remote()
                .unwrap_or_else(|| Remote {
                    name: String::from(DEFAULT_REMOTE_NAME),
                    url: String::from(DEFAULT_ORIGIN_HOST),
                })
                .url,
        }
    }
}
