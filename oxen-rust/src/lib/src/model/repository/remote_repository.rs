use crate::api::endpoint;
use crate::constants::{DEFAULT_REMOTE_NAME};
use crate::config::RemoteConfig;
use crate::error;
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
        let config = RemoteConfig::default().expect(error::REMOTE_CFG_NOT_FOUND);
        let url = endpoint::url_from_remote_config(&config, &format!("/repositories/{}", repository.name));
        RemoteRepository {
            id: repository.id.clone(),
            name: repository.name.clone(),
            url: repository
                .remote()
                .unwrap_or_else(|| Remote {
                    name: String::from(DEFAULT_REMOTE_NAME),
                    url: url,
                })
                .url,
        }
    }
}
