
use serde::{Deserialize, Serialize};
use crate::model::{LocalRepository, Remote};
use crate::error::OxenError;
use crate::constants::{DEFAULT_ORIGIN_NAME, DEFAULT_ORIGIN_VALUE};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoteRepository {
    pub id: String,
    pub name: String,
    pub url: String,
}

impl RemoteRepository {
    pub fn from_local(repository: &LocalRepository) -> Result<RemoteRepository, OxenError> {
        Ok(RemoteRepository {
            id: repository.id.clone(),
            name: repository.name.clone(),
            url: repository
                    .remote()
                    .unwrap_or_else(|| {
                        Remote {
                            name: String::from(DEFAULT_ORIGIN_NAME),
                            value: String::from(DEFAULT_ORIGIN_VALUE)
                        }
                    })
                    .value,
        })
    }
}