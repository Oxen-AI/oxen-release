use crate::api;
use crate::view::RepositoryView;
use crate::{error::OxenError, model::Remote};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoteRepository {
    pub namespace: String,
    pub name: String,
    pub remote: Remote,
}

impl RemoteRepository {
    pub fn from_view(repository: &RepositoryView, remote: &Remote) -> RemoteRepository {
        RemoteRepository {
            namespace: repository.namespace.clone(),
            name: repository.name.clone(),
            remote: remote.clone(),
        }
    }

    pub fn url(&self) -> Result<String, OxenError> {
        // log::info!("creating url_from_repo {self:?}");
        let url = api::endpoint::url_from_repo(self, "")?;
        Ok(url)
    }
}
