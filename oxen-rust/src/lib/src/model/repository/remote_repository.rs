use crate::api;
use crate::view::repository::RepositoryDataTypesView;
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
    pub fn from_data_view(
        repository: &RepositoryDataTypesView,
        remote: &Remote,
    ) -> RemoteRepository {
        RemoteRepository {
            namespace: repository.namespace.clone(),
            name: repository.name.clone(),
            remote: remote.clone(),
        }
    }

    pub fn from_view(repository: &RepositoryView, remote: &Remote) -> RemoteRepository {
        RemoteRepository {
            namespace: repository.namespace.clone(),
            name: repository.name.clone(),
            remote: remote.clone(),
        }
    }

    /// User friendly url for the remote repository
    /// Ex) http://localhost:3000/namespace/name
    pub fn url(&self) -> &str {
        &self.remote.url
    }

    /// Underlying api url for the remote repository
    /// Ex) http://localhost:3000/api/repos/namespace/name
    pub fn api_url(&self) -> Result<String, OxenError> {
        api::endpoint::url_from_repo(self, "")
    }
}
