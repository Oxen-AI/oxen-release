use crate::model::Remote;
use crate::view::RepositoryView;
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
}
