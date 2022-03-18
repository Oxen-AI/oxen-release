use crate::api;
use crate::config::AuthConfig;
use crate::error::OxenError;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Repository {
    pub id: String,
    pub name: String,
    pub url: String,
}

#[derive(Deserialize, Debug)]
pub struct RepositoryResponse {
    pub repository: Repository,
}

#[derive(Deserialize, Debug)]
pub struct ListRepositoriesResponse {
    pub repositories: Vec<Repository>,
}

impl Repository {
    pub fn clone_remote(config: &AuthConfig, url: &str) -> Result<Repository, OxenError> {
        let repository = api::repositories::get_by_url(config, url)?;
        Ok(repository)
    }
}
