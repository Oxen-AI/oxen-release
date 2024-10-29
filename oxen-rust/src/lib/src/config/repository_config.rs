use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::constants::DEFAULT_VNODE_SIZE;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote};
use crate::util;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepositoryConfig {
    // this is the current remote name
    pub remote_name: Option<String>,
    pub remotes: Vec<Remote>,
    // write the version if it is past v0.18.4
    pub min_version: Option<String>,
    pub vnode_size: Option<u64>,
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl RepositoryConfig {
    pub fn new() -> Self {
        RepositoryConfig {
            remote_name: None,
            remotes: Vec::new(),
            min_version: None,
            vnode_size: None,
        }
    }

    pub fn from_repo(repo: &LocalRepository) -> Result<Self, OxenError> {
        let path = util::fs::config_filepath(&repo.path);
        Self::from_file(&path)
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, OxenError> {
        let contents = util::fs::read_from_path(&path)?;
        let remote_config: RepositoryConfig = toml::from_str(&contents)?;
        Ok(remote_config)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), OxenError> {
        let toml = toml::to_string(&self)?;
        util::fs::write_to_path(&path, toml)?;
        Ok(())
    }

    pub fn vnode_size(&self) -> u64 {
        self.vnode_size.unwrap_or(DEFAULT_VNODE_SIZE)
    }
}
