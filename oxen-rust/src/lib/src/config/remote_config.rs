use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::OxenError;
use crate::model::Remote;
use crate::util;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RemoteConfig {
    pub remote_name: Option<String>, // this is the current remote name
    pub remotes: Vec<Remote>,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoteConfig {
    pub fn new() -> Self {
        RemoteConfig {
            remote_name: None,
            remotes: Vec::new(),
        }
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, OxenError> {
        let contents = util::fs::read_from_path(&path)?;
        let remote_config: RemoteConfig = toml::from_str(&contents)?;
        Ok(remote_config)
    }
}
