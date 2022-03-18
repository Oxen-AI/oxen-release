
use crate::config::{AuthConfig, RepoConfig};
use crate::api;
use crate::error::OxenError;
use std::path::Path;

pub fn remote_cfg_file() -> &'static Path {
  Path::new("config/remote_cfg.toml")
}

pub fn auth_cfg_file() -> &'static Path {
  Path::new("config/auth_cfg.toml")
}

pub fn repo_cfg_file() -> &'static Path {
  Path::new("config/repo_cfg.toml")
}

pub fn create_repo_cfg(name: &str) -> Result<RepoConfig, OxenError> {
  let config = AuthConfig::from(auth_cfg_file());
  let repository = api::repositories::create(&config, name)?;
  Ok(RepoConfig::new(&config, &repository))
}