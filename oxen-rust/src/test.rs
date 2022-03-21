use crate::api;
use crate::config::{AuthConfig, RepoConfig};
use crate::error::OxenError;
use std::path::Path;

pub fn remote_cfg_file() -> &'static Path {
    Path::new("data/test/config/remote_cfg.toml")
}

pub fn auth_cfg_file() -> &'static Path {
    Path::new("data/test/config/auth_cfg.toml")
}

pub fn repo_cfg_file() -> &'static Path {
    Path::new("data/test/config/repo_cfg.toml")
}

pub fn test_jpeg_file() -> &'static Path {
    Path::new("data/test/images/dwight_vince.jpeg")
}

pub fn create_repo_cfg(name: &str) -> Result<RepoConfig, OxenError> {
    let config = AuthConfig::new(auth_cfg_file());
    let repository = api::repositories::create(&config, name)?;
    Ok(RepoConfig::from(&config, &repository))
}
