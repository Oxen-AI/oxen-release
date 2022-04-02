use crate::api;
use crate::config::{AuthConfig, RepoConfig};
use crate::error::OxenError;
use crate::cli::Stager;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::prelude::*; // for write_all

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

pub fn create_stager(base_dir: &str) -> Result<(Stager, PathBuf, PathBuf), OxenError> {
    let db_dir = format!("{}/db_{}", base_dir, uuid::Uuid::new_v4());
    let db_path = PathBuf::from(&db_dir);

    let data_dir = format!("{}/data_{}", base_dir, uuid::Uuid::new_v4());
    let repo_dir = PathBuf::from(&data_dir);
    std::fs::create_dir_all(&repo_dir)?;

    Ok((Stager::new(&db_path, &repo_dir)?, repo_dir, db_path))
}

pub fn add_txt_file_to_dir(repo_path: &Path, contents: &str) -> Result<PathBuf, OxenError> {
    let file_path = PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4()));
    let full_path = repo_path.join(&file_path);
    let mut file = File::create(&full_path)?;
    file.write_all(contents.as_bytes())?;
    Ok(full_path)
}