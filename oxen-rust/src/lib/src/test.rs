use crate::api;
use crate::config::{AuthConfig, RepoConfig};
use crate::error::OxenError;
use crate::index::{Indexer, Referencer, Stager};
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf}; // for write_all

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

pub fn setup_env() {
    env::set_var("HOST", "0.0.0.0");
    env::set_var("PORT", "2000");
}

pub fn create_repo_cfg(name: &str) -> Result<RepoConfig, OxenError> {
    let config = AuthConfig::new(auth_cfg_file());
    let repository = api::repositories::create(&config, name)?;
    Ok(RepoConfig::from(config, repository))
}

pub fn create_repo_dir(base_dir: &str) -> Result<PathBuf, OxenError> {
    let repo_name = format!("{}/repo_{}", base_dir, uuid::Uuid::new_v4());
    std::fs::create_dir_all(&repo_name)?;
    Ok(PathBuf::from(&repo_name))
}

pub fn create_stager(base_dir: &str) -> Result<(Stager, PathBuf), OxenError> {
    let repo_dir = create_repo_dir(base_dir)?;

    let indexer = Indexer::new(&repo_dir)?;
    indexer.init()?;

    Ok((Stager::new(&indexer.repo_dir)?, repo_dir))
}

pub fn create_referencer(base_dir: &str) -> Result<(Referencer, PathBuf), OxenError> {
    let repo_dir = create_repo_dir(base_dir)?;

    let indexer = Indexer::new(&repo_dir)?;
    indexer.init()?;

    Ok((Referencer::new(&indexer.repo_dir)?, repo_dir))
}

pub fn add_txt_file_to_dir(dir: &Path, contents: &str) -> Result<PathBuf, OxenError> {
    // Generate random name, because tests run in parallel, then return that name
    let file_path = PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4()));
    let full_path = dir.join(&file_path);
    // println!("add_txt_file_to_dir: {:?} to {:?}", file_path, full_path);

    let mut file = File::create(&full_path)?;
    file.write_all(contents.as_bytes())?;

    Ok(full_path)
}

pub fn add_img_file_to_dir(dir: &Path, file_path: &Path) -> Result<PathBuf, OxenError> {
    if let Some(ext) = file_path.extension() {
        // Generate random name with same extension, because tests run in parallel, then return that name
        let new_path = PathBuf::from(format!(
            "{}.{}",
            uuid::Uuid::new_v4(),
            ext.to_str().unwrap()
        ));
        let full_new_path = dir.join(&new_path);

        // println!("COPY FILE FROM {:?} => {:?}", file_path, full_new_path);
        std::fs::copy(&file_path, &full_new_path)?;
        Ok(full_new_path)
    } else {
        let err = format!("Unknown extension file: {:?}", file_path);
        Err(OxenError::basic_str(&err))
    }
}
