//! Helpers for our unit and integration tests
//!

use crate::api;
use crate::command;
use crate::config::AuthConfig;
use crate::error::OxenError;
use crate::index::{Referencer, Stager};
use crate::model::{LocalRepository, RemoteRepository};

use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

const TEST_RUN_DIR: &str = "data/test/runs";

fn create_repo_dir(base_dir: &str) -> Result<PathBuf, OxenError> {
    let repo_name = format!("{}/repo_{}", base_dir, uuid::Uuid::new_v4());
    std::fs::create_dir_all(&repo_name)?;
    Ok(PathBuf::from(&repo_name))
}

/// # Run a unit test on a test repo directory
///
/// This function will create a directory with a uniq name
/// and take care of cleaning it up afterwards
///
/// ```
/// # use liboxen::test;
/// test::run_empty_repo_dir_test(|repo_dir| {
///   // do your fancy testing here
///   assert!(true);
///   Ok(())
/// });
/// ```
pub fn run_empty_repo_dir_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(&Path) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(&repo_dir) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    std::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());

    Ok(())
}

pub fn run_empty_repo_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    std::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

pub fn run_empty_stager_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(Stager) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;
    let stager = Stager::new(&repo)?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(stager) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    std::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

pub fn run_referencer_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(Referencer) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;
    let referencer = Referencer::new(&repo)?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(referencer) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    std::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

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

pub fn create_remote_repo(name: &str) -> Result<RemoteRepository, OxenError> {
    let config = AuthConfig::new(auth_cfg_file());
    let repository = api::remote::repositories::create(&config, name)?;
    Ok(repository)
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
