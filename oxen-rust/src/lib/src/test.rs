//! Helpers for our unit and integration tests
//!

use crate::api;
use crate::config::{AuthConfig};
use crate::error::OxenError;
use crate::index::{Referencer, Stager};
use crate::model::{LocalRepository, RemoteRepository};
use crate::command;

use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

const TEST_RUN_DIR: &str = "data/test/runs";

/// # Create a directory for a repo to run tests in
///
/// ```
/// # use liboxen::test::create_repo_dir;
/// # use liboxen::error::OxenError;
/// # fn main() -> Result<(), OxenError> {
/// 
/// let base_dir = "/tmp/base_dir";
/// let repo_dir = create_repo_dir(base_dir)?;
/// assert!(repo_dir.exists());
/// 
/// # std::fs::remove_dir_all(repo_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn create_repo_dir(base_dir: &str) -> Result<PathBuf, OxenError> {
    let repo_name = format!("{}/repo_{}", base_dir, uuid::Uuid::new_v4());
    std::fs::create_dir_all(&repo_name)?;
    Ok(PathBuf::from(&repo_name))
}

// 
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
pub fn run_empty_repo_dir_test<T>(test: T) -> ()
    where T: FnOnce(&Path) -> Result<(), OxenError> + std::panic::UnwindSafe
{
    match create_repo_dir(TEST_RUN_DIR) {
        Ok(repo_dir) => {
            // Run test to see if it panic'd
            let result = std::panic::catch_unwind(|| {
                match test(&repo_dir) {
                    Ok(_) => {},
                    Err(err) => {
                        eprintln!("Error running test. Err: {}", err);
                    }
                }
            });

            // Remove repo dir
            match std::fs::remove_dir_all(&repo_dir) {
                Ok(_) => {},
                Err(err) => {
                    eprintln!("Could not remove test dir. Err: {}", err);
                }
            }

            // Assert everything okay after we cleanup the repo dir
            assert!(result.is_ok());
        },
        Err(_) => {
            panic!("Could not create repo dir for test!");
        }
    }
}

pub fn run_empty_repo_test<T>(test: T) -> ()
    where T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe
{
    match create_repo_dir(TEST_RUN_DIR) {
        Ok(repo_dir) => {
            match command::init(&repo_dir) {
                Ok(repo) => {
                    // Run test to see if it panic'd
                    let result = std::panic::catch_unwind(|| {
                        match test(repo) {
                            Ok(_) => {},
                            Err(err) => {
                                eprintln!("Error running test. Err: {}", err);
                            }
                        }
                    });

                    // Remove repo dir
                    match std::fs::remove_dir_all(&repo_dir) {
                        Ok(_) => {},
                        Err(err) => {
                            eprintln!("Could not remove test dir. Err: {}", err);
                        }
                    }

                    // Assert everything okay after we cleanup the repo dir
                    assert!(result.is_ok());
                },
                Err(_) => {
                    panic!("Could not instantiate repository object for test!");
                }
            }
        },
        Err(_) => {
            panic!("Could not create repo dir for test!");
        }
    }
}

pub fn create_stager(base_dir: &str) -> Result<(Stager, PathBuf), OxenError> {
    let repo_dir = create_repo_dir(base_dir)?;
    command::init(&repo_dir)?;
    Ok((Stager::new(&repo_dir)?, repo_dir))
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

pub fn setup_env() {
    env::set_var("HOST", "0.0.0.0");
    env::set_var("PORT", "2000");
}

pub fn create_remote_repo(name: &str) -> Result<RemoteRepository, OxenError> {
    let config = AuthConfig::new(auth_cfg_file());
    let repository = api::remote::repositories::create(&config, name)?;
    Ok(repository)
}

pub fn create_referencer(base_dir: &str) -> Result<(Referencer, PathBuf), OxenError> {
    let repo_dir = create_repo_dir(base_dir)?;
    command::init(&repo_dir)?;
    Ok((Referencer::new(&repo_dir)?, repo_dir))
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
