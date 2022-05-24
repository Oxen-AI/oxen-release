//! Helpers for our unit and integration tests
//!

use crate::api;
use crate::command;
use crate::error::OxenError;
use crate::index::{Referencer, Stager};
use crate::model::{LocalRepository, RemoteRepository};

use env_logger::Env;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

const TEST_RUN_DIR: &str = "data/test/runs";

pub fn init_test_env() {
    let env = Env::default();
    if env_logger::try_init_from_env(env).is_ok() {
        log::debug!("Logger initialized");
    }

    std::env::set_var("TEST", "true");
}

fn create_prefixed_dir(base_dir: &str, prefix: &str) -> Result<PathBuf, OxenError> {
    let repo_name = format!("{}/{}_{}", prefix, base_dir, uuid::Uuid::new_v4());
    std::fs::create_dir_all(&repo_name)?;
    Ok(PathBuf::from(&repo_name))
}

fn create_repo_dir(base_dir: &str) -> Result<PathBuf, OxenError> {
    create_prefixed_dir(base_dir, "repo")
}

fn create_empty_dir(base_dir: &str) -> Result<PathBuf, OxenError> {
    create_prefixed_dir(base_dir, "dir")
}

/// # Run a unit test on a test repo directory
///
/// This function will create a directory with a uniq name
/// and take care of cleaning it up afterwards
///
/// ```
/// # use liboxen::test;
/// test::run_empty_dir_test(|repo_dir| {
///   // do your fancy testing here
///   assert!(true);
///   Ok(())
/// });
/// ```
pub fn run_empty_dir_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(&Path) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    let repo_dir = create_empty_dir(TEST_RUN_DIR)?;

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

pub fn run_empty_local_repo_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
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

/// Test syncing between local and remote, where both exist, and both are empty
pub fn run_empty_sync_repo_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(&LocalRepository, &RemoteRepository) -> Result<(), OxenError>
        + std::panic::UnwindSafe,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    
    let local_repo = command::init(&repo_dir)?;
    let remote_repo = api::remote::repositories::create_or_get(&local_repo)?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(&local_repo, &remote_repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Cleanup local repo
    std::fs::remove_dir_all(&repo_dir)?;

    // Cleanup remote repo
    api::remote::repositories::delete(remote_repo)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

/// Test where the local repo has training data in it
pub fn run_training_data_sync_test_no_commits<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(&LocalRepository, &RemoteRepository) -> Result<(), OxenError>
        + std::panic::UnwindSafe,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let local_repo = command::init(&repo_dir)?;

    // Write all the training data files
    populate_dir_with_training_data(&repo_dir)?;

    let remote_repo = api::remote::repositories::create_or_get(&local_repo)?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(&local_repo, &remote_repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Cleanup local repo
    std::fs::remove_dir_all(&repo_dir)?;

    // Cleanup remote repo
    api::remote::repositories::delete(remote_repo)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

/// Test interacting with a remote repo that has nothing synced
pub fn run_empty_remote_repo_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(&RemoteRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    let name = format!("repo_{}", uuid::Uuid::new_v4());
    let path = Path::new(&name);
    let local_repo = command::init(&path)?;
    let repo = api::remote::repositories::create_or_get(&local_repo)?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(&repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Cleanup remote repo
    api::remote::repositories::delete(repo)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

/// Run a test on a repo with a bunch of filees
pub fn run_training_data_repo_test_no_commits<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;

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

/// Run a test on a repo with a bunch of filees
pub fn run_training_data_repo_test_fully_committed<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;
    command::add(&repo, &repo_dir.join("train"))?;
    command::add(&repo, &repo_dir.join("test"))?;
    command::add(&repo, &repo_dir.join("annotations"))?;
    command::add(&repo, &repo_dir.join("labels.txt"))?;
    command::add(&repo, &repo_dir.join("README.md"))?;
    command::commit(&repo, "adding all data baby")?;

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
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    println!("BEFORE COMMAND::INIT");
    let repo = command::init(&repo_dir)?;
    println!("AFTER COMMAND::INIT");
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
    init_test_env();
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
    Path::new("data/test/config/remote_config.toml")
}

pub fn auth_cfg_file() -> &'static Path {
    Path::new("data/test/config/auth_config.toml")
}

pub fn repo_cfg_file() -> &'static Path {
    Path::new("data/test/config/repo_config.toml")
}

pub fn test_jpeg_file() -> &'static Path {
    Path::new("data/test/images/dwight_vince.jpeg")
}

pub fn populate_dir_with_training_data(repo_dir: &Path) -> Result<(), OxenError> {
    // Directory Structure
    // Features:
    //   - has multiple content types (jpg, txt, md)
    //   - has multiple directory levels (annotations/train/one_shot.txt)
    //   - has a file at top level (README.md)
    //   - has files/dirs at different levels with same names
    //
    // train/
    //   dog_1.jpg
    //   dog_2.jpg
    //   dog_3.jpg
    //   cat_1.jpg
    //   cat_2.jpg
    // test/
    //   1.jpg
    //   2.jpg
    // annotations/
    //   train/
    //     one_shot.txt
    //     annotations.txt
    //   test/
    //     annotations.txt
    // labels.txt
    // README.md

    // README.md
    write_txt_file_to_path(
        repo_dir.join("README.md"),
        r#"
        # Welcome to the party

        If you are seeing this, you are deep in the test framework, love to see it, keep testing.

        Yes I am biased, dog is label 0, cat is label 1, not alphabetical. Interpret that as you will.

        🐂 💨
    "#,
    )?;

    write_txt_file_to_path(
        repo_dir.join("labels.txt"),
        r#"
        dog
        cat
    "#,
    )?;

    // train/
    let train_dir = repo_dir.join("train");
    std::fs::create_dir_all(&train_dir)?;
    std::fs::copy(
        Path::new("data/test/images/dog_1.jpg"),
        train_dir.join("dog_1.jpg"),
    )?;
    std::fs::copy(
        Path::new("data/test/images/dog_2.jpg"),
        train_dir.join("dog_2.jpg"),
    )?;
    std::fs::copy(
        Path::new("data/test/images/dog_3.jpg"),
        train_dir.join("dog_3.jpg"),
    )?;
    std::fs::copy(
        Path::new("data/test/images/cat_1.jpg"),
        train_dir.join("cat_1.jpg"),
    )?;
    std::fs::copy(
        Path::new("data/test/images/cat_2.jpg"),
        train_dir.join("cat_2.jpg"),
    )?;

    // test/
    let test_dir = repo_dir.join("test");
    std::fs::create_dir_all(&test_dir)?;
    std::fs::copy(
        Path::new("data/test/images/dog_4.jpg"),
        test_dir.join("1.jpg"),
    )?;
    std::fs::copy(
        Path::new("data/test/images/cat_3.jpg"),
        test_dir.join("2.jpg"),
    )?;

    // annotations/train/
    let train_annotations_dir = repo_dir.join("annotations/train");
    std::fs::create_dir_all(&train_annotations_dir)?;
    write_txt_file_to_path(
        train_annotations_dir.join("annotations.txt"),
        r#"
        train/dog_1.jpg 0
        train/dog_2.jpg 0
        train/dog_3.jpg 0
        train/cat_1.jpg 1
        train/cat_2.jpg 1
    "#,
    )?;
    write_txt_file_to_path(
        train_annotations_dir.join("one_shot.txt"),
        r#"
        train/dog_1.jpg 0
        train/cat_1.jpg 1
    "#,
    )?;

    // annotations/test/
    let test_annotations_dir = repo_dir.join("annotations/test");
    std::fs::create_dir_all(&test_annotations_dir)?;
    write_txt_file_to_path(
        test_annotations_dir.join("annotations.txt"),
        r#"
        test/1.jpg 0
        test/2.jpg 1
    "#,
    )?;

    Ok(())
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

pub fn write_txt_file_to_path(path: PathBuf, contents: &str) -> Result<PathBuf, OxenError> {
    let mut file = File::create(&path)?;
    file.write_all(contents.as_bytes())?;
    Ok(path)
}

pub fn modify_txt_file(path: PathBuf, contents: &str) -> Result<PathBuf, OxenError> {
    // Overwrite
    if path.exists() {
        std::fs::remove_file(&path)?;
    }

    let path = write_txt_file_to_path(path, contents)?;
    Ok(path)
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
