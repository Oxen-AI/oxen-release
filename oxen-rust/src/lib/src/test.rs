//! Helpers for our unit and integration tests
//!

use crate::api;
use crate::command;
use crate::constants;

use crate::core::index::{RefWriter, Stager};
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteRepository};

use crate::opts::RmOpts;
use crate::util;

use env_logger::Env;
use rand::Rng;
use std::fs::File;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

const TEST_RUN_DIR: &str = "data/test/runs";
pub const DEFAULT_TEST_HOST: &str = "localhost:3000";

pub fn test_host() -> String {
    match std::env::var("OXEN_TEST_HOST") {
        Ok(host) => host,
        Err(_err) => String::from(DEFAULT_TEST_HOST),
    }
}

pub fn repo_remote_url_from(name: &str) -> String {
    // Tests always point to localhost
    api::endpoint::remote_url_from_host(test_host().as_str(), constants::DEFAULT_NAMESPACE, name)
}

pub fn init_test_env() {
    let env = Env::default();
    if env_logger::try_init_from_env(env).is_ok() {
        log::debug!("Logger initialized");
    }

    std::env::set_var("TEST", "true");
}

fn create_prefixed_dir(base_dir: &str, prefix: &str) -> Result<PathBuf, OxenError> {
    let repo_name = format!("{}_{}_{}", prefix, base_dir, uuid::Uuid::new_v4());
    let full_dir = Path::new(base_dir).join(repo_name);
    std::fs::create_dir_all(&full_dir)?;
    Ok(full_dir)
}

fn create_repo_dir(base_dir: &str) -> Result<PathBuf, OxenError> {
    create_prefixed_dir(base_dir, "repo")
}

fn create_empty_dir(base_dir: &str) -> Result<PathBuf, OxenError> {
    create_prefixed_dir(base_dir, "dir")
}

pub async fn create_remote_repo(repo: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    api::remote::repositories::create(
        repo,
        constants::DEFAULT_NAMESPACE,
        &repo.dirname(),
        test_host(),
    )
    .await
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
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());

    Ok(())
}

pub async fn run_empty_dir_test_async<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = Result<PathBuf, OxenError>>,
{
    init_test_env();
    let repo_dir = create_empty_dir(TEST_RUN_DIR)?;

    // Run test to see if it panic'd
    let result = match test(repo_dir).await {
        Ok(repo_dir) => {
            // Remove repo dir
            util::fs::remove_dir_all(repo_dir)?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);

    Ok(())
}

pub fn run_empty_local_repo_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError>,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;

    let result = match test(repo) {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

pub async fn run_empty_local_repo_test_async<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;

    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test syncing between local and remote, where both exist, and both are empty
pub async fn run_empty_sync_repo_test<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(&LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;

    let local_repo = command::init(&repo_dir)?;

    let namespace = constants::DEFAULT_NAMESPACE;
    let name = local_repo.dirname();
    let remote_repo =
        api::remote::repositories::create(&local_repo, namespace, &name, test_host()).await?;

    // Run test to see if it panic'd
    let result = match test(&local_repo, remote_repo).await {
        Ok(remote_repo) => {
            // Cleanup remote repo
            api::remote::repositories::delete(&remote_repo).await?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Cleanup local repo
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test where the local repo has training data in it
pub async fn run_training_data_sync_test_no_commits<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let local_repo = command::init(&repo_dir)?;

    // Write all the training data files
    populate_dir_with_training_data(&repo_dir)?;

    let namespace = constants::DEFAULT_NAMESPACE;
    let name = local_repo.dirname();
    let remote_repo =
        api::remote::repositories::create(&local_repo, namespace, &name, test_host()).await?;
    println!("Got remote repo: {remote_repo:?}");

    // Run test to see if it panic'd
    let result = match test(local_repo, remote_repo).await {
        Ok(_remote_repo) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test where we synced training data to the remote
pub async fn run_training_data_fully_sync_remote<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let mut local_repo = command::init(&repo_dir)?;

    // Write all the training data files
    populate_dir_with_training_data(&repo_dir)?;
    // Make a few commits before we sync
    command::add(&local_repo, local_repo.path.join("train"))?;
    command::commit(&local_repo, "Adding train/")?;

    command::add(&local_repo, local_repo.path.join("test"))?;
    command::commit(&local_repo, "Adding test/")?;

    command::add(&local_repo, local_repo.path.join("annotations"))?;
    command::commit(&local_repo, "Adding annotations/")?;

    command::add(&local_repo, local_repo.path.join("nlp"))?;
    command::commit(&local_repo, "Adding nlp/")?;

    // Remove the test dir to make a more complex history
    let rm_opts = RmOpts {
        path: PathBuf::from("test"),
        recursive: true,
        staged: false,
        remote: false,
    };
    command::rm(&local_repo, &rm_opts).await?;
    command::commit(&local_repo, "Removing test/")?;

    // Add all the files
    command::add(&local_repo, &local_repo.path)?;
    // Commit all the data locally
    command::commit(&local_repo, "Adding rest of data")?;

    // Create remote
    let namespace = constants::DEFAULT_NAMESPACE;
    let name = local_repo.dirname();
    let remote_repo =
        api::remote::repositories::create(&local_repo, namespace, &name, test_host()).await?;

    // Add remote
    let remote_url = repo_remote_url_from(&local_repo.dirname());
    command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote_url)?;
    // Push data
    command::push(&local_repo).await?;

    // Run test to see if it panic'd
    let result = match test(local_repo, remote_repo).await {
        Ok(_remote_repo) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test interacting with a remote repo that was created via API, not local repo
pub async fn run_no_commit_remote_repo_test<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    let name = format!("repo_{}", uuid::Uuid::new_v4());
    let namespace = constants::DEFAULT_NAMESPACE;
    let repo = api::remote::repositories::create_no_root(namespace, &name, test_host()).await?;

    // Run test to see if it panic'd
    let result = match test(repo).await {
        Ok(repo) => {
            // Cleanup remote repo
            api::remote::repositories::delete(&repo).await?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test interacting with a remote repo that has nothing synced
pub async fn run_empty_remote_repo_test<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    let empty_dir = create_empty_dir(TEST_RUN_DIR)?;
    let name = format!("repo_{}", uuid::Uuid::new_v4());
    let path = empty_dir.join(name);
    let local_repo = command::init(&path)?;
    let namespace = constants::DEFAULT_NAMESPACE;
    let name = local_repo.dirname();
    let remote_repo =
        api::remote::repositories::create(&local_repo, namespace, &name, test_host()).await?;

    println!("REMOTE REPO: {remote_repo:?}");

    // Run test to see if it panic'd
    let result = match test(local_repo, remote_repo).await {
        Ok(repo) => {
            // Cleanup remote repo
            api::remote::repositories::delete(&repo).await?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Cleanup Local
    util::fs::remove_dir_all(path)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test interacting with a remote repo that has has the initial commit pushed
pub async fn run_remote_repo_test_all_data_pushed<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    let empty_dir = create_empty_dir(TEST_RUN_DIR)?;
    let name = format!("repo_{}", uuid::Uuid::new_v4());
    let path = empty_dir.join(name);
    let mut local_repo = command::init(&path)?;

    // Write all the files
    populate_dir_with_training_data(&local_repo.path)?;
    add_all_data_to_repo(&local_repo)?;
    command::commit(&local_repo, "Adding all data")?;

    // Set the proper remote
    let remote = repo_remote_url_from(&local_repo.dirname());
    command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

    // Create remote repo
    let repo = create_remote_repo(&local_repo).await?;

    command::push(&local_repo).await?;

    // Run test to see if it panic'd
    let result = match test(repo).await {
        Ok(_repo) => {
            // TODO: Cleanup remote repo
            // this was failing
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Cleanup Local
    util::fs::remove_dir_all(path)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Run a test on a repo with a bunch of filees
pub async fn run_training_data_repo_test_no_commits_async<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;

    // Run test to see if it panic'd
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Run a test on a repo with a bunch of files
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
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

/// Run a test on a repo with a bunch of filees
pub async fn run_training_data_repo_test_fully_committed_async<T, Fut>(
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;
    // Add all the files
    command::add(&repo, &repo.path)?;

    // Make it easy to find these schemas during testing
    command::schemas::set_name(&repo, "b821946753334c083124fd563377d795", "bounding_box")?;
    command::schemas::set_name(
        &repo,
        "34a3b58f5471d7ae9580ebcf2582be2f",
        "text_classification",
    )?;

    command::commit(&repo, "adding all data baby")?;

    // Run test to see if it panic'd
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Run a test on a repo with a bunch of files
pub fn run_training_data_repo_test_fully_committed<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;

    // Add all the files
    command::add(&repo, &repo.path)?;

    // Make it easy to find these schemas during testing
    command::schemas::set_name(&repo, "b821946753334c083124fd563377d795", "bounding_box")?;
    command::schemas::set_name(
        &repo,
        "34a3b58f5471d7ae9580ebcf2582be2f",
        "text_classification",
    )?;

    command::commit(&repo, "adding all data baby")?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

fn add_all_data_to_repo(repo: &LocalRepository) -> Result<(), OxenError> {
    command::add(repo, repo.path.join("train"))?;
    command::add(repo, repo.path.join("test"))?;
    command::add(repo, repo.path.join("annotations"))?;
    command::add(repo, repo.path.join("large_files"))?;
    command::add(repo, repo.path.join("nlp"))?;
    command::add(repo, repo.path.join("labels.txt"))?;
    command::add(repo, repo.path.join("README.md"))?;

    // Make it easy to find these schemas during testing
    command::schemas::set_name(repo, "b821946753334c083124fd563377d795", "bounding_box")?;
    command::schemas::set_name(
        repo,
        "34a3b58f5471d7ae9580ebcf2582be2f",
        "text_classification",
    )?;

    Ok(())
}

pub fn run_empty_stager_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(Stager, LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    log::debug!("BEFORE COMMAND::INIT");
    let repo = command::init(&repo_dir)?;
    log::debug!("AFTER COMMAND::INIT");
    let stager = Stager::new(&repo)?;
    log::debug!("AFTER CREATE STAGER");

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(stager, repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

pub fn run_referencer_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(RefWriter) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    let repo_dir = create_repo_dir(TEST_RUN_DIR)?;
    let repo = command::init(&repo_dir)?;
    let referencer = RefWriter::new(&repo)?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(referencer) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

pub fn user_cfg_file() -> &'static Path {
    Path::new("data/test/config/user_config.toml")
}

pub fn repo_cfg_file() -> &'static Path {
    Path::new("data/test/config/repo_config.toml")
}

pub fn test_jpeg_file() -> &'static Path {
    Path::new("data/test/images/dwight_vince.jpeg")
}

pub fn test_jpeg_file_with_name(name: &str) -> PathBuf {
    PathBuf::from(&format!("data/test/images/{name}"))
}

pub fn test_200k_csv() -> &'static Path {
    Path::new("data/test/text/celeb_a_200k.csv")
}

pub fn test_nlp_classification_csv() -> &'static Path {
    Path::new("nlp/classification/annotations/test.tsv")
}

pub fn populate_dir_with_training_data(repo_dir: &Path) -> Result<(), OxenError> {
    // Directory Structure
    // Features:
    //   - has multiple content types (jpg, txt, md)
    //   - has a few large data files that we have to chunk and transfer
    //   - has multiple directory levels (annotations/train/one_shot.txt)
    //   - has files at top level (README.md)
    //   - has files without extensions (LICENSE)
    //   - has files/dirs at different levels with same names (annotations.txt)
    //
    // nlp/
    //   classification/
    //     annotations/
    //       train.tsv
    //       test.tsv
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
    //   README.md
    //   train/
    //     bounding_box.csv
    //     one_shot.csv
    //     two_shot.csv
    //     annotations.txt
    //   test/
    //     annotations.txt
    // labels.txt
    // LICENSE
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

    // large_files
    let large_dir = repo_dir.join("large_files");
    std::fs::create_dir_all(&large_dir)?;
    let large_file_1 = large_dir.join("test.csv");
    let from_file = test_200k_csv();
    util::fs::copy(from_file, large_file_1)?;

    // train/
    let train_dir = repo_dir.join("train");
    std::fs::create_dir_all(&train_dir)?;
    util::fs::copy(
        Path::new("data/test/images/dog_1.jpg"),
        train_dir.join("dog_1.jpg"),
    )?;
    util::fs::copy(
        Path::new("data/test/images/dog_2.jpg"),
        train_dir.join("dog_2.jpg"),
    )?;
    util::fs::copy(
        Path::new("data/test/images/dog_3.jpg"),
        train_dir.join("dog_3.jpg"),
    )?;
    util::fs::copy(
        Path::new("data/test/images/cat_1.jpg"),
        train_dir.join("cat_1.jpg"),
    )?;
    util::fs::copy(
        Path::new("data/test/images/cat_2.jpg"),
        train_dir.join("cat_2.jpg"),
    )?;

    // test/
    let test_dir = repo_dir.join("test");
    std::fs::create_dir_all(&test_dir)?;
    util::fs::copy(
        Path::new("data/test/images/dog_4.jpg"),
        test_dir.join("1.jpg"),
    )?;
    util::fs::copy(
        Path::new("data/test/images/cat_3.jpg"),
        test_dir.join("2.jpg"),
    )?;

    // annotations/README.md
    let annotations_dir = repo_dir.join("annotations");
    std::fs::create_dir_all(&annotations_dir)?;
    let annotations_readme_file = annotations_dir.join("README.md");
    write_txt_file_to_path(
        annotations_readme_file,
        r#"
        # Annotations
        Some info about our annotations structure....
        "#,
    )?;

    // annotations/train/
    let train_annotations_dir = repo_dir.join("annotations").join("train");
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
        train_annotations_dir.join("bounding_box.csv"),
        r#"file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
train/dog_1.jpg,dog,102.5,31.0,386,330
train/dog_2.jpg,dog,7.0,29.5,246,247
train/dog_3.jpg,dog,19.0,63.5,376,421
train/cat_1.jpg,cat,57.0,35.5,304,427
train/cat_2.jpg,cat,30.5,44.0,333,396
"#,
    )?;

    write_txt_file_to_path(
        train_annotations_dir.join("one_shot.csv"),
        r#"file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
"#,
    )?;

    write_txt_file_to_path(
        train_annotations_dir.join("two_shot.csv"),
        r#"file,label,min_x,min_y,width,height
train/dog_3.jpg,dog,19.0,63.5,376,421
train/cat_1.jpg,cat,57.0,35.5,304,427
"#,
    )?;

    // annotations/test/
    let test_annotations_dir = repo_dir.join("annotations").join("test");
    std::fs::create_dir_all(&test_annotations_dir)?;
    write_txt_file_to_path(
        test_annotations_dir.join("annotations.csv"),
        r#"file,label,min_x,min_y,width,height
test/dog_3.jpg,dog,19.0,63.5,376,421
test/cat_1.jpg,cat,57.0,35.5,304,427
test/unknown.jpg,unknown,0.0,0.0,0,0
"#,
    )?;

    // nlp/classification/annotations/
    // Make sure to add a few duplicate examples for testing
    let nlp_annotations_dir = repo_dir.join("nlp/classification/annotations");
    std::fs::create_dir_all(&nlp_annotations_dir)?;
    write_txt_file_to_path(
        nlp_annotations_dir.join("train.tsv"),
        r#"text	label
My tummy hurts	negative
I have a headache	negative
My tummy hurts	negative
I have a headache	negative
loving the sunshine	positive
And another unique one	positive
My tummy hurts	negative
loving the sunshine	positive
I am a lonely example	negative
I am adding more examples	positive
One more time	positive
"#,
    )?;

    write_txt_file_to_path(
        nlp_annotations_dir.join("test.tsv"),
        r#"text	label
My tummy hurts	negative
My tummy hurts	negative
My tummy hurts	negative
I have a headache	negative
I have a headache	negative
loving the sunshine	positive
loving the sunshine	positive
I am a lonely example	negative
I am a great testing example	positive
    "#,
    )?;

    Ok(())
}

pub fn add_file_to_dir(dir: &Path, contents: &str, extension: &str) -> Result<PathBuf, OxenError> {
    // Generate random name, because tests run in parallel, then return that name
    let file_path = PathBuf::from(format!("{}.{extension}", uuid::Uuid::new_v4()));
    let full_path = dir.join(file_path);
    // println!("add_txt_file_to_dir: {:?} to {:?}", file_path, full_path);

    let mut file = File::create(&full_path)?;
    file.write_all(contents.as_bytes())?;

    Ok(full_path)
}

pub fn add_txt_file_to_dir(dir: &Path, contents: &str) -> Result<PathBuf, OxenError> {
    add_file_to_dir(dir, contents, "txt")
}

pub fn add_csv_file_to_dir(dir: &Path, contents: &str) -> Result<PathBuf, OxenError> {
    add_file_to_dir(dir, contents, "csv")
}

pub fn write_txt_file_to_path<P: AsRef<Path>>(
    path: P,
    contents: &str,
) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();
    let mut file = File::create(path)?;
    file.write_all(contents.as_bytes())?;
    Ok(path.to_path_buf())
}

pub fn append_line_txt_file<P: AsRef<Path>>(path: P, line: &str) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();

    let mut file = OpenOptions::new().write(true).append(true).open(path)?;

    if let Err(e) = writeln!(file, "{line}") {
        return Err(OxenError::basic_str(format!("Couldn't write to file: {e}")));
    }

    Ok(path.to_path_buf())
}

pub fn modify_txt_file<P: AsRef<Path>>(path: P, contents: &str) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();

    // Overwrite
    if path.exists() {
        util::fs::remove_file(path)?;
    }

    let path = write_txt_file_to_path(path, contents)?;
    Ok(path)
}

pub fn add_random_bbox_to_file<P: AsRef<Path>>(path: P) -> Result<PathBuf, OxenError> {
    let mut rng = rand::thread_rng();
    let file_name = format!("train/random_img_{}.jpg", rng.gen_range(0..10));
    let x: f64 = rng.gen_range(0.0..1000.0);
    let y: f64 = rng.gen_range(0.0..1000.0);
    let w: i64 = rng.gen_range(0..1000);
    let h: i64 = rng.gen_range(0..1000);
    let line = format!("{file_name},{x:2},{y:2},{w},{h}");
    append_line_txt_file(path, &line)
}

pub fn add_img_file_to_dir(dir: &Path, file_path: &Path) -> Result<PathBuf, OxenError> {
    if let Some(ext) = file_path.extension() {
        // Generate random name with same extension, because tests run in parallel, then return that name
        let new_path = PathBuf::from(format!(
            "{}.{}",
            uuid::Uuid::new_v4(),
            ext.to_str().unwrap()
        ));
        let full_new_path = dir.join(new_path);

        // println!("COPY FILE FROM {:?} => {:?}", file_path, full_new_path);
        util::fs::copy(file_path, &full_new_path)?;
        Ok(full_new_path)
    } else {
        let err = format!("Unknown extension file: {file_path:?}");
        Err(OxenError::basic_str(err))
    }
}
