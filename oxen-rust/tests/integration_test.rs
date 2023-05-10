use liboxen::api;
use liboxen::command;
use liboxen::config::UserConfig;
use liboxen::constants;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::core::df::tabular;
use liboxen::core::index::CommitDirReader;
use liboxen::error::OxenError;
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::CommitBody;
use liboxen::model::ContentType;
use liboxen::model::StagedEntryStatus;
use liboxen::model::User;
use liboxen::opts::CloneOpts;
use liboxen::opts::DFOpts;
use liboxen::opts::PaginateOpts;
use liboxen::opts::RestoreOpts;
use liboxen::opts::RmOpts;
use liboxen::test;
use liboxen::util;

use futures::future;
use polars::prelude::AnyValue;
use std::path::Path;
use std::path::PathBuf;

#[test]
fn test_command_init() -> Result<(), OxenError> {
    test::run_empty_dir_test(|repo_dir| {
        // Init repo
        let repo = command::init(repo_dir)?;

        // Init should create the .oxen directory
        let hidden_dir = util::fs::oxen_hidden_dir(repo_dir);
        let config_file = util::fs::config_filepath(repo_dir);
        assert!(hidden_dir.exists());
        assert!(config_file.exists());

        // We make an initial parent commit and branch called "main"
        // just to make our lives easier down the line
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();
        assert_eq!(orig_branch.name, constants::DEFAULT_BRANCH_NAME);
        assert!(!orig_branch.commit_id.is_empty());

        Ok(())
    })
}

#[test]
fn test_command_status_empty() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        let repo_status = command::status(&repo)?;

        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 0);
        assert_eq!(repo_status.untracked_dirs.len(), 0);

        Ok(())
    })
}

#[test]
fn test_command_status_nothing_staged_full_directory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let repo_status = command::status(&repo)?;

        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        // README.md
        // labels.txt
        assert_eq!(repo_status.untracked_files.len(), 2);
        // train/
        // test/
        // nlp/
        // large_files/
        // annotations/
        assert_eq!(repo_status.untracked_dirs.len(), 5);

        Ok(())
    })
}

#[test]
fn test_command_add_one_file_top_level() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        command::add(&repo, repo.path.join(Path::new("labels.txt")))?;

        let repo_status = command::status(&repo)?;
        repo_status.print_stdout();

        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 1);
        // README.md
        // labels.txt
        assert_eq!(repo_status.untracked_files.len(), 1);
        // train/
        // test/
        // nlp/
        // large_files/
        // annotations/
        assert_eq!(repo_status.untracked_dirs.len(), 5);

        Ok(())
    })
}

#[test]
fn test_command_status_shows_intermediate_directory_if_file_added() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Add a deep file
        command::add(
            &repo,
            repo.path.join(Path::new("annotations/train/one_shot.csv")),
        )?;

        // Make sure that we now see the full annotations/train/ directory
        let repo_status = command::status(&repo)?;
        repo_status.print_stdout();

        // annotations/
        assert_eq!(repo_status.added_dirs.len(), 1);
        // annotations/train/one_shot.csv
        assert_eq!(repo_status.added_files.len(), 1);
        // annotations/test/
        // train/
        // large_files/
        // test/
        // nlp/
        assert_eq!(repo_status.untracked_dirs.len(), 5);
        // README.md
        // labels.txt
        // annotations/README.md
        // annotations/train/two_shot.csv
        // annotations/train/annotations.txt
        // annotations/train/bounding_box.csv
        assert_eq!(repo_status.untracked_files.len(), 6);

        Ok(())
    })
}

#[test]
fn test_command_commit_nothing_staged() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        let commits = api::local::commits::list(&repo)?;
        let initial_len = commits.len();
        let result = command::commit(&repo, "Should not work");
        assert!(result.is_err());
        let commits = api::local::commits::list(&repo)?;
        // We should not have added any commits
        assert_eq!(commits.len(), initial_len);
        Ok(())
    })
}

#[test]
fn test_command_commit_nothing_staged_but_file_modified() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let commits = api::local::commits::list(&repo)?;
        let initial_len = commits.len();

        let labels_path = repo.path.join("labels.txt");
        util::fs::write_to_path(&labels_path, "changing this guy, but not committing")?;

        let result = command::commit(&repo, "Should not work");
        assert!(result.is_err());
        let commits = api::local::commits::list(&repo)?;
        // We should not have added any commits
        assert_eq!(commits.len(), initial_len);
        Ok(())
    })
}

#[test]
fn test_command_status_has_txt_file() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write to file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello World")?;

        // Get status
        let repo_status = command::status(&repo)?;
        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 1);
        assert_eq!(repo_status.untracked_dirs.len(), 0);

        Ok(())
    })
}

#[test]
fn test_command_add_file() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write to file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello World")?;

        // Track the file
        command::add(&repo, &hello_file)?;
        // Get status and make sure it is removed from the untracked, and added to the tracked
        let repo_status = command::status(&repo)?;
        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 1);
        assert_eq!(repo_status.untracked_files.len(), 0);
        assert_eq!(repo_status.untracked_dirs.len(), 0);

        Ok(())
    })
}

#[test]
fn test_command_commit_file() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write to file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello World")?;

        // Track the file
        command::add(&repo, &hello_file)?;
        // Commit the file
        command::commit(&repo, "My message")?;

        // Get status and make sure it is removed from the untracked and added
        let repo_status = command::status(&repo)?;
        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 0);
        assert_eq!(repo_status.untracked_dirs.len(), 0);

        let commits = api::local::commits::list(&repo)?;
        assert_eq!(commits.len(), 2);

        Ok(())
    })
}

#[test]
fn test_command_restore_removed_file_from_head() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write to file
        let hello_filename = "hello.txt";
        let hello_file = repo.path.join(hello_filename);
        util::fs::write_to_path(&hello_file, "Hello World")?;

        // Track the file
        command::add(&repo, &hello_file)?;
        // Commit the file
        command::commit(&repo, "My message")?;

        // Remove the file from disk
        std::fs::remove_file(&hello_file)?;

        // Check that it doesn't exist, then it does after we restore it
        assert!(!hello_file.exists());
        // Restore takes the filename not the full path to the test repo
        // ie: "hello.txt" instead of data/test/runs/repo_data/test/runs_fc1544ab-cd55-4344-aa13-5360dc91d0fe/hello.txt
        command::restore(&repo, RestoreOpts::from_path(hello_filename))?;
        assert!(hello_file.exists());

        Ok(())
    })
}

#[test]
fn test_command_restore_file_from_commit_id() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write to file
        let hello_filename = "hello.txt";
        let hello_file = repo.path.join(hello_filename);
        util::fs::write_to_path(&hello_file, "Hello World")?;

        // Track the file
        command::add(&repo, &hello_file)?;
        // Commit the file
        command::commit(&repo, "My message")?;

        // Modify the file once
        let first_modification = "Hola Mundo";
        let hello_file = test::modify_txt_file(hello_file, first_modification)?;
        command::add(&repo, &hello_file)?;
        let first_mod_commit = command::commit(&repo, "Changing to spanish")?;

        // Modify again
        let second_modification = "Bonjour le monde";
        let hello_file = test::modify_txt_file(hello_file, second_modification)?;
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Changing to french")?;

        // Restore from the first commit
        command::restore(
            &repo,
            RestoreOpts::from_path_ref(hello_filename, first_mod_commit.id),
        )?;
        let content = util::fs::read_from_path(&hello_file)?;
        assert!(hello_file.exists());
        assert_eq!(content, first_modification);

        Ok(())
    })
}

#[tokio::test]
async fn test_command_checkout_non_existant_commit_id() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|repo| async move {
        // This shouldn't work
        let checkout_result = command::checkout(&repo, "non-existant").await;
        assert!(checkout_result.is_err());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_checkout_commit_id() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|repo| async move {
        // Write a hello file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;

        // Stage a hello file
        command::add(&repo, &hello_file)?;
        // Commit the hello file
        let first_commit = command::commit(&repo, "Adding hello")?;

        // Write a world
        let world_file = repo.path.join("world.txt");
        util::fs::write_to_path(&world_file, "World")?;

        // Stage a world file
        command::add(&repo, &world_file)?;

        // Commit the world file
        command::commit(&repo, "Adding world")?;

        // We have the world file
        assert!(world_file.exists());

        // We checkout the previous commit
        command::checkout(&repo, first_commit.id).await?;

        // // Then we do not have the world file anymore
        assert!(!world_file.exists());

        // // Check status
        let status = command::status(&repo)?;
        assert!(status.is_clean());

        Ok(())
    })
    .await
}

#[test]
fn test_command_commit_dir() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Track the file
        let train_dir = repo.path.join("train");
        command::add(&repo, train_dir)?;
        // Commit the file
        command::commit(&repo, "Adding training data")?;

        let repo_status = command::status(&repo)?;
        repo_status.print_stdout();
        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 2);
        assert_eq!(repo_status.untracked_dirs.len(), 4);

        let commits = api::local::commits::list(&repo)?;
        assert_eq!(commits.len(), 2);

        Ok(())
    })
}

#[test]
fn test_command_commit_dir_recursive() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Track the annotations dir, which has sub dirs
        let annotations_dir = repo.path.join("annotations");
        command::add(&repo, annotations_dir)?;
        command::commit(&repo, "Adding annotations data dir, which has two levels")?;

        let repo_status = command::status(&repo)?;
        repo_status.print_stdout();

        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 2);
        assert_eq!(repo_status.untracked_dirs.len(), 4);

        let commits = api::local::commits::list(&repo)?;
        assert_eq!(commits.len(), 2);

        Ok(())
    })
}

#[tokio::test]
async fn test_command_checkout_current_branch_name_does_nothing() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|repo| async move {
        // Write the first file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;

        // Track & commit the file
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Added hello.txt")?;

        // Create and checkout branch
        let branch_name = "feature/world-explorer";
        api::local::branches::create_checkout(&repo, branch_name)?;
        command::checkout(&repo, branch_name).await?;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_cannot_checkout_branch_with_dots_in_name() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|repo| async move {
        // Write the first file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;

        // Track & commit the file
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Added hello.txt")?;

        // Create and checkout branch
        let branch_name = "test..ing";
        let result = api::local::branches::create_checkout(&repo, branch_name);
        assert!(result.is_err());

        Ok(())
    })
    .await
}

#[test]
fn test_rename_current_branch() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Create and checkout branch
        let og_branch_name = "feature/world-explorer";
        api::local::branches::create_checkout(&repo, og_branch_name)?;

        // Rename branch
        let new_branch_name = "feature/brave-new-world";
        api::local::branches::rename_current_branch(&repo, new_branch_name)?;

        // Check that the branch name has changed
        let current_branch = api::local::branches::current_branch(&repo)?.unwrap();
        assert_eq!(current_branch.name, new_branch_name);

        // Check that old branch no longer exists
        api::local::branches::list(&repo)?
            .iter()
            .for_each(|branch| {
                assert_ne!(branch.name, og_branch_name);
            });

        Ok(())
    })
}

#[tokio::test]
async fn test_command_checkout_added_file() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|repo| async move {
        // Write the first file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;

        // Track & commit the file
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Added hello.txt")?;

        // Get the original branch name
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Create and checkout branch
        let branch_name = "feature/world-explorer";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Write a second file
        let world_file = repo.path.join("world.txt");
        util::fs::write_to_path(&world_file, "World")?;

        // Track & commit the second file in the branch
        command::add(&repo, &world_file)?;
        command::commit(&repo, "Added world.txt")?;

        // Make sure we have both commits after the initial
        let commits = api::local::commits::list(&repo)?;
        assert_eq!(commits.len(), 3);

        let branches = api::local::branches::list(&repo)?;
        assert_eq!(branches.len(), 2);

        // Make sure we have both files on disk in our repo dir
        assert!(hello_file.exists());
        assert!(world_file.exists());

        // Go back to the main branch
        command::checkout(&repo, orig_branch.name).await?;

        // The world file should no longer be there
        assert!(hello_file.exists());
        assert!(!world_file.exists());

        // Go back to the world branch
        command::checkout(&repo, branch_name).await?;
        assert!(hello_file.exists());
        assert!(world_file.exists());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_checkout_added_file_keep_untracked() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|repo| async move {
        // Write the first file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;

        // Have another file lying around we will not remove
        let keep_file = repo.path.join("keep_me.txt");
        util::fs::write_to_path(&keep_file, "I am untracked, don't remove me")?;

        // Track & commit the file
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Added hello.txt")?;

        // Get the original branch name
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Create and checkout branch
        let branch_name = "feature/world-explorer";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Write a second file
        let world_file = repo.path.join("world.txt");
        util::fs::write_to_path(&world_file, "World")?;

        // Track & commit the second file in the branch
        command::add(&repo, &world_file)?;
        command::commit(&repo, "Added world.txt")?;

        // Make sure we have both commits after the initial
        let commits = api::local::commits::list(&repo)?;
        assert_eq!(commits.len(), 3);

        let branches = api::local::branches::list(&repo)?;
        assert_eq!(branches.len(), 2);

        // Make sure we have all files on disk in our repo dir
        assert!(hello_file.exists());
        assert!(world_file.exists());
        assert!(keep_file.exists());

        // Go back to the main branch
        command::checkout(&repo, orig_branch.name).await?;

        // The world file should no longer be there
        assert!(hello_file.exists());
        assert!(!world_file.exists());
        assert!(keep_file.exists());

        // Go back to the world branch
        command::checkout(&repo, branch_name).await?;
        assert!(hello_file.exists());
        assert!(world_file.exists());
        assert!(keep_file.exists());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_checkout_modified_file() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|repo| async move {
        // Write the first file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;

        // Track & commit the file
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Added hello.txt")?;

        // Get the original branch name
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Create and checkout branch
        let branch_name = "feature/world-explorer";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Modify the file
        let hello_file = test::modify_txt_file(hello_file, "World")?;

        // Track & commit the change in the branch
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Changed file to world")?;

        // It should say World at this point
        assert_eq!(util::fs::read_from_path(&hello_file)?, "World");

        // Go back to the main branch
        command::checkout(&repo, orig_branch.name).await?;

        // The file contents should be Hello, not World
        log::debug!("HELLO FILE NAME: {:?}", hello_file);
        assert!(hello_file.exists());

        // It should be reverted back to Hello
        assert_eq!(util::fs::read_from_path(&hello_file)?, "Hello");

        Ok(())
    })
    .await
}

#[test]
fn test_command_add_modified_file_in_subdirectory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        // Modify and add the file deep in a sub dir
        let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
        let file_contents = "file,label\ntrain/cat_1.jpg,0";
        test::modify_txt_file(one_shot_path, file_contents)?;
        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 1);
        // Add the top level directory, and make sure the modified file gets added
        let annotation_dir_path = repo.path.join("annotations");
        command::add(&repo, annotation_dir_path)?;
        let status = command::status(&repo)?;
        status.print_stdout();
        assert_eq!(status.added_files.len(), 1);
        command::commit(&repo, "Changing one shot")?;
        let status = command::status(&repo)?;
        assert!(status.is_clean());

        Ok(())
    })
}

#[tokio::test]
async fn test_command_checkout_modified_file_in_subdirectory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // Get the original branch name
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Track & commit the file
        let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
        command::add(&repo, &one_shot_path)?;
        command::commit(&repo, "Adding one shot")?;

        // Get OG file contents
        let og_content = util::fs::read_from_path(&one_shot_path)?;

        let branch_name = "feature/change-the-shot";
        api::local::branches::create_checkout(&repo, branch_name)?;

        let file_contents = "file,label\ntrain/cat_1.jpg,0\n";
        let one_shot_path = test::modify_txt_file(one_shot_path, file_contents)?;
        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 1);
        status.print_stdout();
        command::add(&repo, &one_shot_path)?;
        let status = command::status(&repo)?;
        status.print_stdout();
        command::commit(&repo, "Changing one shot")?;

        // checkout OG and make sure it reverts
        command::checkout(&repo, orig_branch.name).await?;
        let updated_content = util::fs::read_from_path(&one_shot_path)?;
        assert_eq!(og_content, updated_content);

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name).await?;
        let updated_content = util::fs::read_from_path(&one_shot_path)?;
        assert_eq!(file_contents, updated_content);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_checkout_modified_file_from_fully_committed_repo() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // Get the original branch name
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Track & commit all the data
        let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
        command::add(&repo, &repo.path)?;
        command::commit(&repo, "Adding one shot")?;

        // Get OG file contents
        let og_content = util::fs::read_from_path(&one_shot_path)?;

        let branch_name = "feature/modify-data";
        api::local::branches::create_checkout(&repo, branch_name)?;

        let file_contents = "file,label\ntrain/cat_1.jpg,0\n";
        let one_shot_path = test::modify_txt_file(one_shot_path, file_contents)?;
        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 1);
        command::add(&repo, &one_shot_path)?;
        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 0);
        assert_eq!(status.added_files.len(), 1);

        let status = command::status(&repo)?;
        status.print_stdout();
        command::commit(&repo, "Changing one shot")?;

        // checkout OG and make sure it reverts
        command::checkout(&repo, orig_branch.name).await?;
        let updated_content = util::fs::read_from_path(&one_shot_path)?;
        assert_eq!(og_content, updated_content);

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name).await?;
        let updated_content = util::fs::read_from_path(&one_shot_path)?;
        assert_eq!(file_contents, updated_content);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_commit_top_level_dir_then_revert() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // Get the original branch name
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Create a branch to make the changes
        let branch_name = "feature/adding-train";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Track & commit (train dir already created in helper)
        let train_path = repo.path.join("train");
        let og_num_files = util::fs::rcount_files_in_dir(&train_path);

        // Add directory
        command::add(&repo, &train_path)?;
        // Make sure we can get the status
        let status = command::status(&repo)?;
        assert_eq!(status.added_dirs.len(), 1);

        // Commit changes
        command::commit(&repo, "Adding train dir")?;
        // Make sure we can get the status and they are no longer added
        let status = command::status(&repo)?;
        assert_eq!(status.added_dirs.len(), 0);

        // checkout OG and make sure it removes the train dir
        command::checkout(&repo, orig_branch.name).await?;
        assert!(!train_path.exists());

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name).await?;
        assert!(train_path.exists());
        assert_eq!(util::fs::rcount_files_in_dir(&train_path), og_num_files);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_add_second_level_dir_then_revert() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // Get the original branch name
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Create a branch to make the changes
        let branch_name = "feature/adding-annotations";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Track & commit (dir already created in helper)
        let new_dir_path = repo.path.join("annotations").join("train");
        let og_num_files = util::fs::rcount_files_in_dir(&new_dir_path);

        command::add(&repo, &new_dir_path)?;
        command::commit(&repo, "Adding train dir")?;

        // checkout OG and make sure it removes the train dir
        command::checkout(&repo, orig_branch.name).await?;
        assert!(!new_dir_path.exists());

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name).await?;
        assert!(new_dir_path.exists());
        assert_eq!(util::fs::rcount_files_in_dir(&new_dir_path), og_num_files);

        Ok(())
    })
    .await
}

#[test]
fn test_command_add_removed_file() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // (file already created in helper)
        let file_to_remove = repo.path.join("labels.txt");

        // Commit the file
        command::add(&repo, &file_to_remove)?;
        command::commit(&repo, "Adding labels file")?;

        // Delete the file
        std::fs::remove_file(&file_to_remove)?;

        // We should recognize it as missing now
        let status = command::status(&repo)?;
        assert_eq!(status.removed_files.len(), 1);

        Ok(())
    })
}

#[tokio::test]
async fn test_command_restore_removed_file_from_branch_with_commits_between(
) -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // (file already created in helper)
        let file_to_remove = repo.path.join("labels.txt");

        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Commit the file
        command::add(&repo, &file_to_remove)?;
        command::commit(&repo, "Adding labels file")?;

        let train_dir = repo.path.join("train");
        command::add(&repo, train_dir)?;
        command::commit(&repo, "Adding train dir")?;

        // Branch
        api::local::branches::create_checkout(&repo, "remove-labels")?;

        // Delete the file
        std::fs::remove_file(&file_to_remove)?;

        // We should recognize it as missing now
        let status = command::status(&repo)?;
        assert_eq!(status.removed_files.len(), 1);

        // Commit removed file
        command::add(&repo, &file_to_remove)?;
        command::commit(&repo, "Removing labels file")?;

        // Make sure file is not there
        assert!(!file_to_remove.exists());

        // Switch back to main branch
        command::checkout(&repo, orig_branch.name).await?;
        // Make sure we restore file
        assert!(file_to_remove.exists());

        Ok(())
    })
    .await
}

#[test]
fn test_command_commit_removed_dir() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // (dir already created in helper)
        let dir_to_remove = repo.path.join("train");
        let og_file_count = util::fs::rcount_files_in_dir(&dir_to_remove);

        command::add(&repo, &dir_to_remove)?;
        command::commit(&repo, "Adding train directory")?;

        // Delete the directory
        std::fs::remove_dir_all(&dir_to_remove)?;

        // Add the deleted dir, so that we can commit the deletion
        command::add(&repo, &dir_to_remove)?;

        // Make sure we have the correct amount of files tagged as removed
        let status = command::status(&repo)?;
        assert_eq!(status.added_files.len(), og_file_count);
        assert_eq!(
            status.added_files.iter().next().unwrap().1.status,
            StagedEntryStatus::Removed
        );

        // Make sure they don't show up in the status
        assert_eq!(status.removed_files.len(), 0);

        Ok(())
    })
}

#[tokio::test]
async fn test_command_remove_dir_then_revert() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // Get the original branch name
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // (dir already created in helper)
        let dir_to_remove = repo.path.join("train");
        let og_num_files = util::fs::rcount_files_in_dir(&dir_to_remove);

        // track the dir
        command::add(&repo, &dir_to_remove)?;
        command::commit(&repo, "Adding train dir")?;

        // Create a branch to make the changes
        let branch_name = "feature/removing-train";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Delete the directory from disk
        std::fs::remove_dir_all(&dir_to_remove)?;

        // Track the deletion
        command::add(&repo, &dir_to_remove)?;
        command::commit(&repo, "Removing train dir")?;

        // checkout OG and make sure it restores the train dir
        command::checkout(&repo, orig_branch.name).await?;
        assert!(dir_to_remove.exists());
        assert_eq!(util::fs::rcount_files_in_dir(&dir_to_remove), og_num_files);

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name).await?;
        assert!(!dir_to_remove.exists());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_push_one_commit() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        let mut repo = repo;

        // Track the file
        let train_dir = repo.path.join("train");
        let num_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dir
        let commit = command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        let page_num = 1;
        let page_size = num_files + 10;
        let entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, "train", page_num, page_size)
                .await?;
        assert_eq!(entries.total_entries, num_files);
        assert_eq!(entries.entries.len(), num_files);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_command_push_one_commit_check_is_synced() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        let mut repo = repo;

        // Track the train and annotations dir
        let train_dir = repo.path.join("train");
        let annotations_dir = repo.path.join("annotations");

        command::add(&repo, &train_dir)?;
        command::add(&repo, &annotations_dir)?;
        // Commit the train dir
        let commit = command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Sleep so it can unpack...
        std::thread::sleep(std::time::Duration::from_secs(2));

        let is_synced = api::remote::commits::commit_is_synced(&remote_repo, &commit.id)
            .await?
            .unwrap();
        assert!(is_synced.is_valid);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_command_push_multiple_commit_check_is_synced() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        let mut repo = repo;

        // Track the train and annotations dir
        let train_dir = repo.path.join("train");
        let train_bounding_box = repo
            .path
            .join("annotations")
            .join("train")
            .join("bounding_box.csv");

        command::add(&repo, &train_dir)?;
        command::add(&repo, &train_bounding_box)?;
        // Commit the train dir
        command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Sleep so it can unpack...
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Add and commit the rest of the annotations
        // The nlp annotations have duplicates which broke the system at a time
        let annotations_dir = repo.path.join("nlp");
        command::add(&repo, &annotations_dir)?;
        let commit = command::commit(&repo, "adding the rest of the annotations")?;

        // Push again
        command::push(&repo).await?;

        let is_synced = api::remote::commits::commit_is_synced(&remote_repo, &commit.id)
            .await?
            .unwrap();
        assert!(is_synced.is_valid);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_command_push_inbetween_two_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        let mut repo = repo;
        // Track the train dir
        let train_dir = repo.path.join("train");
        let num_train_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dur
        command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push the files
        command::push(&repo).await?;

        // Track the test dir
        let test_dir = repo.path.join("test");
        let num_test_files = util::fs::count_files_in_dir(&test_dir);
        command::add(&repo, &test_dir)?;
        let commit = command::commit(&repo, "Adding test data")?;

        // Push the files
        command::push(&repo).await?;

        let page_num = 1;
        let page_size = num_train_files + num_test_files + 5;
        let train_entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, "/train", page_num, page_size)
                .await?;
        let test_entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, "/test", page_num, page_size)
                .await?;
        assert_eq!(
            train_entries.total_entries + test_entries.total_entries,
            num_train_files + num_test_files
        );
        assert_eq!(
            train_entries.entries.len() + test_entries.entries.len(),
            num_train_files + num_test_files
        );

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_command_push_after_two_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        // Make mutable copy so we can set remote
        let mut repo = repo;

        // Track the train dir
        let train_dir = repo.path.join("train");
        command::add(&repo, &train_dir)?;
        // Commit the train dur
        command::commit(&repo, "Adding training data")?;

        // Track the test dir
        let test_dir = repo.path.join("test");
        let num_test_files = util::fs::rcount_files_in_dir(&test_dir);
        command::add(&repo, &test_dir)?;
        let commit = command::commit(&repo, "Adding test data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push the files
        command::push(&repo).await?;

        let page_num = 1;
        let entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, ".", page_num, 10).await?;
        assert_eq!(entries.total_entries, 2);
        assert_eq!(entries.entries.len(), 2);

        let page_size = num_test_files + 10;
        let entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, "test", page_num, page_size)
                .await?;
        assert_eq!(entries.total_entries, num_test_files);
        assert_eq!(entries.entries.len(), num_test_files);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

// At some point we were adding rocksdb inside the working dir...def should not do that
#[test]
fn test_command_add_dot_should_not_add_new_files() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let num_files = util::fs::count_files_in_dir(&repo.path);

        command::add(&repo, &repo.path)?;

        // Add shouldn't add any new files in the working dir
        let num_files_after_add = util::fs::count_files_in_dir(&repo.path);

        assert_eq!(num_files, num_files_after_add);

        Ok(())
    })
}

// This broke when you tried to add the "." directory to add everything, after already committing the train directory.
#[tokio::test]
async fn test_command_push_after_two_commits_adding_dot() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        // Make mutable copy so we can set remote
        let mut repo = repo;

        // Track the train dir
        let train_dir = repo.path.join("train");

        command::add(&repo, &train_dir)?;
        // Commit the train dur
        command::commit(&repo, "Adding training data")?;

        // Track the rest of the files
        let full_dir = &repo.path;
        let num_files = util::fs::count_items_in_dir(full_dir);
        command::add(&repo, full_dir)?;
        let commit = command::commit(&repo, "Adding rest of data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push the files
        command::push(&repo).await?;

        let page_num = 1;
        let page_size = num_files + 10;
        let entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, ".", page_num, page_size).await?;
        assert_eq!(entries.total_entries, num_files);
        assert_eq!(entries.entries.len(), num_files);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_cannot_push_if_remote_not_set() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // Track the file
        let train_dirname = "train";
        let train_dir = repo.path.join(train_dirname);
        command::add(&repo, &train_dir)?;
        // Commit the train dir
        command::commit(&repo, "Adding training data")?;

        // Should not be able to push
        let result = command::push(&repo).await;
        assert!(result.is_err());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_push_clone_pull_push() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track the file
        let train_dirname = "train";
        let train_dir = repo.path.join(train_dirname);
        let og_num_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dir
        command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Add a new file
        let party_ppl_filename = "party_ppl.txt";
        let party_ppl_contents = String::from("Wassup Party Ppl");
        let party_ppl_file_path = repo.path.join(party_ppl_filename);
        util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents)?;

        // Add and commit and push
        command::add(&repo, &party_ppl_file_path)?;
        let latest_commit = command::commit(&repo, "Adding party_ppl.txt")?;
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            let oxen_dir = cloned_repo.path.join(".oxen");
            assert!(oxen_dir.exists());
            command::pull(&cloned_repo).await?;

            // Make sure we pulled all of the train dir
            let cloned_train_dir = cloned_repo.path.join(train_dirname);
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_train_dir);
            assert_eq!(og_num_files, cloned_num_files);

            // Make sure we have the party ppl file from the next commit
            let cloned_party_ppl_path = cloned_repo.path.join(party_ppl_filename);
            assert!(cloned_party_ppl_path.exists());
            let cloned_contents = util::fs::read_from_path(&cloned_party_ppl_path)?;
            assert_eq!(cloned_contents, party_ppl_contents);

            // Make sure that pull updates local HEAD to be correct
            let head = api::local::commits::head_commit(&cloned_repo)?;
            assert_eq!(head.id, latest_commit.id);

            // Make sure we synced all the commits
            let repo_commits = api::local::commits::list(&repo)?;
            let cloned_commits = api::local::commits::list(&cloned_repo)?;
            assert_eq!(repo_commits.len(), cloned_commits.len());

            // Make sure we updated the dbs properly
            let status = command::status(&cloned_repo)?;
            assert!(status.is_clean());

            // Have this side add a file, and send it back over
            let send_it_back_filename = "send_it_back.txt";
            let send_it_back_contents = String::from("Hello from the other side");
            let send_it_back_file_path = cloned_repo.path.join(send_it_back_filename);
            util::fs::write_to_path(&send_it_back_file_path, &send_it_back_contents)?;

            // Add and commit and push
            command::add(&cloned_repo, &send_it_back_file_path)?;
            command::commit(&cloned_repo, "Adding send_it_back.txt")?;
            command::push(&cloned_repo).await?;

            // Pull back from the OG Repo
            command::pull(&repo).await?;
            let old_repo_status = command::status(&repo)?;
            old_repo_status.print_stdout();
            // Make sure we don't modify the timestamps or anything of the OG data
            assert!(!old_repo_status.has_modified_entries());

            let pulled_send_it_back_path = repo.path.join(send_it_back_filename);
            assert!(pulled_send_it_back_path.exists());
            let pulled_contents = util::fs::read_from_path(&pulled_send_it_back_path)?;
            assert_eq!(pulled_contents, send_it_back_contents);

            // Modify the party ppl contents
            let party_ppl_contents = String::from("Late to the party");
            util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents)?;
            command::add(&repo, &party_ppl_file_path)?;
            command::commit(&repo, "Modified party ppl contents")?;
            command::push(&repo).await?;

            // Pull the modifications
            command::pull(&cloned_repo).await?;
            let pulled_contents = util::fs::read_from_path(&cloned_party_ppl_path)?;
            assert_eq!(pulled_contents, party_ppl_contents);

            println!("----BEFORE-----");
            // Remove a file, add, commit, push the change
            std::fs::remove_file(&send_it_back_file_path)?;
            command::add(&cloned_repo, &send_it_back_file_path)?;
            command::commit(&cloned_repo, "Removing the send it back file")?;
            command::push(&cloned_repo).await?;
            println!("----AFTER-----");

            // Pull down the changes and make sure the file is removed
            command::pull(&repo).await?;
            let pulled_send_it_back_path = repo.path.join(send_it_back_filename);
            assert!(!pulled_send_it_back_path.exists());

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

// This specific flow broke during a demo
// * add file *
// push
// pull
// * modify file *
// push
// pull
// * remove file *
// push
#[tokio::test]
async fn test_command_add_modify_remove_push_pull() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a file
        let filename = "labels.txt";
        let filepath = repo.path.join(filename);
        command::add(&repo, &filepath)?;
        command::commit(&repo, "Adding labels file")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            command::pull(&cloned_repo).await?;

            // Modify the file in the cloned dir
            let cloned_filepath = cloned_repo.path.join(filename);
            let changed_content = "messing up the labels";
            util::fs::write_to_path(&cloned_filepath, changed_content)?;
            command::add(&cloned_repo, &cloned_filepath)?;
            command::commit(&cloned_repo, "I messed with the label file")?;

            // Push back to server
            command::push(&cloned_repo).await?;

            // Pull back to original guy
            command::pull(&repo).await?;

            // Make sure content changed
            let pulled_content = util::fs::read_from_path(&filepath)?;
            assert_eq!(pulled_content, changed_content);

            // Delete the file in the og filepath
            std::fs::remove_file(&filepath)?;

            // Stage & Commit & Push the removal
            command::add(&repo, &filepath)?;
            command::commit(&repo, "You mess with it, I remove it")?;
            command::push(&repo).await?;

            command::pull(&cloned_repo).await?;
            assert!(!cloned_filepath.exists());

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_pull_multiple_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a file
        let filename = "labels.txt";
        let file_path = repo.path.join(filename);
        command::add(&repo, &file_path)?;
        command::commit(&repo, "Adding labels file")?;

        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        let test_path = repo.path.join("test");
        command::add(&repo, &test_path)?;
        command::commit(&repo, "Adding test dir")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            command::pull(&cloned_repo).await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // 2 test, 5 train, 1 labels
            assert_eq!(8, cloned_num_files);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_clone_full() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a file
        let filename = "labels.txt";
        let file_path = repo.path.join(filename);
        command::add(&repo, &file_path)?;
        command::commit(&repo, "Adding labels file")?;

        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        let test_path = repo.path.join("test");
        command::add(&repo, &test_path)?;
        command::commit(&repo, "Adding test dir")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = false; // full pull
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // 2 test, 5 train, 1 labels
            assert_eq!(8, cloned_num_files);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_pull_data_frame() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a file
        let filename = "annotations/train/bounding_box.csv";
        let file_path = repo.path.join(filename);
        let og_df = tabular::read_df(&file_path, DFOpts::empty())?;
        let og_contents = util::fs::read_from_path(&file_path)?;

        command::add(&repo, &file_path)?;
        command::commit(&repo, "Adding bounding box file")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            command::pull(&cloned_repo).await?;
            let file_path = cloned_repo.path.join(filename);

            let cloned_df = tabular::read_df(&file_path, DFOpts::empty())?;
            let cloned_contents = util::fs::read_from_path(&file_path)?;
            assert_eq!(og_df.height(), cloned_df.height());
            assert_eq!(og_df.width(), cloned_df.width());
            assert_eq!(cloned_contents, og_contents);

            // Status should be empty too
            let status = command::status(&cloned_repo)?;
            status.print_stdout();
            assert!(status.is_clean());

            // Make sure that the schema gets pulled
            let schemas = command::schemas::list(&repo, None)?;
            assert!(!schemas.is_empty());

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

// Test that we pull down the proper data frames
#[tokio::test]
async fn test_pull_multiple_data_frames_multiple_schemas() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
        let filename = "nlp/classification/annotations/train.tsv";
        let file_path = repo.path.join(filename);
        let og_df = tabular::read_df(&file_path, DFOpts::empty())?;
        let og_sentiment_contents = util::fs::read_from_path(&file_path)?;

        let schemas = command::schemas::list(&repo, None)?;
        let num_schemas = schemas.len();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            command::pull(&cloned_repo).await?;

            let filename = "nlp/classification/annotations/train.tsv";
            let file_path = cloned_repo.path.join(filename);
            let cloned_df = tabular::read_df(&file_path, DFOpts::empty())?;
            let cloned_contents = util::fs::read_from_path(&file_path)?;
            assert_eq!(og_df.height(), cloned_df.height());
            assert_eq!(og_df.width(), cloned_df.width());
            assert_eq!(cloned_contents, og_sentiment_contents);
            println!("Cloned {filename:?} {cloned_df}");

            // Status should be empty too
            let status = command::status(&cloned_repo)?;
            status.print_stdout();
            assert!(status.is_clean());

            // Make sure we grab the same amount of schemas
            let pulled_schemas = command::schemas::list(&repo, None)?;
            assert_eq!(pulled_schemas.len(), num_schemas);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

// Make sure we can push again after pulling on the other side, then pull again
#[tokio::test]
async fn test_push_pull_push_pull_on_branch() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a dir
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        // Track larger files
        let larger_dir = repo.path.join("large_files");
        command::add(&repo, &larger_dir)?;
        command::commit(&repo, "Adding larger files")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            command::pull(&cloned_repo).await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            assert_eq!(6, cloned_num_files);
            let og_commits = api::local::commits::list(&repo)?;
            let cloned_commits = api::local::commits::list(&cloned_repo)?;
            assert_eq!(og_commits.len(), cloned_commits.len());

            // Create a branch to collab on
            let branch_name = "adding-training-data";
            api::local::branches::create_checkout(&cloned_repo, branch_name)?;

            // Track some more data in the cloned repo
            let hotdog_path = Path::new("data/test/images/hotdog_1.jpg");
            let new_file_path = cloned_repo.path.join("train").join("hotdog_1.jpg");
            util::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&cloned_repo, &new_file_path)?;
            command::commit(&cloned_repo, "Adding one file to train dir")?;

            // Push it back
            command::push_remote_branch(&cloned_repo, constants::DEFAULT_REMOTE_NAME, branch_name)
                .await?;

            // Pull it on the OG side
            command::pull_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);
            // Now there should be 7 train files
            assert_eq!(7, og_num_files);

            // Add another file on the OG side, and push it back
            let hotdog_path = Path::new("data/test/images/hotdog_2.jpg");
            let new_file_path = train_path.join("hotdog_2.jpg");
            util::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&repo, &train_path)?;
            command::commit(&repo, "Adding next file to train dir")?;
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            // Pull it on the second side again
            command::pull_remote_branch(&cloned_repo, constants::DEFAULT_REMOTE_NAME, branch_name)
                .await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // Now there should be 7 train/ files and 1 in large_files/
            assert_eq!(8, cloned_num_files);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

// Make sure we can push again after pulling on the other side, then pull again
#[tokio::test]
async fn test_push_pull_push_pull_on_other_branch() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|mut repo| async move {
        // Track a dir
        let train_dir = repo.path.join("train");
        let train_paths = vec![
            Path::new("data/test/images/cat_1.jpg"),
            Path::new("data/test/images/cat_2.jpg"),
            Path::new("data/test/images/cat_3.jpg"),
            Path::new("data/test/images/dog_1.jpg"),
            Path::new("data/test/images/dog_2.jpg"),
        ];
        std::fs::create_dir_all(&train_dir)?;
        for path in train_paths.iter() {
            util::fs::copy(path, train_dir.join(path.file_name().unwrap()))?;
        }

        command::add(&repo, &train_dir)?;
        command::commit(&repo, "Adding train dir")?;

        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            command::pull(&cloned_repo).await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // the original training files
            assert_eq!(train_paths.len(), cloned_num_files);

            // Create a branch to collaborate on
            let branch_name = "adding-training-data";
            api::local::branches::create_checkout(&cloned_repo, branch_name)?;

            // Track some more data in the cloned repo
            let hotdog_path = Path::new("data/test/images/hotdog_1.jpg");
            let new_file_path = cloned_repo.path.join("train").join("hotdog_1.jpg");
            util::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&cloned_repo, &new_file_path)?;
            command::commit(&cloned_repo, "Adding one file to train dir")?;

            // Push it back
            command::push_remote_branch(&cloned_repo, constants::DEFAULT_REMOTE_NAME, branch_name)
                .await?;

            // Pull it on the OG side
            command::pull_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, &og_branch.name)
                .await?;
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);
            // Now there should be still be the original train files, not the new file
            assert_eq!(train_paths.len(), og_num_files);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_push_branch_with_with_no_new_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a dir
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        let new_branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, new_branch_name)?;

        // Push new branch, without any new commits, should still create the branch
        command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, new_branch_name).await?;

        let remote_branches = api::remote::branches::list(&remote_repo).await?;
        assert_eq!(2, remote_branches.len());

        api::remote::repositories::delete(&remote_repo).await?;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_delete_remote_branch() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // Create new branch
        let new_branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, new_branch_name)?;

        // Push new branch
        command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, new_branch_name).await?;

        // Delete the branch
        api::remote::branches::delete(&remote_repo, new_branch_name).await?;

        let remote_branches = api::remote::branches::list(&remote_repo).await?;
        assert_eq!(1, remote_branches.len());

        api::remote::repositories::delete(&remote_repo).await?;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_should_not_push_branch_that_does_not_exist() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push main branch first
        if command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, "main")
            .await
            .is_err()
        {
            panic!("Pushing main branch should work");
        }

        // Then try to push branch that doesn't exist
        if command::push_remote_branch(
            &repo,
            constants::DEFAULT_REMOTE_NAME,
            "branch-does-not-exist",
        )
        .await
        .is_ok()
        {
            panic!("Should not be able to push branch that does not exist");
        }

        let remote_branches = api::remote::branches::list(&remote_repo).await?;
        assert_eq!(1, remote_branches.len());

        api::remote::repositories::delete(&remote_repo).await?;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_pull_full_commit_history() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // First commit
        let filename = "labels.txt";
        let filepath = repo.path.join(filename);
        command::add(&repo, &filepath)?;
        command::commit(&repo, "Adding labels file")?;

        // Second commit
        let new_filename = "new.txt";
        let new_filepath = repo.path.join(new_filename);
        util::fs::write_to_path(&new_filepath, "hallo")?;
        command::add(&repo, &new_filepath)?;
        command::commit(&repo, "Adding a new file")?;

        // Third commit
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        // Fourth commit
        let test_path = repo.path.join("test");
        command::add(&repo, &test_path)?;
        command::commit(&repo, "Adding test dir")?;

        // Get local history
        let local_history = api::local::commits::list(&repo)?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            command::pull(&cloned_repo).await?;

            // Get cloned history
            let cloned_history = api::local::commits::list(&cloned_repo)?;

            // Make sure the histories match
            assert_eq!(local_history.len(), cloned_history.len());

            // Make sure we have grabbed all the history dirs
            let hidden_dir = util::fs::oxen_hidden_dir(&cloned_repo.path);
            let history_dir = hidden_dir.join(Path::new(constants::HISTORY_DIR));
            for commit in cloned_history.iter() {
                let commit_history_dir = history_dir.join(&commit.id);
                assert!(commit_history_dir.exists());

                // make sure we can successfully open the db and read entries
                let reader = CommitDirReader::new(&cloned_repo, commit)?;
                let entries = reader.list_entries();
                assert!(entries.is_ok());
            }

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[test]
fn test_do_not_commit_any_files_on_init() -> Result<(), OxenError> {
    test::run_empty_dir_test(|dir| {
        test::populate_dir_with_training_data(dir)?;

        let repo = command::init(dir)?;
        let commits = api::local::commits::list(&repo)?;
        let commit = commits.last().unwrap();
        let reader = CommitDirReader::new(&repo, commit)?;
        let num_entries = reader.num_entries()?;
        assert_eq!(num_entries, 0);

        Ok(())
    })
}

#[tokio::test]
async fn test_delete_branch() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // Get the original branches
        let og_branches = api::local::branches::list(&repo)?;
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Must checkout main again before deleting
        command::checkout(&repo, og_branch.name).await?;

        // Now we can delete
        api::local::branches::delete(&repo, branch_name)?;

        // Should be same num as og_branches
        let leftover_branches = api::local::branches::list(&repo)?;
        assert_eq!(og_branches.len(), leftover_branches.len());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_cannot_delete_branch_you_are_on() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add another commit on this branch that moves us ahead of main
        if api::local::branches::delete(&repo, branch_name).is_ok() {
            panic!("Should not be able to delete the branch you are on");
        }

        Ok(())
    })
    .await
}

#[test]
fn test_cannot_force_delete_branch_you_are_on() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add another commit on this branch that moves us ahead of main
        if api::local::branches::force_delete(&repo, branch_name).is_ok() {
            panic!("Should not be able to force delete the branch you are on");
        }

        Ok(())
    })
}

#[tokio::test]
async fn test_cannot_delete_branch_that_is_ahead_of_current() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        let og_branches = api::local::branches::list(&repo)?;
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add another commit on this branch
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        // Checkout main again
        command::checkout(&repo, og_branch.name).await?;

        // Should not be able to delete `my-branch` because it is ahead of `main`
        if api::local::branches::delete(&repo, branch_name).is_ok() {
            panic!("Should not be able to delete the branch that is ahead of the one you are on");
        }

        // Should be one less branch
        let leftover_branches = api::local::branches::list(&repo)?;
        assert_eq!(og_branches.len(), leftover_branches.len() - 1);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_force_delete_branch_that_is_ahead_of_current() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        let og_branches = api::local::branches::list(&repo)?;
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add another commit on this branch
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        // Checkout main again
        command::checkout(&repo, og_branch.name).await?;

        // Force delete
        api::local::branches::force_delete(&repo, branch_name)?;

        // Should be one less branch
        let leftover_branches = api::local::branches::list(&repo)?;
        assert_eq!(og_branches.len(), leftover_branches.len());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_merge_conflict_shows_in_status() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Add a "none" category on a branch
        let branch_name = "change-labels";
        api::local::branches::create_checkout(&repo, branch_name)?;

        test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding none category")?;

        // Add a "person" category on a the main branch
        command::checkout(&repo, og_branch.name).await?;

        test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding person category")?;

        // Try to merge in the changes
        let commit = command::merge(&repo, branch_name)?;

        // Make sure we didn't get a commit out of it
        assert!(commit.is_none());

        // Make sure we can access the conflicts in the status command
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_can_add_merge_conflict() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Add a "none" category on a branch
        let branch_name = "change-labels";
        api::local::branches::create_checkout(&repo, branch_name)?;

        test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding none category")?;

        // Add a "person" category on a the main branch
        command::checkout(&repo, og_branch.name).await?;

        test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding person category")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Assume that we fixed the conflict and added the file
        let path = status.merge_conflicts[0].base_entry.path.clone();
        let fullpath = repo.path.join(path);
        command::add(&repo, fullpath)?;

        // Adding should add to added files
        let status = command::status(&repo)?;

        assert_eq!(status.added_files.len(), 1);

        // Adding should get rid of the merge conflict
        assert_eq!(status.merge_conflicts.len(), 0);

        Ok(())
    })
    .await
}

// Test diff during a merge conflict should show conflicts for a dataframe
#[tokio::test]
async fn test_has_merge_conflicts_without_merging() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|repo| async move {
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();
        let data_path = repo.path.join("data.csv");
        util::fs::write_to_path(&data_path, "file,label\nimages/0.png,dog\n")?;
        command::add(&repo, &data_path)?;
        command::commit(&repo, "Add initial data.csv file with dog")?;

        // Add a fish label to the file on a branch
        let fish_branch_name = "add-fish-label";
        api::local::branches::create_checkout(&repo, fish_branch_name)?;
        let data_path = test::append_line_txt_file(data_path, "images/fish.png,fish\n")?;
        command::add(&repo, &data_path)?;
        command::commit(&repo, "Adding fish to data.csv file")?;

        // Checkout main, and branch from it to another branch to add a cat label
        command::checkout(&repo, &og_branch.name).await?;
        let cat_branch_name = "add-cat-label";
        api::local::branches::create_checkout(&repo, cat_branch_name)?;
        let data_path = test::append_line_txt_file(data_path, "images/cat.png,cat\n")?;
        command::add(&repo, &data_path)?;
        command::commit(&repo, "Adding cat to data.csv file")?;

        // Checkout main again
        command::checkout(&repo, &og_branch.name).await?;

        // Merge the fish branch in
        let result = command::merge(&repo, fish_branch_name)?;
        assert!(result.is_some());

        // And then the cat branch should have conflicts
        let result = command::merge(&repo, cat_branch_name)?;
        assert!(result.is_none());

        // Make sure we can access the conflicts in the status command
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Get the diff dataframe
        let diff = command::diff(&repo, None, &data_path)?;
        log::debug!("{diff:?}");

        assert_eq!(
            diff,
            r"Added Rows

shape: (1, 2)
┌────────────────┬───────┐
│ file           ┆ label │
│ ---            ┆ ---   │
│ str            ┆ str   │
╞════════════════╪═══════╡
│ images/cat.png ┆ cat   │
└────────────────┴───────┘


Removed Rows

shape: (1, 2)
┌─────────────────┬───────┐
│ file            ┆ label │
│ ---             ┆ ---   │
│ str             ┆ str   │
╞═════════════════╪═══════╡
│ images/fish.png ┆ fish  │
└─────────────────┴───────┘

"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_commit_after_merge_conflict() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Add a "none" category on a branch
        let branch_name = "change-labels";
        api::local::branches::create_checkout(&repo, branch_name)?;

        test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding none category")?;

        // Add a "person" category on a the main branch
        command::checkout(&repo, og_branch.name).await?;

        test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding person category")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        // We should have a conflict
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Assume that we fixed the conflict and added the file
        let path = status.merge_conflicts[0].base_entry.path.clone();
        let fullpath = repo.path.join(path);
        command::add(&repo, fullpath)?;

        // Should commit, and then see full commit history
        command::commit(&repo, "merging into main")?;

        // Should have commits:
        //  1) initial
        //  2) add labels
        //  3) change-labels branch modification
        //  4) main branch modification
        //  5) merge commit
        let history = api::local::commits::list(&repo)?;
        assert_eq!(history.len(), 5);

        Ok(())
    })
    .await
}

#[test]
fn test_add_nested_nlp_dir() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let dir = Path::new("nlp");
        let repo_dir = repo.path.join(dir);
        command::add(&repo, repo_dir)?;

        let status = command::status(&repo)?;
        status.print_stdout();

        // Should add all the sub dirs
        // nlp/
        //   classification/
        //     annotations/
        assert_eq!(
            status.added_dirs.paths.get(Path::new("nlp")).unwrap().len(),
            3
        );
        // Should add sub files
        // nlp/classification/annotations/train.tsv
        // nlp/classification/annotations/test.tsv
        assert_eq!(status.added_files.len(), 2);

        Ok(())
    })
}

#[tokio::test]
async fn test_add_commit_push_pull_file_without_extension() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        let filename = "LICENSE";
        let filepath = repo.path.join(filename);

        let og_content = "I am the License.";
        test::write_txt_file_to_path(&filepath, og_content)?;

        command::add(&repo, filepath)?;
        let commit = command::commit(&repo, "Adding file without extension");

        assert!(commit.is_ok());

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;
            command::pull(&cloned_repo).await?;
            let filepath = cloned_repo.path.join(filename);
            let content = util::fs::read_from_path(&filepath)?;
            assert_eq!(og_content, content);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[test]
fn test_restore_directory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let history = api::local::commits::list(&repo)?;
        let last_commit = history.first().unwrap();

        let annotations_dir = Path::new("annotations");

        // Remove one file
        let bbox_file = annotations_dir.join("train").join("bounding_box.csv");
        let bbox_path = repo.path.join(bbox_file);

        let og_bbox_contents = util::fs::read_from_path(&bbox_path)?;
        std::fs::remove_file(&bbox_path)?;

        // Modify another file
        let readme_file = annotations_dir.join("README.md");
        let readme_path = repo.path.join(readme_file);
        let og_readme_contents = util::fs::read_from_path(&readme_path)?;

        let readme_path = test::append_line_txt_file(readme_path, "Adding s'more")?;

        // Restore the directory
        command::restore(
            &repo,
            RestoreOpts::from_path_ref(annotations_dir, last_commit.id.clone()),
        )?;

        // Make sure the removed file is restored
        let restored_contents = util::fs::read_from_path(&bbox_path)?;
        assert_eq!(og_bbox_contents, restored_contents);

        // Make sure the modified file is restored
        let restored_contents = util::fs::read_from_path(&readme_path)?;
        assert_eq!(og_readme_contents, restored_contents);

        Ok(())
    })
}

#[test]
fn test_restore_removed_tabular_data() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let history = api::local::commits::list(&repo)?;
        let last_commit = history.first().unwrap();

        let bbox_file = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_path = repo.path.join(&bbox_file);

        let og_contents = util::fs::read_from_path(&bbox_path)?;
        std::fs::remove_file(&bbox_path)?;

        command::restore(
            &repo,
            RestoreOpts::from_path_ref(bbox_file, last_commit.id.clone()),
        )?;
        let restored_contents = util::fs::read_from_path(&bbox_path)?;
        assert_eq!(og_contents, restored_contents);

        Ok(())
    })
}

#[test]
fn test_restore_modified_tabular_data() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let history = api::local::commits::list(&repo)?;
        let last_commit = history.first().unwrap();

        let bbox_file = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_path = repo.path.join(&bbox_file);

        let og_contents = util::fs::read_from_path(&bbox_path)?;

        let mut opts = DFOpts::empty();
        opts.add_row = Some("train/dog_99.jpg,dog,101.5,32.0,385,330".to_string());
        opts.content_type = ContentType::Csv;
        let mut df = tabular::read_df(&bbox_path, opts)?;
        tabular::write_df(&mut df, &bbox_path)?;

        command::restore(
            &repo,
            RestoreOpts::from_path_ref(bbox_file, last_commit.id.clone()),
        )?;
        let restored_contents = util::fs::read_from_path(&bbox_path)?;
        assert_eq!(og_contents, restored_contents);

        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 0);
        assert!(status.is_clean());

        Ok(())
    })
}

#[test]
fn test_restore_modified_text_data() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let history = api::local::commits::list(&repo)?;
        let last_commit = history.first().unwrap();

        let bbox_file = Path::new("annotations")
            .join("train")
            .join("annotations.txt");
        let bbox_path = repo.path.join(&bbox_file);

        let og_contents = util::fs::read_from_path(&bbox_path)?;
        let new_contents = format!("{og_contents}\nnew 0");
        util::fs::write_to_path(&bbox_path, &new_contents)?;

        command::restore(
            &repo,
            RestoreOpts::from_path_ref(bbox_file, last_commit.id.clone()),
        )?;
        let restored_contents = util::fs::read_from_path(&bbox_path)?;
        assert_eq!(og_contents, restored_contents);

        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 0);
        assert!(status.is_clean());

        Ok(())
    })
}

#[test]
fn test_restore_staged_file() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let bbox_file = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_path = repo.path.join(&bbox_file);

        // Stage file
        command::add(&repo, bbox_path)?;

        // Make sure is staged
        let status = command::status(&repo)?;
        assert_eq!(status.added_files.len(), 1);
        status.print_stdout();

        // Remove from staged
        command::restore(&repo, RestoreOpts::from_staged_path(bbox_file))?;

        // Make sure is unstaged
        let status = command::status(&repo)?;
        assert_eq!(status.added_files.len(), 0);

        Ok(())
    })
}

#[test]
fn test_restore_data_frame_with_duplicates() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let ann_file = Path::new("nlp")
            .join("classification")
            .join("annotations")
            .join("train.tsv");
        let ann_path = repo.path.join(&ann_file);
        let orig_df = tabular::read_df(&ann_path, DFOpts::empty())?;
        let og_contents = util::fs::read_from_path(&ann_path)?;

        // Commit
        command::add(&repo, &ann_path)?;
        let commit = command::commit(&repo, "adding data with duplicates")?;

        // Remove
        std::fs::remove_file(&ann_path)?;

        // Restore from commit
        command::restore(&repo, RestoreOpts::from_path_ref(ann_file, commit.id))?;

        // Make sure is same size
        let restored_df = tabular::read_df(&ann_path, DFOpts::empty())?;
        assert_eq!(restored_df.height(), orig_df.height());
        assert_eq!(restored_df.width(), orig_df.width());

        let restored_contents = util::fs::read_from_path(&ann_path)?;
        assert_eq!(og_contents, restored_contents);

        Ok(())
    })
}

#[test]
fn test_restore_bounding_box_data_frame() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let ann_file = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let ann_path = repo.path.join(&ann_file);
        let orig_df = tabular::read_df(&ann_path, DFOpts::empty())?;
        let og_contents = util::fs::read_from_path(&ann_path)?;

        // Commit
        command::add(&repo, &ann_path)?;
        let commit = command::commit(&repo, "adding data with duplicates")?;

        // Remove
        std::fs::remove_file(&ann_path)?;

        // Restore from commit
        command::restore(&repo, RestoreOpts::from_path_ref(ann_file, commit.id))?;

        // Make sure is same size
        let restored_df = tabular::read_df(&ann_path, DFOpts::empty())?;
        assert_eq!(restored_df.height(), orig_df.height());
        assert_eq!(restored_df.width(), orig_df.width());

        let restored_contents = util::fs::read_from_path(&ann_path)?;
        assert_eq!(og_contents, restored_contents);

        Ok(())
    })
}

#[test]
fn test_restore_staged_directory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let relative_path = Path::new("annotations");
        let annotations_dir = repo.path.join(relative_path);

        // Stage file
        command::add(&repo, annotations_dir)?;

        // Make sure is staged
        let status = command::status(&repo)?;
        assert_eq!(status.added_dirs.len(), 1);
        assert_eq!(status.added_files.len(), 6);
        status.print_stdout();

        // Remove from staged
        command::restore(&repo, RestoreOpts::from_staged_path(relative_path))?;

        // Make sure is unstaged
        let status = command::status(&repo)?;
        assert_eq!(status.added_dirs.len(), 0);
        assert_eq!(status.added_files.len(), 0);

        Ok(())
    })
}

#[test]
fn test_command_schema_list() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let schemas = command::schemas::list(&repo, None)?;
        assert_eq!(schemas.len(), 3);

        let schema = command::schemas::get_from_head(&repo, "bounding_box")?.unwrap();

        assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
        assert_eq!(schema.fields.len(), 6);
        assert_eq!(schema.fields[0].name, "file");
        assert_eq!(schema.fields[0].dtype, "str");
        assert_eq!(schema.fields[1].name, "label");
        assert_eq!(schema.fields[1].dtype, "str");
        assert_eq!(schema.fields[2].name, "min_x");
        assert_eq!(schema.fields[2].dtype, "f64");
        assert_eq!(schema.fields[3].name, "min_y");
        assert_eq!(schema.fields[3].dtype, "f64");
        assert_eq!(schema.fields[4].name, "width");
        assert_eq!(schema.fields[4].dtype, "i64");
        assert_eq!(schema.fields[5].name, "height");
        assert_eq!(schema.fields[5].dtype, "i64");

        Ok(())
    })
}

#[test]
fn test_stage_and_commit_schema() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Make sure no schemas are staged
        let status = command::status(&repo)?;
        assert_eq!(status.added_schemas.len(), 0);

        // Make sure no schemas are committed
        let schemas = command::schemas::list(&repo, None)?;
        assert_eq!(schemas.len(), 0);

        // Schema should be staged when added
        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(bbox_filename);
        command::add(&repo, bbox_file)?;

        // Make sure it is staged
        let status = command::status(&repo)?;
        assert_eq!(status.added_schemas.len(), 1);
        for (path, schema) in status.added_schemas.iter() {
            println!("GOT SCHEMA {path:?} -> {schema:?}");
        }

        // name the schema when staged
        let schema_ref = "b821946753334c083124fd563377d795";
        let schema_name = "bounding_box";
        command::schemas::set_name(&repo, schema_ref, schema_name)?;

        // Schema should be committed after commit
        command::commit(&repo, "Adding bounding box schema")?;

        // Make sure no schemas are staged after commit
        let status = command::status(&repo)?;
        assert_eq!(status.added_schemas.len(), 0);

        // Fetch schema from HEAD commit
        let schema = command::schemas::get_from_head(&repo, "bounding_box")?.unwrap();

        assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
        assert_eq!(schema.fields.len(), 6);
        assert_eq!(schema.fields[0].name, "file");
        assert_eq!(schema.fields[0].dtype, "str");
        assert_eq!(schema.fields[1].name, "label");
        assert_eq!(schema.fields[1].dtype, "str");
        assert_eq!(schema.fields[2].name, "min_x");
        assert_eq!(schema.fields[2].dtype, "f64");
        assert_eq!(schema.fields[3].name, "min_y");
        assert_eq!(schema.fields[3].dtype, "f64");
        assert_eq!(schema.fields[4].name, "width");
        assert_eq!(schema.fields[4].dtype, "i64");
        assert_eq!(schema.fields[5].name, "height");
        assert_eq!(schema.fields[5].dtype, "i64");

        Ok(())
    })
}

#[tokio::test]
async fn test_command_merge_dataframe_conflict_both_added_rows_checkout_theirs(
) -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Add a more rows on this branch
        let branch_name = "ox-add-rows";
        api::local::branches::create_checkout(&repo, branch_name)?;

        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);
        let bbox_file =
            test::append_line_txt_file(bbox_file, "train/cat_3.jpg,cat,41.0,31.5,410,427")?;
        let their_branch_contents = util::fs::read_from_path(&bbox_file)?;
        let their_df = tabular::read_df(&bbox_file, DFOpts::empty())?;
        println!("their df {their_df}");

        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

        // Add a more rows on the main branch
        command::checkout(&repo, og_branch.name).await?;

        let bbox_file =
            test::append_line_txt_file(bbox_file, "train/dog_4.jpg,dog,52.0,62.5,256,429")?;

        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation on main branch")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        // We should have a conflict....
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Run command::checkout_theirs() and make sure their changes get kept
        command::checkout_theirs(&repo, &bbox_filename)?;
        let restored_df = tabular::read_df(&bbox_file, DFOpts::empty())?;
        println!("restored df {restored_df}");

        let file_contents = util::fs::read_from_path(&bbox_file)?;

        assert_eq!(file_contents, their_branch_contents);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_merge_dataframe_conflict_both_added_rows_combine_uniq(
) -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);

        // Add a more rows on this branch
        let branch_name = "ox-add-rows";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add in a line in this branch
        let row_from_branch = "train/cat_3.jpg,cat,41.0,31.5,410,427";
        let bbox_file = test::append_line_txt_file(bbox_file, row_from_branch)?;

        // Add the changes
        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

        // Add a more rows on the main branch
        command::checkout(&repo, og_branch.name).await?;

        let row_from_main = "train/dog_4.jpg,dog,52.0,62.5,256,429";
        let bbox_file = test::append_line_txt_file(bbox_file, row_from_main)?;

        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation on main branch")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        // We should have a conflict....
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Run command::checkout_theirs() and make sure their changes get kept
        command::checkout_combine(&repo, bbox_filename)?;
        let df = tabular::read_df(&bbox_file, DFOpts::empty())?;

        // This doesn't guarantee order, but let's make sure we have 7 annotations now
        assert_eq!(df.height(), 8);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_merge_dataframe_conflict_error_added_col() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);

        // Add a more columns on this branch
        let branch_name = "ox-add-column";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add in a column in this branch
        let mut opts = DFOpts::empty();
        opts.add_col = Some(String::from("random_col:unknown:str"));
        let mut df = tabular::read_df(&bbox_file, opts)?;
        println!("WRITE DF IN BRANCH {df:?}");
        tabular::write_df(&mut df, &bbox_file)?;

        // Add the changes
        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new column as an Ox on a branch.")?;

        // Add a more rows on the main branch
        command::checkout(&repo, og_branch.name).await?;

        let row_from_main = "train/dog_4.jpg,dog,52.0,62.5,256,429";
        let bbox_file = test::append_line_txt_file(bbox_file, row_from_main)?;

        command::add(&repo, bbox_file)?;
        command::commit(&repo, "Adding new row on main branch")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        // We should have a conflict....
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Run command::checkout_theirs() and make sure we cannot
        let result = command::checkout_combine(&repo, bbox_filename);
        println!("{result:?}");
        assert!(result.is_err());

        Ok(())
    })
    .await
}

#[test]
fn test_diff_tabular_add_col() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(bbox_filename);

        let mut opts = DFOpts::empty();
        // Add Column
        opts.add_col = Some(String::from("is_cute:unknown:str"));
        // Save to Output
        opts.output = Some(bbox_file.clone());
        // Perform df transform
        command::df(&bbox_file, opts)?;

        let diff = command::diff(&repo, None, &bbox_file);
        println!("{:?}", diff);

        assert!(diff.is_ok());
        let diff = diff.unwrap();
        assert_eq!(
            diff,
            r"Added Columns

shape: (6, 1)
┌─────────┐
│ is_cute │
│ ---     │
│ str     │
╞═════════╡
│ unknown │
│ unknown │
│ unknown │
│ unknown │
│ unknown │
│ unknown │
└─────────┘

"
        );

        Ok(())
    })
}

#[test]
fn test_diff_tabular_add_row() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(bbox_filename);

        let mut opts = DFOpts::empty();
        // Add Row
        opts.add_row = Some(String::from("train/cat_100.jpg,cat,100.0,100.0,100,100"));
        opts.content_type = ContentType::Csv;
        // Save to Output
        opts.output = Some(bbox_file.clone());
        // Perform df transform
        command::df(&bbox_file, opts)?;

        match command::diff(&repo, None, &bbox_file) {
            Ok(diff) => {
                println!("{diff}");

                assert_eq!(
                    diff,
                    r"Added Rows

shape: (1, 6)
┌───────────────────┬───────┬───────┬───────┬───────┬────────┐
│ file              ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
│ ---               ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
│ str               ┆ str   ┆ f64   ┆ f64   ┆ i64   ┆ i64    │
╞═══════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
│ train/cat_100.jpg ┆ cat   ┆ 100.0 ┆ 100.0 ┆ 100   ┆ 100    │
└───────────────────┴───────┴───────┴───────┴───────┴────────┘

"
                );
            }
            Err(err) => {
                panic!("Error diffing: {}", err);
            }
        }

        Ok(())
    })
}

#[test]
fn test_diff_tabular_remove_row() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(bbox_filename);

        // Remove a row
        let bbox_file = test::modify_txt_file(
            bbox_file,
            r"
file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
train/dog_2.jpg,dog,7.0,29.5,246,247
train/cat_2.jpg,cat,30.5,44.0,333,396
",
        )?;

        match command::diff(&repo, None, bbox_file) {
            Ok(diff) => {
                println!("{diff}");

                assert_eq!(
                    diff,
                    r"Removed Rows

shape: (3, 6)
┌─────────────────┬───────┬───────┬───────┬───────┬────────┐
│ file            ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
│ ---             ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
│ str             ┆ str   ┆ f64   ┆ f64   ┆ i64   ┆ i64    │
╞═════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
│ train/dog_1.jpg ┆ dog   ┆ 102.5 ┆ 31.0  ┆ 386   ┆ 330    │
│ train/dog_3.jpg ┆ dog   ┆ 19.0  ┆ 63.5  ┆ 376   ┆ 421    │
│ train/cat_1.jpg ┆ cat   ┆ 57.0  ┆ 35.5  ┆ 304   ┆ 427    │
└─────────────────┴───────┴───────┴───────┴───────┴────────┘

"
                );
            }
            Err(err) => {
                panic!("Error diffing: {}", err);
            }
        }

        Ok(())
    })
}

#[tokio::test]
async fn test_status_rm_regular_file() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        // Move the file to a new name
        let og_basename = PathBuf::from("README.md");
        let og_file = repo.path.join(&og_basename);
        std::fs::remove_file(og_file)?;

        let status = command::status(&repo)?;
        status.print_stdout();

        assert_eq!(status.removed_files.len(), 1);

        let opts = RmOpts::from_path(&og_basename);
        command::rm(&repo, &opts).await?;
        let status = command::status(&repo)?;
        status.print_stdout();

        assert_eq!(status.added_files.len(), 1);
        assert_eq!(
            status.added_files[&og_basename].status,
            StagedEntryStatus::Removed
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_status_rm_directory_file() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        // Move the file to a new name
        let og_basename = PathBuf::from("README.md");
        let og_file = repo.path.join(&og_basename);
        std::fs::remove_file(og_file)?;

        let status = command::status(&repo)?;
        status.print_stdout();

        assert_eq!(status.removed_files.len(), 1);

        let opts = RmOpts::from_path(&og_basename);
        command::rm(&repo, &opts).await?;
        let status = command::status(&repo)?;
        status.print_stdout();

        assert_eq!(status.added_files.len(), 1);
        assert_eq!(
            status.added_files[&og_basename].status,
            StagedEntryStatus::Removed
        );

        Ok(())
    })
    .await
}

/// Should be able to use `oxen rm -r` then restore to get files back
///
/// $ oxen rm -r train/
/// $ oxen restore --staged train/
/// $ oxen restore train/
#[tokio::test]
async fn test_rm_directory_restore_directory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        let rm_dir = PathBuf::from("train");
        let full_path = repo.path.join(&rm_dir);
        let num_files = util::fs::rcount_files_in_dir(&full_path);

        // Remove directory
        let opts = RmOpts {
            path: rm_dir.to_owned(),
            recursive: true,
            staged: false,
            remote: false,
        };
        command::rm(&repo, &opts).await?;

        // Make sure we staged these removals
        let status = command::status(&repo)?;
        status.print_stdout();
        assert_eq!(num_files, status.added_files.len());
        for (_path, entry) in status.added_files.iter() {
            assert_eq!(entry.status, StagedEntryStatus::Removed);
        }
        // Make sure directory is no longer on disk
        assert!(!full_path.exists());

        // Restore the content from staging area
        let opts = RestoreOpts::from_staged_path(&rm_dir);
        command::restore(&repo, opts)?;

        // This should have removed all the staged files, but not restored from disk yet.
        let status = command::status(&repo)?;
        status.print_stdout();
        assert_eq!(0, status.added_files.len());
        assert_eq!(num_files, status.removed_files.len());

        // This should restore all the files from the HEAD commit
        let opts = RestoreOpts::from_path(&rm_dir);
        command::restore(&repo, opts)?;

        let status = command::status(&repo)?;
        status.print_stdout();

        let num_restored = util::fs::rcount_files_in_dir(&full_path);
        assert_eq!(num_restored, num_files);

        Ok(())
    })
    .await
}

#[test]
fn test_oxen_ignore_file() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        // Add a file that we are going to ignore
        let ignore_filename = "ignoreme.txt";
        let ignore_path = repo.path.join(ignore_filename);
        test::write_txt_file_to_path(ignore_path, "I should be ignored")?;

        let oxenignore_file = repo.path.join(".oxenignore");
        test::write_txt_file_to_path(oxenignore_file, ignore_filename)?;

        let status = command::status(&repo)?;
        // Only untracked file should be .oxenignore
        assert_eq!(status.untracked_files.len(), 1);
        assert_eq!(
            status.untracked_files.first().unwrap(),
            Path::new(".oxenignore")
        );

        Ok(())
    })
}

#[test]
fn test_oxen_ignore_dir() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        // Add a file that we are going to ignore
        let ignore_dir = "ignoreme/";
        let ignore_path = repo.path.join(ignore_dir);
        std::fs::create_dir(&ignore_path)?;
        test::write_txt_file_to_path(ignore_path.join("0.txt"), "I should be ignored")?;
        test::write_txt_file_to_path(ignore_path.join("1.txt"), "I should also be ignored")?;

        let oxenignore_file = repo.path.join(".oxenignore");
        test::write_txt_file_to_path(oxenignore_file, "ignoreme.txt")?;

        let status = command::status(&repo)?;
        // Only untracked file should be .oxenignore
        assert_eq!(status.untracked_files.len(), 1);
        assert_eq!(
            status.untracked_files.first().unwrap(),
            Path::new(".oxenignore")
        );

        Ok(())
    })
}

#[tokio::test]
async fn test_oxen_clone_empty_repo() -> Result<(), OxenError> {
    test::run_no_commit_remote_repo_test(|remote_repo| async move {
        let ret_repo = remote_repo.clone();

        // Create a new repo to clone to, then clean it up
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = false;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;

            let status = command::status(&cloned_repo);
            assert!(status.is_ok());

            Ok(new_repo_dir)
        })
        .await?;

        Ok(ret_repo)
    })
    .await
}

#[tokio::test]
async fn test_oxen_clone_empty_repo_then_push() -> Result<(), OxenError> {
    test::run_no_commit_remote_repo_test(|remote_repo| async move {
        let ret_repo = remote_repo.clone();

        // Create a new repo to clone to, then clean it up
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let shallow = false;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: new_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;

            let status = command::status(&cloned_repo);
            assert!(status.is_ok());

            // Add a file to the cloned repo
            let new_file = "new_file.txt";
            let new_file_path = cloned_repo.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
            command::add(&cloned_repo, &new_file_path)?;
            command::commit(&cloned_repo, "Adding new file path.")?;

            command::push(&cloned_repo).await?;

            Ok(new_repo_dir)
        })
        .await?;

        Ok(ret_repo)
    })
    .await
}

#[tokio::test]
async fn test_cannot_push_two_separate_empty_roots() -> Result<(), OxenError> {
    test::run_no_commit_remote_repo_test(|remote_repo| async move {
        let ret_repo = remote_repo.clone();

        // Clone the first repo
        test::run_empty_dir_test_async(|first_repo_dir| async move {
            let shallow = false;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: first_repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let first_cloned_repo = command::clone(&opts).await?;

            // Clone the second repo
            test::run_empty_dir_test_async(|second_repo_dir| async move {
                let shallow = false;
                let opts = CloneOpts {
                    url: remote_repo.remote.url.to_string(),
                    dst: second_repo_dir.to_owned(),
                    shallow,
                    branch: DEFAULT_BRANCH_NAME.to_string(),
                };
                let second_cloned_repo = command::clone(&opts).await?;

                // Add to the first repo, after we have the second repo cloned
                let new_file = "new_file.txt";
                let new_file_path = first_cloned_repo.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                command::add(&first_cloned_repo, &new_file_path)?;
                command::commit(&first_cloned_repo, "Adding first file path.")?;
                command::push(&first_cloned_repo).await?;

                // The push to the second version of the same repo should fail
                // Adding two commits to have a longer history that also should fail
                let new_file = "new_file_2.txt";
                let new_file_path = second_cloned_repo.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
                command::add(&second_cloned_repo, &new_file_path)?;
                command::commit(&second_cloned_repo, "Adding second file path.")?;

                let new_file = "new_file_3.txt";
                let new_file_path = second_cloned_repo.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                command::add(&second_cloned_repo, &new_file_path)?;
                command::commit(&second_cloned_repo, "Adding third file path.")?;

                // Push should FAIL
                let result = command::push(&second_cloned_repo).await;
                assert!(result.is_err());

                Ok(second_repo_dir)
            })
            .await?;

            Ok(first_repo_dir)
        })
        .await?;

        Ok(ret_repo)
    })
    .await
}

// Test that we cannot push two completely separate local repos to the same history
// 1) Create repo A with data
// 2) Create repo B with data
// 3) Push Repo A
// 4) Push repo B to repo A and fail
#[tokio::test]
async fn test_cannot_push_two_separate_repos() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|mut repo_1| async move {
        test::run_training_data_repo_test_fully_committed_async(|mut repo_2| async move {
            // Add to the first repo
            let new_file = "new_file.txt";
            let new_file_path = repo_1.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
            command::add(&repo_1, &new_file_path)?;
            command::commit(&repo_1, "Adding first file path.")?;
            // Set/create the proper remote
            let remote = test::repo_remote_url_from(&repo_1.dirname());
            command::config::set_remote(&mut repo_1, constants::DEFAULT_REMOTE_NAME, &remote)?;
            test::create_remote_repo(&repo_1).await?;
            command::push(&repo_1).await?;

            // Adding two commits to have a longer history that also should fail
            let new_file = "new_file_2.txt";
            let new_file_path = repo_2.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
            command::add(&repo_2, &new_file_path)?;
            command::commit(&repo_2, "Adding second file path.")?;

            let new_file = "new_file_3.txt";
            let new_file_path = repo_2.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
            command::add(&repo_2, &new_file_path)?;
            command::commit(&repo_2, "Adding third file path.")?;

            // Set remote to the same as the first repo
            command::config::set_remote(&mut repo_2, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push should FAIL
            let result = command::push(&repo_2).await;
            assert!(result.is_err());

            Ok(())
        })
        .await?;

        Ok(())
    })
    .await
}

// Test that we cannot clone separate repos with separate histories, then push to the same history
// 1) Clone repo A with data
// 2) Clone repo B with data
// 3) Push Repo A
// 4) Push repo B to repo A and fail
#[tokio::test]
async fn test_cannot_push_two_separate_cloned_repos() -> Result<(), OxenError> {
    // Push the first repo with data
    test::run_training_data_fully_sync_remote(|_, remote_repo_1| async move {
        let remote_repo_1_copy = remote_repo_1.clone();

        // Push the second repo with data
        test::run_training_data_fully_sync_remote(|_, remote_repo_2| async move {
            let remote_repo_2_copy = remote_repo_2.clone();
            // Clone the first repo
            test::run_empty_dir_test_async(|first_repo_dir| async move {
                let shallow = false;
                let opts = CloneOpts {
                    url: remote_repo_1.remote.url.to_string(),
                    dst: first_repo_dir.to_owned(),
                    shallow,
                    branch: DEFAULT_BRANCH_NAME.to_string(),
                };
                let first_cloned_repo = command::clone(&opts).await?;

                // Clone the second repo
                test::run_empty_dir_test_async(|second_repo_dir| async move {
                    let shallow = false;
                    let opts = CloneOpts {
                        url: remote_repo_2.remote.url.to_string(),
                        dst: second_repo_dir.to_owned(),
                        shallow,
                        branch: DEFAULT_BRANCH_NAME.to_string(),
                    };
                    let mut second_cloned_repo = command::clone(&opts).await?;

                    // Add to the first repo, after we have the second repo cloned
                    let new_file = "new_file.txt";
                    let new_file_path = first_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    command::add(&first_cloned_repo, &new_file_path)?;
                    command::commit(&first_cloned_repo, "Adding first file path.")?;
                    command::push(&first_cloned_repo).await?;

                    // Reset the remote on the second repo to the first repo
                    let first_remote = test::repo_remote_url_from(&first_cloned_repo.dirname());
                    command::config::set_remote(
                        &mut second_cloned_repo,
                        constants::DEFAULT_REMOTE_NAME,
                        &first_remote,
                    )?;

                    // Adding two commits to have a longer history that also should fail
                    let new_file = "new_file_2.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
                    command::add(&second_cloned_repo, &new_file_path)?;
                    command::commit(&second_cloned_repo, "Adding second file path.")?;

                    let new_file = "new_file_3.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                    command::add(&second_cloned_repo, &new_file_path)?;
                    command::commit(&second_cloned_repo, "Adding third file path.")?;

                    // Push should FAIL
                    let result = command::push(&second_cloned_repo).await;
                    assert!(result.is_err());

                    Ok(second_repo_dir)
                })
                .await?;

                Ok(first_repo_dir)
            })
            .await?;
            Ok(remote_repo_2_copy)
        })
        .await?;

        Ok(remote_repo_1_copy)
    })
    .await
}

/*
Checks workflow:

$ oxen clone <URL>

$ oxen checkout f412d166be1bead8 # earlier commit
$ oxen checkout 55a4df7cd5d00eee # later commit

Checkout commit: 55a4df7cd5d00eee
Setting working directory to 55a4df7cd5d00eee
IO(Os { code: 2, kind: NotFound, message: "No such file or directory" })

*/
#[tokio::test]
async fn test_clone_checkout_old_commit_checkout_new_commit() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let shallow = false;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;

            let commits = api::local::commits::list(&cloned_repo)?;
            // iterate over commits in reverse order and checkout each one
            for commit in commits.iter().rev() {
                println!(
                    "TEST checking out commit: {} -> '{}'",
                    commit.id, commit.message
                );
                command::checkout(&cloned_repo, &commit.id).await?;
            }

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}

#[tokio::test]
async fn test_pull_shallow_local_status_is_err() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let cloned_repo = command::shallow_clone(&remote_repo.remote.url, &repo_dir).await?;

            let result = command::status(&cloned_repo);
            assert!(result.is_err());

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}

#[tokio::test]
async fn test_pull_shallow_local_add_is_err() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;

            let path = cloned_repo.path.join("README.md");
            util::fs::write_to_path(&path, "# Can't add this")?;

            let result = command::add(&cloned_repo, path);
            assert!(result.is_err());

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}

#[tokio::test]
async fn test_remote_stage_add_row_commit_clears_remote_status() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;

            // Remote add row
            let path = test::test_nlp_classification_csv();
            let mut opts = DFOpts::empty();
            opts.add_row = Some("I am a new row,neutral".to_string());
            opts.content_type = ContentType::Csv;
            command::remote::df(&cloned_repo, path, opts).await?;

            // Make sure it is listed as modified
            let branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
            let directory = Path::new("");
            let opts = StagedDataOpts {
                is_remote: true,
                ..Default::default()
            };
            let status = command::remote::status(&remote_repo, &branch, directory, &opts).await?;
            assert_eq!(status.added_files.len(), 1);

            // Commit it
            command::remote::commit(&cloned_repo, "Remotely committing").await?;

            // Now status should be empty
            let status = command::remote::status(&remote_repo, &branch, directory, &opts).await?;
            assert_eq!(status.added_files.len(), 0);

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}

#[tokio::test]
async fn test_remote_stage_delete_row_clears_remote_status() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let shallow = true;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;

            // Remote add row
            let path = test::test_nlp_classification_csv();
            let mut opts = DFOpts::empty();
            opts.add_row = Some("I am a new row,neutral".to_string());
            opts.content_type = ContentType::Csv;
            // Grab ID from the row we just added
            let df = command::remote::df(&cloned_repo, &path, opts).await?;
            let uuid = match df.get(0).unwrap().first().unwrap() {
                AnyValue::Utf8(s) => s.to_string(),
                _ => panic!("Expected string"),
            };

            // Make sure it is listed as modified
            let branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
            let directory = Path::new("");
            let opts = StagedDataOpts {
                is_remote: true,
                ..Default::default()
            };
            let status = command::remote::status(&remote_repo, &branch, directory, &opts).await?;
            assert_eq!(status.added_files.len(), 1);

            // Delete it
            let mut delete_opts = DFOpts::empty();
            delete_opts.delete_row = Some(uuid);
            command::remote::df(&cloned_repo, &path, delete_opts).await?;

            // Now status should be empty
            let status = command::remote::status(&remote_repo, &branch, directory, &opts).await?;
            assert_eq!(status.added_files.len(), 0);

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}

#[tokio::test]
async fn test_remote_commit_fails_if_schema_changed() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let shallow = false;
            let opts = CloneOpts {
                url: remote_repo.remote.url.to_string(),
                dst: repo_dir.to_owned(),
                shallow,
                branch: DEFAULT_BRANCH_NAME.to_string(),
            };
            let cloned_repo = command::clone(&opts).await?;

            // Remote stage row
            let path = test::test_nlp_classification_csv();
            let mut opts = DFOpts::empty();
            opts.add_row = Some("I am a new row,neutral".to_string());
            opts.content_type = ContentType::Csv;
            command::remote::df(&cloned_repo, path, opts).await?;

            // Local add col
            let full_path = cloned_repo.path.join(path);
            let mut opts = DFOpts::empty();
            opts.add_col = Some("is_something:n/a:str".to_string());
            opts.output = Some(full_path.to_path_buf()); // write back to same path
            command::df(&full_path, opts)?;
            command::add(&cloned_repo, &full_path)?;

            // Commit and push the changed schema
            command::commit(&cloned_repo, "Changed the schema 😇")?;
            command::push(&cloned_repo).await?;

            // Try to commit the remote changes, should fail
            let result = command::remote::commit(&cloned_repo, "Remotely committing").await;
            println!("{:?}", result);
            assert!(result.is_err());

            // Now status should be empty
            // let branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
            // let directory = Path::new("");
            // let opts = StagedDataOpts {
            //     is_remote: true,
            //     ..Default::default()
            // };
            // let status = command::remote_status(&remote_repo, &branch, directory, &opts).await?;
            // assert_eq!(status.modified_files.len(), 1);

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}

#[tokio::test]
async fn test_remote_ls_ten_items() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|mut repo| async move {
        // Create 8 directories
        for n in 0..8 {
            let dirname = format!("dir_{}", n);
            let dir_path = repo.path.join(dirname);
            util::fs::create_dir_all(&dir_path)?;
            let filename = "data.txt";
            let filepath = dir_path.join(filename);
            util::fs::write(&filepath, format!("Hi {}", n))?;
        }
        // Create 2 files
        let filename = "labels.txt";
        let filepath = repo.path.join(filename);
        util::fs::write(&filepath, "hello world")?;

        let filename = "README.md";
        let filepath = repo.path.join(filename);
        util::fs::write(&filepath, "readme....")?;

        // Add and commit all the dirs and files
        command::add(&repo, &repo.path)?;
        command::commit(&repo, "Adding all the data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Now list the remote
        let branch = api::local::branches::current_branch(&repo)?.unwrap();
        let dir = Path::new(".");
        let opts = PaginateOpts {
            page_num: 1,
            page_size: 10,
        };
        let paginated = command::remote::ls(&remote_repo, &branch, dir, &opts).await?;
        assert_eq!(paginated.entries.len(), 10);
        assert_eq!(paginated.page_number, 1);
        assert_eq!(paginated.page_size, 10);
        assert_eq!(paginated.total_pages, 1);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_commit_behind_main() -> Result<(), OxenError> {
    test::run_remote_repo_test_all_data_pushed(|remote_repo| async move {
        // Create branch behind-main off main
        let new_branch = "behind-main";
        let main_branch = "main";

        let main_path = "images/folder";
        let identifier = UserConfig::identifier()?;

        api::remote::branches::create_from_or_get(&remote_repo, new_branch, main_branch).await?;
        // assert_eq!(branch.name, branch_name);

        // Advance head on main branch, leave behind-main behind
        let path = test::test_jpeg_file().to_path_buf();
        let result =
            api::remote::staging::add_file(&remote_repo, main_branch, &identifier, main_path, path)
                .await;
        assert!(result.is_ok());

        let body = CommitBody {
            message: "Add to main".to_string(),
            user: User {
                name: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            },
        };

        api::remote::staging::commit_staged(&remote_repo, main_branch, &identifier, &body).await?;

        // Make an EMPTY commit to behind-main
        let body = CommitBody {
            message: "Add behind main".to_string(),
            user: User {
                name: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            },
        };
        let _commit =
            api::remote::staging::commit_staged(&remote_repo, new_branch, &identifier, &body)
                .await?;

        // Add file at images/folder to behind-main, committed to main
        let image_path = test::test_jpeg_file().to_path_buf();
        let result = api::remote::staging::add_file(
            &remote_repo,
            new_branch,
            &identifier,
            main_path,
            image_path,
        )
        .await;
        assert!(result.is_ok());

        // Check status: if valid, there should be an entry here for the file at images/folder
        let page_num = constants::DEFAULT_PAGE_NUM;
        let page_size = constants::DEFAULT_PAGE_SIZE;
        let path = Path::new("");
        let entries = api::remote::staging::status(
            &remote_repo,
            new_branch,
            &identifier,
            path,
            page_num,
            page_size,
        )
        .await?;

        assert_eq!(entries.added_files.entries.len(), 1);
        assert_eq!(entries.added_files.total_entries, 1);

        Ok(remote_repo)
    })
    .await
}
