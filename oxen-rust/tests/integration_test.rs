use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::error::OxenError;
use liboxen::index::CommitEntryReader;
use liboxen::model::StagedEntryStatus;
use liboxen::test;
use liboxen::util;

use std::path::Path;

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

        // Name and id will be random but should be populated
        assert!(!repo.id.is_empty());
        assert!(!repo.name.is_empty());

        // We make an initial parent commit and branch called "main"
        // just to make our lives easier down the line
        let orig_branch = command::current_branch(&repo)?.unwrap();
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
fn test_command_commit_nothing_staged() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        let commits = command::log(&repo)?;
        let initial_len = commits.len();
        command::commit(&repo, "Should not work")?;
        let commits = command::log(&repo)?;
        // We should not have added any commits
        assert_eq!(commits.len(), initial_len);
        Ok(())
    })
}

#[test]
fn test_command_commit_nothing_staged_but_file_modified() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let commits = command::log(&repo)?;
        let initial_len = commits.len();

        let labels_path = repo.path.join("labels.txt");
        util::fs::write_to_path(&labels_path, "changing this guy, but not committing");

        command::commit(&repo, "Should not work")?;
        let commits = command::log(&repo)?;
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
        util::fs::write_to_path(&hello_file, "Hello World");

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
        util::fs::write_to_path(&hello_file, "Hello World");

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
        util::fs::write_to_path(&hello_file, "Hello World");

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

        let commits = command::log(&repo)?;
        assert_eq!(commits.len(), 2);

        Ok(())
    })
}

#[test]
fn test_command_checkout_non_existant_commit_id() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // This shouldn't work
        let checkout_result = command::checkout(&repo, "non-existant");
        assert!(checkout_result.is_err());

        Ok(())
    })
}

#[test]
fn test_command_checkout_commit_id() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write a hello file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello");

        // Stage a hello file
        command::add(&repo, &hello_file)?;
        // Commit the hello file
        let first_commit = command::commit(&repo, "Adding hello")?;
        assert!(first_commit.is_some());

        // Write a world
        let world_file = repo.path.join("world.txt");
        util::fs::write_to_path(&world_file, "World");

        // Stage a world file
        command::add(&repo, &world_file)?;

        // Commit the world file
        let second_commit = command::commit(&repo, "Adding world")?;
        assert!(second_commit.is_some());

        // We have the world file
        assert!(world_file.exists());

        // We checkout the previous commit
        command::checkout(&repo, &first_commit.unwrap().id)?;

        // Then we do not have the world file anymore
        assert!(!world_file.exists());

        // Check status
        let status = command::status(&repo)?;
        assert!(status.is_clean());

        Ok(())
    })
}

#[test]
fn test_command_commit_dir() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Track the file
        let train_dir = repo.path.join("train");
        command::add(&repo, &train_dir)?;
        // Commit the file
        command::commit(&repo, "Adding training data")?;

        let repo_status = command::status(&repo)?;
        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 2);
        assert_eq!(repo_status.untracked_dirs.len(), 2);

        let commits = command::log(&repo)?;
        assert_eq!(commits.len(), 2);

        Ok(())
    })
}

#[test]
fn test_command_commit_dir_recursive() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Track the annotations dir, which has sub dirs
        let annotations_dir = repo.path.join("annotations");
        command::add(&repo, &annotations_dir)?;
        command::commit(&repo, "Adding annotations data dir, which has two levels")?;

        let repo_status = command::status(&repo)?;
        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 2);
        assert_eq!(repo_status.untracked_dirs.len(), 2);

        let commits = command::log(&repo)?;
        assert_eq!(commits.len(), 2);

        Ok(())
    })
}

#[test]
fn test_command_checkout_current_branch_name_does_nothing() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write the first file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello");

        // Track & commit the file
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Added hello.txt")?;

        // Create and checkout branch
        let branch_name = "feature/world-explorer";
        command::create_checkout_branch(&repo, branch_name)?;
        command::checkout(&repo, branch_name)?;

        Ok(())
    })
}

#[test]
fn test_command_checkout_added_file() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write the first file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello");

        // Track & commit the file
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Added hello.txt")?;

        // Get the original branch name
        let orig_branch = command::current_branch(&repo)?.unwrap();

        // Create and checkout branch
        let branch_name = "feature/world-explorer";
        command::create_checkout_branch(&repo, branch_name)?;

        // Write a second file
        let world_file = repo.path.join("world.txt");
        util::fs::write_to_path(&world_file, "World");

        // Track & commit the second file in the branch
        command::add(&repo, &world_file)?;
        command::commit(&repo, "Added world.txt")?;

        // Make sure we have both commits after the initial
        let commits = command::log(&repo)?;
        assert_eq!(commits.len(), 3);

        let branches = command::list_branches(&repo)?;
        assert_eq!(branches.len(), 2);

        // Make sure we have both files on disk in our repo dir
        assert!(hello_file.exists());
        assert!(world_file.exists());

        // Go back to the main branch
        command::checkout(&repo, &orig_branch.name)?;

        // The world file should no longer be there
        assert!(hello_file.exists());
        assert!(!world_file.exists());

        // Go back to the world branch
        command::checkout(&repo, branch_name)?;
        assert!(hello_file.exists());
        assert!(world_file.exists());

        Ok(())
    })
}

#[test]
fn test_command_checkout_added_file_keep_untracked() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write the first file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello");

        // Have another file lying around we will not remove
        let keep_file = repo.path.join("keep_me.txt");
        util::fs::write_to_path(&keep_file, "I am untracked, don't remove me");

        // Track & commit the file
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Added hello.txt")?;

        // Get the original branch name
        let orig_branch = command::current_branch(&repo)?.unwrap();

        // Create and checkout branch
        let branch_name = "feature/world-explorer";
        command::create_checkout_branch(&repo, branch_name)?;

        // Write a second file
        let world_file = repo.path.join("world.txt");
        util::fs::write_to_path(&world_file, "World");

        // Track & commit the second file in the branch
        command::add(&repo, &world_file)?;
        command::commit(&repo, "Added world.txt")?;

        // Make sure we have both commits after the initial
        let commits = command::log(&repo)?;
        assert_eq!(commits.len(), 3);

        let branches = command::list_branches(&repo)?;
        assert_eq!(branches.len(), 2);

        // Make sure we have all files on disk in our repo dir
        assert!(hello_file.exists());
        assert!(world_file.exists());
        assert!(keep_file.exists());

        // Go back to the main branch
        command::checkout(&repo, &orig_branch.name)?;

        // The world file should no longer be there
        assert!(hello_file.exists());
        assert!(!world_file.exists());
        assert!(keep_file.exists());

        // Go back to the world branch
        command::checkout(&repo, branch_name)?;
        assert!(hello_file.exists());
        assert!(world_file.exists());
        assert!(keep_file.exists());

        Ok(())
    })
}

#[test]
fn test_command_checkout_modified_file() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write the first file
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello");

        // Track & commit the file
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Added hello.txt")?;

        // Get the original branch name
        let orig_branch = command::current_branch(&repo)?.unwrap();

        // Create and checkout branch
        let branch_name = "feature/world-explorer";
        command::create_checkout_branch(&repo, branch_name)?;

        // Modify the file
        let hello_file = test::modify_txt_file(hello_file, "World")?;

        // Track & commit the change in the branch
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Changed file to world")?;

        // It should say World at this point
        assert_eq!(util::fs::read_from_path(&hello_file)?, "World");

        // Go back to the main branch
        command::checkout(&repo, &orig_branch.name)?;

        // The file contents should be Hello, not World
        log::debug!("HELLO FILE NAME: {:?}", hello_file);
        assert!(hello_file.exists());

        // It should be reverted back to Hello
        assert_eq!(util::fs::read_from_path(&hello_file)?, "Hello");

        Ok(())
    })
}

#[test]
fn test_command_add_modified_file_in_subdirectory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        // Modify and add the file deep in a sub dir
        let one_shot_path = repo.path.join("annotations/train/one_shot.txt");
        let file_contents = "train/cat_1.jpg 0";
        test::modify_txt_file(one_shot_path, file_contents)?;
        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 1);
        // Add the top level directory, and make sure the modified file gets added
        let annotation_dir_path = repo.path.join("annotations");
        command::add(&repo, &annotation_dir_path)?;
        let status = command::status(&repo)?;
        status.print();
        assert_eq!(status.added_files.len(), 1);
        command::commit(&repo, "Changing one shot")?;
        let status = command::status(&repo)?;
        assert!(status.is_clean());

        Ok(())
    })
}

#[test]
fn test_command_checkout_modified_file_in_subdirectory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Get the original branch name
        let orig_branch = command::current_branch(&repo)?.unwrap();

        // Track & commit the file
        let one_shot_path = repo.path.join("annotations/train/one_shot.txt");
        command::add(&repo, &one_shot_path)?;
        command::commit(&repo, "Adding one shot")?;

        // Get OG file contents
        let og_content = util::fs::read_from_path(&one_shot_path)?;

        let branch_name = "feature/change-the-shot";
        command::create_checkout_branch(&repo, branch_name)?;

        let file_contents = "train/cat_1.jpg 0";
        let one_shot_path = test::modify_txt_file(one_shot_path, file_contents)?;
        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 1);
        status.print();
        command::add(&repo, &one_shot_path)?;
        let status = command::status(&repo)?;
        status.print();
        command::commit(&repo, "Changing one shot")?;

        // checkout OG and make sure it reverts
        command::checkout(&repo, &orig_branch.name)?;
        let updated_content = util::fs::read_from_path(&one_shot_path)?;
        assert_eq!(og_content, updated_content);

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name)?;
        let updated_content = util::fs::read_from_path(&one_shot_path)?;
        assert_eq!(file_contents, updated_content);

        Ok(())
    })
}

#[test]
fn test_command_checkout_modified_file_from_fully_committed_repo() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Get the original branch name
        let orig_branch = command::current_branch(&repo)?.unwrap();

        // Track & commit all the data
        let one_shot_path = repo.path.join("annotations/train/one_shot.txt");
        command::add(&repo, &repo.path)?;
        command::commit(&repo, "Adding one shot")?;

        // Get OG file contents
        let og_content = util::fs::read_from_path(&one_shot_path)?;

        let branch_name = "feature/modify-data";
        command::create_checkout_branch(&repo, branch_name)?;

        let file_contents = "train/cat_1.jpg 0";
        let one_shot_path = test::modify_txt_file(one_shot_path, file_contents)?;
        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 1);
        command::add(&repo, &one_shot_path)?;
        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 0);
        assert_eq!(status.added_files.len(), 1);

        let status = command::status(&repo)?;
        status.print();
        command::commit(&repo, "Changing one shot")?;

        // checkout OG and make sure it reverts
        command::checkout(&repo, &orig_branch.name)?;
        let updated_content = util::fs::read_from_path(&one_shot_path)?;
        assert_eq!(og_content, updated_content);

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name)?;
        let updated_content = util::fs::read_from_path(&one_shot_path)?;
        assert_eq!(file_contents, updated_content);

        Ok(())
    })
}

#[test]
fn test_command_commit_top_level_dir_then_revert() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Get the original branch name
        let orig_branch = command::current_branch(&repo)?.unwrap();

        // Create a branch to make the changes
        let branch_name = "feature/adding-train";
        command::create_checkout_branch(&repo, branch_name)?;

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
        command::checkout(&repo, &orig_branch.name)?;
        assert!(!train_path.exists());

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name)?;
        assert!(train_path.exists());
        assert_eq!(util::fs::rcount_files_in_dir(&train_path), og_num_files);

        Ok(())
    })
}

#[test]
fn test_command_add_second_level_dir_then_revert() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Get the original branch name
        let orig_branch = command::current_branch(&repo)?.unwrap();

        // Create a branch to make the changes
        let branch_name = "feature/adding-annotations";
        command::create_checkout_branch(&repo, branch_name)?;

        // Track & commit (dir already created in helper)
        let new_dir_path = repo.path.join("annotations").join("train");
        let og_num_files = util::fs::rcount_files_in_dir(&new_dir_path);

        command::add(&repo, &new_dir_path)?;
        command::commit(&repo, "Adding train dir")?;

        // checkout OG and make sure it removes the train dir
        command::checkout(&repo, &orig_branch.name)?;
        assert!(!new_dir_path.exists());

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name)?;
        assert!(new_dir_path.exists());
        assert_eq!(util::fs::rcount_files_in_dir(&new_dir_path), og_num_files);

        Ok(())
    })
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

#[test]
fn test_command_restore_removed_file_from_branch_with_commits_between() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // (file already created in helper)
        let file_to_remove = repo.path.join("labels.txt");

        let orig_branch = command::current_branch(&repo)?.unwrap();

        // Commit the file
        command::add(&repo, &file_to_remove)?;
        command::commit(&repo, "Adding labels file")?;

        let train_dir = repo.path.join("train");
        command::add(&repo, &train_dir)?;
        command::commit(&repo, "Adding train dir")?;

        // Branch
        command::create_checkout_branch(&repo, "remove-labels")?;

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
        command::checkout(&repo, &orig_branch.name)?;
        // Make sure we restore file
        assert!(file_to_remove.exists());

        Ok(())
    })
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
        assert_eq!(status.added_files[0].1.status, StagedEntryStatus::Removed);

        // Make sure they don't show up in the status
        assert_eq!(status.removed_files.len(), 0);

        Ok(())
    })
}

#[test]
fn test_command_remove_dir_then_revert() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Get the original branch name
        let orig_branch = command::current_branch(&repo)?.unwrap();

        // (dir already created in helper)
        let dir_to_remove = repo.path.join("train");
        let og_num_files = util::fs::rcount_files_in_dir(&dir_to_remove);

        // track the dir
        command::add(&repo, &dir_to_remove)?;
        command::commit(&repo, "Adding train dir")?;

        // Create a branch to make the changes
        let branch_name = "feature/removing-train";
        command::create_checkout_branch(&repo, branch_name)?;

        // Delete the directory from disk
        std::fs::remove_dir_all(&dir_to_remove)?;

        // Track the deletion
        command::add(&repo, &dir_to_remove)?;
        command::commit(&repo, "Removing train dir")?;

        // checkout OG and make sure it restores the train dir
        command::checkout(&repo, &orig_branch.name)?;
        assert!(dir_to_remove.exists());
        assert_eq!(util::fs::rcount_files_in_dir(&dir_to_remove), og_num_files);

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name)?;
        assert!(!dir_to_remove.exists());

        Ok(())
    })
}

#[test]
fn test_command_push_one_commit() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let mut repo = repo;

        // Track the file
        let train_dir = repo.path.join("train");
        let num_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dir
        let commit = command::commit(&repo, "Adding training data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push it real good
        command::push(&repo)?;

        let page_num = 1;
        let page_size = num_files;
        let entries = api::remote::entries::list_page(&repo, &commit.id, page_num, page_size)?;
        assert_eq!(entries.total_entries, num_files);
        assert_eq!(entries.entries.len(), num_files);

        Ok(())
    })
}

#[test]
fn test_command_push_inbetween_two_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let mut repo = repo;
        // Track the train dir
        let train_dir = repo.path.join("train");
        let mut num_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dur
        command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push the files
        command::push(&repo)?;

        // Track the test dir
        let test_dir = repo.path.join("test");
        num_files += util::fs::rcount_files_in_dir(&test_dir);
        command::add(&repo, &test_dir)?;
        let commit = command::commit(&repo, "Adding test data")?.unwrap();

        // Push the files
        command::push(&repo)?;

        let page_num = 1;
        let page_size = num_files;
        let entries = api::remote::entries::list_page(&repo, &commit.id, page_num, page_size)?;
        assert_eq!(entries.total_entries, num_files);
        assert_eq!(entries.entries.len(), num_files);

        Ok(())
    })
}

#[test]
fn test_command_push_after_two_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Make mutable copy so we can set remote
        let mut repo = repo;

        // Track the train dir
        let train_dir = repo.path.join("train");
        let mut num_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dur
        command::commit(&repo, "Adding training data")?;

        // Track the test dir
        let test_dir = repo.path.join("test");
        num_files += util::fs::rcount_files_in_dir(&test_dir);
        command::add(&repo, &test_dir)?;
        let commit = command::commit(&repo, "Adding test data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push the files
        command::push(&repo)?;

        let page_num = 1;
        let page_size = num_files;
        let entries = api::remote::entries::list_page(&repo, &commit.id, page_num, page_size)?;
        assert_eq!(entries.total_entries, num_files);
        assert_eq!(entries.entries.len(), num_files);

        Ok(())
    })
}

// This broke when you tried to add the "." directory to add everything, after already committing the train directory.
#[test]
fn test_command_push_after_two_commits_adding_dot() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Make mutable copy so we can set remote
        let mut repo = repo;

        // Track the train dir
        let train_dir = repo.path.join("train");

        command::add(&repo, &train_dir)?;
        // Commit the train dur
        command::commit(&repo, "Adding training data")?;

        // Track the rest of the files
        let full_dir = &repo.path;
        let num_files = util::fs::rcount_files_in_dir(full_dir);
        command::add(&repo, full_dir)?;
        let commit = command::commit(&repo, "Adding rest of data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push the files
        command::push(&repo)?;

        let page_num = 1;
        let page_size = num_files;
        let entries = api::remote::entries::list_page(&repo, &commit.id, page_num, page_size)?;
        assert_eq!(entries.total_entries, num_files);
        assert_eq!(entries.entries.len(), num_files);

        Ok(())
    })
}

#[test]
fn test_cannot_push_if_remote_not_set() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Track the file
        let train_dirname = "train";
        let train_dir = repo.path.join(train_dirname);
        command::add(&repo, &train_dir)?;
        // Commit the train dir
        command::commit(&repo, "Adding training data")?.unwrap();

        // Should not be able to push
        let result = command::push(&repo);
        assert!(result.is_err());
        Ok(())
    })
}

#[test]
fn test_command_push_clone_pull_push() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|mut repo| {
        // Track the file
        let train_dirname = "train";
        let train_dir = repo.path.join(train_dirname);
        let og_num_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dir
        command::commit(&repo, "Adding training data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push it real good
        let remote_repo = command::push(&repo)?;

        // Add a new file
        let party_ppl_filename = "party_ppl.txt";
        let party_ppl_contents = String::from("Wassup Party Ppl");
        let party_ppl_file_path = repo.path.join(party_ppl_filename);
        util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents);

        // Add and commit and push
        command::add(&repo, &party_ppl_file_path)?;
        let latest_commit = command::commit(&repo, "Adding party_ppl.txt")?.unwrap();
        command::push(&repo)?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test(|new_repo_dir| {
            let cloned_repo = command::clone(&remote_repo.url, new_repo_dir)?;
            let oxen_dir = cloned_repo.path.join(".oxen");
            assert!(oxen_dir.exists());
            command::pull(&cloned_repo)?;

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
            let head = command::head_commit(&cloned_repo)?;
            assert_eq!(head.id, latest_commit.id);

            // Make sure we synced all the commits
            let repo_commits = command::log(&repo)?;
            let cloned_commits = command::log(&cloned_repo)?;
            assert_eq!(repo_commits.len(), cloned_commits.len());

            // Make sure we updated the dbs properly
            let status = command::status(&cloned_repo)?;
            assert!(status.is_clean());

            // Have this side add a file, and send it back over
            let send_it_back_filename = "send_it_back.txt";
            let send_it_back_contents = String::from("Hello from the other side");
            let send_it_back_file_path = cloned_repo.path.join(send_it_back_filename);
            util::fs::write_to_path(&send_it_back_file_path, &send_it_back_contents);

            // Add and commit and push
            command::add(&cloned_repo, &send_it_back_file_path)?;
            command::commit(&cloned_repo, "Adding send_it_back.txt")?;
            command::push(&cloned_repo)?;

            // Pull back from the OG Repo
            command::pull(&repo)?;
            let old_repo_status = command::status(&repo)?;
            old_repo_status.print();
            // Make sure we don't modify the timestamps or anything of the OG data
            assert!(old_repo_status.is_clean());

            let pulled_send_it_back_path = repo.path.join(send_it_back_filename);
            assert!(pulled_send_it_back_path.exists());
            let pulled_contents = util::fs::read_from_path(&pulled_send_it_back_path)?;
            assert_eq!(pulled_contents, send_it_back_contents);

            // Modify the party ppl contents
            let party_ppl_contents = String::from("Late to the party");
            util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents);
            command::add(&repo, &party_ppl_file_path)?;
            command::commit(&repo, "Modified party ppl contents")?;
            command::push(&repo)?;

            // Pull the modifications
            command::pull(&cloned_repo)?;
            let pulled_contents = util::fs::read_from_path(&cloned_party_ppl_path)?;
            assert_eq!(pulled_contents, party_ppl_contents);

            // Remove a file, add, commit, push the change
            std::fs::remove_file(&send_it_back_file_path)?;
            command::add(&cloned_repo, &send_it_back_file_path)?;
            command::commit(&cloned_repo, "Removing the send it back file")?;
            command::push(&cloned_repo)?;

            // Pull down the changes and make sure the file is removed
            command::pull(&repo)?;
            let pulled_send_it_back_path = repo.path.join(send_it_back_filename);
            assert!(!pulled_send_it_back_path.exists());

            Ok(())
        })
    })
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
#[test]
fn test_command_add_modify_remove_push_pull() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|mut repo| {
        // Track a file
        let filename = "labels.txt";
        let filepath = repo.path.join(filename);
        command::add(&repo, &filepath)?;
        command::commit(&repo, "Adding labels file")?.unwrap();

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push it real good
        let remote_repo = command::push(&repo)?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test(|new_repo_dir| {
            let cloned_repo = command::clone(&remote_repo.url, new_repo_dir)?;
            command::pull(&cloned_repo)?;

            // Modify the file in the cloned dir
            let cloned_filepath = cloned_repo.path.join(filename);
            let changed_content = "messing up the labels";
            util::fs::write_to_path(&cloned_filepath, changed_content);
            command::add(&cloned_repo, &cloned_filepath)?;
            command::commit(&cloned_repo, "I messed with the label file")?.unwrap();

            // Push back to server
            command::push(&cloned_repo)?;

            // Pull back to original guy
            command::pull(&repo)?;

            // Make sure content changed
            let pulled_content = util::fs::read_from_path(&filepath)?;
            assert_eq!(pulled_content, changed_content);

            // Delete the file in the og filepath
            std::fs::remove_file(&filepath)?;

            // Stage & Commit & Push the removal
            command::add(&repo, &filepath)?;
            command::commit(&repo, "You mess with it, I remove it")?.unwrap();
            command::push(&repo)?;

            command::pull(&cloned_repo)?;
            assert!(!cloned_filepath.exists());

            Ok(())
        })
    })
}

#[test]
fn test_pull_multiple_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|mut repo| {
        // Track a file
        let filename = "labels.txt";
        let file_path = repo.path.join(filename);
        command::add(&repo, &file_path)?;
        command::commit(&repo, "Adding labels file")?.unwrap();

        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?.unwrap();

        let test_path = repo.path.join("test");
        command::add(&repo, &test_path)?;
        command::commit(&repo, "Adding test dir")?.unwrap();

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push it
        let remote_repo = command::push(&repo)?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test(|new_repo_dir| {
            let cloned_repo = command::clone(&remote_repo.url, new_repo_dir)?;
            command::pull(&cloned_repo)?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // 2 test, 5 train, 1 labels
            assert_eq!(8, cloned_num_files);

            Ok(())
        })
    })
}

// Make sure we can push again after pulling on the other side, then pull again
#[test]
fn test_push_pull_push_pull_on_branch() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|mut repo| {
        // Track a dir
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?.unwrap();

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push it
        let remote_repo = command::push(&repo)?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test(|new_repo_dir| {
            let cloned_repo = command::clone(&remote_repo.url, new_repo_dir)?;
            command::pull(&cloned_repo)?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // 5 training files
            assert_eq!(5, cloned_num_files);
            let og_commits = command::log(&repo)?;
            let cloned_commits = command::log(&cloned_repo)?;
            assert_eq!(og_commits.len(), cloned_commits.len());

            // Create a branch to collab on
            let branch_name = "adding-training-data";
            command::create_checkout_branch(&cloned_repo, branch_name)?;

            // Track some more data in the cloned repo
            let hotdog_path = Path::new("data/test/images/hotdog_1.jpg");
            let new_file_path = cloned_repo.path.join("train").join("hotdog_1.jpg");
            std::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&cloned_repo, &new_file_path)?;
            command::commit(&cloned_repo, "Adding one file to train dir")?.unwrap();

            // Push it back
            command::push_remote_branch(&cloned_repo, constants::DEFAULT_REMOTE_NAME, branch_name)?;

            // Pull it on the OG side
            command::pull_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name)?;
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);
            // Now there should be 6 train files
            assert_eq!(6, og_num_files);

            // Add another file on the OG side, and push it back
            let hotdog_path = Path::new("data/test/images/hotdog_2.jpg");
            let new_file_path = train_path.join("hotdog_2.jpg");
            std::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&repo, &train_path)?;
            let commit = command::commit(&repo, "Adding next file to train dir")?.unwrap();
            println!("========== AFTER COMMIT {:?}", commit);
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name)?;
            println!("========== AFTER PUSH REMOTE BRANCH {:?}", commit);

            // Pull it on the second side again
            command::pull_remote_branch(&cloned_repo, constants::DEFAULT_REMOTE_NAME, branch_name)?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // Now there should be 7 train files
            assert_eq!(7, cloned_num_files);

            Ok(())
        })
    })
}

// Make sure we can push again after pulling on the other side, then pull again
#[test]
fn test_push_pull_push_pull_on_other_branch() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|mut repo| {
        // Track a dir
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?.unwrap();

        let og_branch = command::current_branch(&repo)?.unwrap();

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push it
        let remote_repo = command::push(&repo)?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test(|new_repo_dir| {
            let cloned_repo = command::clone(&remote_repo.url, new_repo_dir)?;
            command::pull(&cloned_repo)?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // 5 training files
            assert_eq!(5, cloned_num_files);

            // Create a branch to collab on
            let branch_name = "adding-training-data";
            command::create_checkout_branch(&cloned_repo, branch_name)?;

            // Track some more data in the cloned repo
            let hotdog_path = Path::new("data/test/images/hotdog_1.jpg");
            let new_file_path = cloned_repo.path.join("train").join("hotdog_1.jpg");
            std::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&cloned_repo, &new_file_path)?;
            command::commit(&cloned_repo, "Adding one file to train dir")?.unwrap();

            // Push it back
            command::push_remote_branch(&cloned_repo, constants::DEFAULT_REMOTE_NAME, branch_name)?;

            // Pull it on the OG side
            command::pull_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, &og_branch.name)?;
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);
            // Now there should be still be 5 train files
            assert_eq!(5, og_num_files);

            Ok(())
        })
    })
}

#[test]
fn test_only_store_changes_in_version_dir() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Track a file
        let filename = "labels.txt";
        let filepath = repo.path.join(filename);
        command::add(&repo, &filepath)?;
        command::commit(&repo, "Adding labels file")?.unwrap();

        let new_filename = "new.txt";
        let new_filepath = repo.path.join(new_filename);
        util::fs::write_to_path(&new_filepath, "hallo");
        command::add(&repo, &new_filepath)?;
        command::commit(&repo, "Adding a new file")?.unwrap();

        let version_dir =
            util::fs::oxen_hidden_dir(&repo.path).join(Path::new(constants::VERSIONS_DIR));
        log::debug!("version_dir hash_filename: {:?}", filepath);

        let id = util::hasher::hash_filename(Path::new(filename));
        let original_file_version_dir = version_dir.join(id);
        log::debug!("version dir: {:?}", original_file_version_dir);
        let num_files = util::fs::rcount_files_in_dir(&original_file_version_dir);
        assert_eq!(num_files, 1);

        Ok(())
    })
}

#[test]
fn test_we_pull_full_commit_history() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|mut repo| {
        // First commit
        let filename = "labels.txt";
        let filepath = repo.path.join(filename);
        command::add(&repo, &filepath)?;
        command::commit(&repo, "Adding labels file")?.unwrap();

        // Second commit
        let new_filename = "new.txt";
        let new_filepath = repo.path.join(new_filename);
        util::fs::write_to_path(&new_filepath, "hallo");
        command::add(&repo, &new_filepath)?;
        command::commit(&repo, "Adding a new file")?.unwrap();

        // Third commit
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?.unwrap();

        // Fourth commit
        let test_path = repo.path.join("test");
        command::add(&repo, &test_path)?;
        command::commit(&repo, "Adding test dir")?.unwrap();

        // Get local history
        let local_history = command::log(&repo)?;

        // Set the proper remote
        let remote = test::repo_url_from(&repo.name);
        command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Push it
        let remote_repo = command::push(&repo)?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test(|new_repo_dir| {
            let cloned_repo = command::clone(&remote_repo.url, new_repo_dir)?;
            command::pull(&cloned_repo)?;

            // Get cloned history
            let cloned_history = command::log(&cloned_repo)?;

            // Make sure the histories match
            assert_eq!(local_history.len(), cloned_history.len());

            // Make sure we have grabbed all the history dirs
            let hidden_dir = util::fs::oxen_hidden_dir(&cloned_repo.path);
            let history_dir = hidden_dir.join(Path::new(constants::HISTORY_DIR));
            for commit in cloned_history.iter() {
                let commit_history_dir = history_dir.join(&commit.id);
                assert!(commit_history_dir.exists());

                // make sure we can successfully open the db and read entries
                let reader = CommitEntryReader::new(&cloned_repo, commit)?;
                let entries = reader.list_entries();
                assert!(entries.is_ok());
            }

            Ok(())
        })
    })
}

#[test]
fn test_do_not_commit_any_files_on_init() -> Result<(), OxenError> {
    test::run_empty_dir_test(|dir| {
        test::populate_dir_with_training_data(dir)?;

        let repo = command::init(dir)?;
        let commits = command::log(&repo)?;
        let commit = commits.last().unwrap();
        let reader = CommitEntryReader::new(&repo, commit)?;
        let num_entries = reader.num_entries()?;
        assert_eq!(num_entries, 0);

        Ok(())
    })
}

#[test]
fn test_merge_conflict_shows_in_status() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        let og_branch = command::current_branch(&repo)?.unwrap();

        // Add a "none" category on a branch
        let branch_name = "change-labels";
        command::create_checkout_branch(&repo, branch_name)?;

        test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding none category")?;

        // Add a "person" category on a the main branch
        command::checkout(&repo, &og_branch.name)?;

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
}

#[test]
fn test_can_add_merge_conflict() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        let og_branch = command::current_branch(&repo)?.unwrap();

        // Add a "none" category on a branch
        let branch_name = "change-labels";
        command::create_checkout_branch(&repo, branch_name)?;

        test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding none category")?;

        // Add a "person" category on a the main branch
        command::checkout(&repo, &og_branch.name)?;

        test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding person category")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Assume that we fixed the conflict and added the file
        let path = status.merge_conflicts[0].head_entry.path.clone();
        let fullpath = repo.path.join(path);
        command::add(&repo, fullpath)?;

        // Adding should add to added files
        let status = command::status(&repo)?;

        assert_eq!(status.added_files.len(), 1);

        // Adding should get rid of the merge conflict
        assert_eq!(status.merge_conflicts.len(), 0);

        Ok(())
    })
}

#[test]
fn test_commit_after_merge_conflict() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        let og_branch = command::current_branch(&repo)?.unwrap();

        // Add a "none" category on a branch
        let branch_name = "change-labels";
        command::create_checkout_branch(&repo, branch_name)?;

        test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding none category")?;

        // Add a "person" category on a the main branch
        command::checkout(&repo, &og_branch.name)?;

        test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding person category")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Assume that we fixed the conflict and added the file
        let path = status.merge_conflicts[0].head_entry.path.clone();
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
        let history = command::log(&repo)?;
        assert_eq!(history.len(), 5);

        Ok(())
    })
}

// Thought exercise - merge "branch" instead of merge commit, because you will want to do one more experiment,
// then fast forward to that branch
