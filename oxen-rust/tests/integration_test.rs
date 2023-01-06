use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::df::tabular;
use liboxen::df::DFOpts;
use liboxen::error::OxenError;
use liboxen::index::CommitDirReader;
use liboxen::model::StagedEntryStatus;
use liboxen::opts::RestoreOpts;
use liboxen::test;
use liboxen::util;

use futures::future;
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
fn test_command_restore_removed_file_from_head() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write to file
        let hello_filename = "hello.txt";
        let hello_file = repo.path.join(hello_filename);
        util::fs::write_to_path(&hello_file, "Hello World");

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
        util::fs::write_to_path(&hello_file, "Hello World");

        // Track the file
        command::add(&repo, &hello_file)?;
        // Commit the file
        command::commit(&repo, "My message")?;

        // Modify the file once
        let first_modification = "Hola Mundo";
        let hello_file = test::modify_txt_file(hello_file, first_modification)?;
        command::add(&repo, &hello_file)?;
        let first_mod_commit = command::commit(&repo, "Changing to spanish")?.unwrap();

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
        command::checkout(&repo, first_commit.unwrap().id)?;

        // // Then we do not have the world file anymore
        assert!(!world_file.exists());

        // // Check status
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
        command::add(&repo, train_dir)?;
        // Commit the file
        command::commit(&repo, "Adding training data")?;

        let repo_status = command::status(&repo)?;
        repo_status.print_stdout();
        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 2);
        assert_eq!(repo_status.untracked_dirs.len(), 4);

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
        command::add(&repo, annotations_dir)?;
        command::commit(&repo, "Adding annotations data dir, which has two levels")?;

        let repo_status = command::status(&repo)?;
        repo_status.print_stdout();

        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 2);
        assert_eq!(repo_status.untracked_dirs.len(), 4);

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
        command::checkout(&repo, orig_branch.name)?;

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
        command::checkout(&repo, orig_branch.name)?;

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
        command::checkout(&repo, orig_branch.name)?;

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

#[test]
fn test_command_checkout_modified_file_in_subdirectory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Get the original branch name
        let orig_branch = command::current_branch(&repo)?.unwrap();

        // Track & commit the file
        let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
        command::add(&repo, &one_shot_path)?;
        command::commit(&repo, "Adding one shot")?;

        // Get OG file contents
        let og_content = util::fs::read_from_path(&one_shot_path)?;

        let branch_name = "feature/change-the-shot";
        command::create_checkout_branch(&repo, branch_name)?;

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
        command::checkout(&repo, orig_branch.name)?;
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
        let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
        command::add(&repo, &repo.path)?;
        command::commit(&repo, "Adding one shot")?;

        // Get OG file contents
        let og_content = util::fs::read_from_path(&one_shot_path)?;

        let branch_name = "feature/modify-data";
        command::create_checkout_branch(&repo, branch_name)?;

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
        command::checkout(&repo, orig_branch.name)?;
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
        command::checkout(&repo, orig_branch.name)?;
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
        command::checkout(&repo, orig_branch.name)?;
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
        command::add(&repo, train_dir)?;
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
        command::checkout(&repo, orig_branch.name)?;
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
        assert_eq!(
            status.added_files.iter().next().unwrap().1.status,
            StagedEntryStatus::Removed
        );

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
        command::checkout(&repo, orig_branch.name)?;
        assert!(dir_to_remove.exists());
        assert_eq!(util::fs::rcount_files_in_dir(&dir_to_remove), og_num_files);

        // checkout branch again and make sure it reverts
        command::checkout(&repo, branch_name)?;
        assert!(!dir_to_remove.exists());

        Ok(())
    })
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
        let commit = command::commit(&repo, "Adding training data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

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
        let mut num_files = util::fs::rcount_files_in_dir(&train_dir);
        num_files += util::fs::rcount_files_in_dir(&annotations_dir);
        command::add(&repo, &train_dir)?;
        command::add(&repo, &annotations_dir)?;
        // Commit the train dir
        let commit = command::commit(&repo, "Adding training data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Sleep so it can unpack...
        std::thread::sleep(std::time::Duration::from_secs(2));

        let is_synced =
            api::remote::commits::commit_is_synced(&remote_repo, &commit.id, num_files).await?;
        assert!(is_synced);

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
        command::commit(&repo, "Adding training data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

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
        let commit = command::commit(&repo, "adding the rest of the annotations")?.unwrap();
        let commit_reader = CommitDirReader::new(&repo, &commit)?;
        let num_entries = commit_reader.num_entries()?;

        // Push again
        command::push(&repo).await?;

        let is_synced =
            api::remote::commits::commit_is_synced(&remote_repo, &commit.id, num_entries).await?;
        assert!(is_synced);

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
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push the files
        command::push(&repo).await?;

        // Track the test dir
        let test_dir = repo.path.join("test");
        let num_test_files = util::fs::count_files_in_dir(&test_dir);
        command::add(&repo, &test_dir)?;
        let commit = command::commit(&repo, "Adding test data")?.unwrap();

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
        let commit = command::commit(&repo, "Adding test data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

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
        let commit = command::commit(&repo, "Adding rest of data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

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
        command::commit(&repo, "Adding training data")?.unwrap();

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
        command::commit(&repo, "Adding training data")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Add a new file
        let party_ppl_filename = "party_ppl.txt";
        let party_ppl_contents = String::from("Wassup Party Ppl");
        let party_ppl_file_path = repo.path.join(party_ppl_filename);
        util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents);

        // Add and commit and push
        command::add(&repo, &party_ppl_file_path)?;
        let latest_commit = command::commit(&repo, "Adding party_ppl.txt")?.unwrap();
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
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
            util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents);
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
        command::commit(&repo, "Adding labels file")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
            command::pull(&cloned_repo).await?;

            // Modify the file in the cloned dir
            let cloned_filepath = cloned_repo.path.join(filename);
            let changed_content = "messing up the labels";
            util::fs::write_to_path(&cloned_filepath, changed_content);
            command::add(&cloned_repo, &cloned_filepath)?;
            command::commit(&cloned_repo, "I messed with the label file")?.unwrap();

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
            command::commit(&repo, "You mess with it, I remove it")?.unwrap();
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
        command::commit(&repo, "Adding labels file")?.unwrap();

        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?.unwrap();

        let test_path = repo.path.join("test");
        command::add(&repo, &test_path)?;
        command::commit(&repo, "Adding test dir")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
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
async fn test_pull_data_frame() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a file
        let filename = "annotations/train/bounding_box.csv";
        let file_path = repo.path.join(filename);
        let og_df = tabular::read_df(&file_path, DFOpts::empty())?;
        let og_contents = util::fs::read_from_path(&file_path)?;

        command::add(&repo, &file_path)?;
        command::commit(&repo, "Adding bounding box file")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
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

            // // Make sure that CADF gets reconstructed
            // let schemas = command::schema_list(&repo, None)?;
            // let schema = schemas.first().unwrap();
            // let cadf_file = util::fs::schema_df_path(&repo, schema);
            // assert!(cadf_file.exists());
            // let cadf = tabular::read_df(&cadf_file, DFOpts::empty())?;
            // assert_eq!(cadf.height(), cloned_df.height());

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

// Test that we pull down the proper data frames
// #[tokio::test]
// async fn test_pull_multiple_data_frames_multiple_schemas() -> Result<(), OxenError> {
//     test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
//         let filename = "nlp/classification/annotations/train.tsv";
//         let file_path = repo.path.join(filename);
//         let og_df = tabular::read_df(&file_path, DFOpts::empty())?;
//         let og_sentiment_contents = util::fs::read_from_path(&file_path)?;

//         let schemas = command::schema_list(&repo, None)?;
//         let sentiment_schema = schemas
//             .iter()
//             .find(|s| s.name == Some("text_classification".to_string()))
//             .unwrap();
//         let og_sentiment_cadf_path = util::fs::schema_df_path(&repo, sentiment_schema);
//         let og_sentiment_cadf = tabular::read_df(og_sentiment_cadf_path, DFOpts::empty())?;

//         // Set the proper remote
//         let remote = test::repo_remote_url_from(&repo.dirname());
//         command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

//         // Create Remote
//         let remote_repo = test::create_remote_repo(&repo).await?;

//         // Push it
//         command::push(&repo).await?;

//         // run another test with a new repo dir that we are going to sync to
//         test::run_empty_dir_test_async(|new_repo_dir| async move {
//             let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
//             command::pull(&cloned_repo).await?;

//             let filename = "nlp/classification/annotations/train.tsv";
//             let file_path = cloned_repo.path.join(filename);
//             let cloned_df = tabular::read_df(&file_path, DFOpts::empty())?;
//             let cloned_contents = util::fs::read_from_path(&file_path)?;
//             assert_eq!(og_df.height(), cloned_df.height());
//             assert_eq!(og_df.width(), cloned_df.width());
//             assert_eq!(cloned_contents, og_sentiment_contents);
//             println!("Cloned {:?} {}", filename, cloned_df);

//             // Status should be empty too
//             let status = command::status(&cloned_repo)?;
//             status.print_stdout();
//             assert!(status.is_clean());

//             // Make sure that CADF gets reconstructed
//             let new_sentiment_cadf_path = util::fs::schema_df_path(&cloned_repo, sentiment_schema);
//             let new_sentiment_cadf = tabular::read_df(new_sentiment_cadf_path, DFOpts::empty())?;

//             println!("OG Sentiment CADF {}", og_sentiment_cadf);
//             println!("Cloned Sentiment CADF {}", new_sentiment_cadf);

//             assert_eq!(og_sentiment_cadf.height(), new_sentiment_cadf.height());
//             assert_eq!(og_sentiment_cadf.width(), new_sentiment_cadf.width());

//             api::remote::repositories::delete(&remote_repo).await?;

//             Ok(new_repo_dir)
//         })
//         .await
//     })
//     .await
// }

// Make sure we can push again after pulling on the other side, then pull again
#[tokio::test]
async fn test_push_pull_push_pull_on_branch() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a dir
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?.unwrap();

        // Track larger files
        let larger_dir = repo.path.join("large_files");
        command::add(&repo, &larger_dir)?;
        command::commit(&repo, "Adding larger files")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
            command::pull(&cloned_repo).await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            assert_eq!(6, cloned_num_files);
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
            std::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&repo, &train_path)?;
            command::commit(&repo, "Adding next file to train dir")?.unwrap();
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
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a dir
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?.unwrap();

        let og_branch = command::current_branch(&repo)?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
            command::pull(&cloned_repo).await?;
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
            command::push_remote_branch(&cloned_repo, constants::DEFAULT_REMOTE_NAME, branch_name)
                .await?;

            // Pull it on the OG side
            command::pull_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, &og_branch.name)
                .await?;
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);
            // Now there should be still be 5 train files
            assert_eq!(5, og_num_files);

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
        command::commit(&repo, "Adding train dir")?.unwrap();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        let new_branch_name = "my-branch";
        command::create_checkout_branch(&repo, new_branch_name)?;

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
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // Create new branch
        let new_branch_name = "my-branch";
        command::create_checkout_branch(&repo, new_branch_name)?;

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
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

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
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
            command::pull(&cloned_repo).await?;

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
        let commits = command::log(&repo)?;
        let commit = commits.last().unwrap();
        let reader = CommitDirReader::new(&repo, commit)?;
        let num_entries = reader.num_entries()?;
        assert_eq!(num_entries, 0);

        Ok(())
    })
}

#[test]
fn test_delete_branch() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Get the original branches
        let og_branches = command::list_branches(&repo)?;
        let og_branch = command::current_branch(&repo)?.unwrap();

        let branch_name = "my-branch";
        command::create_checkout_branch(&repo, branch_name)?;

        // Must checkout main again before deleting
        command::checkout(&repo, og_branch.name)?;

        // Now we can delete
        command::delete_branch(&repo, branch_name)?;

        // Should be same num as og_branches
        let leftover_branches = command::list_branches(&repo)?;
        assert_eq!(og_branches.len(), leftover_branches.len());

        Ok(())
    })
}

#[test]
fn test_cannot_delete_branch_you_are_on() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let branch_name = "my-branch";
        command::create_checkout_branch(&repo, branch_name)?;

        // Add another commit on this branch that moves us ahead of main
        if command::delete_branch(&repo, branch_name).is_ok() {
            panic!("Should not be able to delete the branch you are on");
        }

        Ok(())
    })
}

#[test]
fn test_cannot_force_delete_branch_you_are_on() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let branch_name = "my-branch";
        command::create_checkout_branch(&repo, branch_name)?;

        // Add another commit on this branch that moves us ahead of main
        if command::force_delete_branch(&repo, branch_name).is_ok() {
            panic!("Should not be able to force delete the branch you are on");
        }

        Ok(())
    })
}

#[test]
fn test_cannot_delete_branch_that_is_ahead_of_current() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let og_branches = command::list_branches(&repo)?;
        let og_branch = command::current_branch(&repo)?.unwrap();

        let branch_name = "my-branch";
        command::create_checkout_branch(&repo, branch_name)?;

        // Add another commit on this branch
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        // Checkout main again
        command::checkout(&repo, og_branch.name)?;

        // Should not be able to delete `my-branch` because it is ahead of `main`
        if command::delete_branch(&repo, branch_name).is_ok() {
            panic!("Should not be able to delete the branch that is ahead of the one you are on");
        }

        // Should be one less branch
        let leftover_branches = command::list_branches(&repo)?;
        assert_eq!(og_branches.len(), leftover_branches.len() - 1);

        Ok(())
    })
}

#[test]
fn test_force_delete_branch_that_is_ahead_of_current() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let og_branches = command::list_branches(&repo)?;
        let og_branch = command::current_branch(&repo)?.unwrap();

        let branch_name = "my-branch";
        command::create_checkout_branch(&repo, branch_name)?;

        // Add another commit on this branch
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        // Checkout main again
        command::checkout(&repo, og_branch.name)?;

        // Force delete
        command::force_delete_branch(&repo, branch_name)?;

        // Should be one less branch
        let leftover_branches = command::list_branches(&repo)?;
        assert_eq!(og_branches.len(), leftover_branches.len());

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
        command::checkout(&repo, og_branch.name)?;

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
        command::checkout(&repo, og_branch.name)?;

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
        command::checkout(&repo, og_branch.name)?;

        test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
        command::add(&repo, &labels_path)?;
        command::commit(&repo, "adding person category")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        // We should have a conflict
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
        command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
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
        let history = command::log(&repo)?;
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
        let history = command::log(&repo)?;
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
        let history = command::log(&repo)?;
        let last_commit = history.first().unwrap();

        let bbox_file = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_path = repo.path.join(&bbox_file);

        let og_contents = util::fs::read_from_path(&bbox_path)?;

        let og_df = tabular::scan_df(&bbox_path)?;
        let vals: Vec<String> = ["train/dog_99.jpg", "dog", "101.5", "32.0", "385", "330"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut new_df = tabular::add_row(og_df, vals)?.collect().unwrap();
        tabular::write_df(&mut new_df, &bbox_path)?;

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
        let history = command::log(&repo)?;
        let last_commit = history.first().unwrap();

        let bbox_file = Path::new("annotations")
            .join("train")
            .join("annotations.txt");
        let bbox_path = repo.path.join(&bbox_file);

        let og_contents = util::fs::read_from_path(&bbox_path)?;
        let new_contents = format!("{og_contents}\nnew 0");
        util::fs::write_to_path(&bbox_path, &new_contents);

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

// #[test]
// fn test_create_cadf_data_frame_with_duplicates() -> Result<(), OxenError> {
//     test::run_training_data_repo_test_no_commits(|repo| {
//         // Commit train
//         let ann_file = Path::new("nlp")
//             .join("classification")
//             .join("annotations")
//             .join("train.tsv");
//         let ann_path = repo.path.join(&ann_file);
//         command::add(&repo, &ann_path)?;
//         command::commit(&repo, "adding train data with duplicates")?.unwrap();

//         // Commit test
//         let ann_file = Path::new("nlp")
//             .join("classification")
//             .join("annotations")
//             .join("test.tsv");
//         let ann_path = repo.path.join(&ann_file);
//         command::add(&repo, &ann_path)?;
//         command::commit(&repo, "adding test data with duplicates")?.unwrap();

//         command::schema_name(
//             &repo,
//             "34a3b58f5471d7ae9580ebcf2582be2f",
//             "text_classification",
//         )?;

//         // Check that we saved off the CADF correctly
//         let schema = command::schema_get_from_head(&repo, "text_classification")?.unwrap();
//         let cadf_path = util::fs::schema_df_path(&repo, &schema);
//         let cadf = tabular::read_df(&cadf_path, DFOpts::empty())?;
//         println!("CADF {}", cadf);

//         // Should have added the _row_num and _row_hash columns
//         assert_eq!(cadf.width(), 4);
//         // Should be 8 unique examples
//         assert_eq!(cadf.height(), 8);

//         let result = format!("{}", cadf);
//         let str_val = r"shape: (8, 4)
// ┌──────────┬──────────────────────────────┬──────────┬──────────────────────────────────┐
// │ _row_num ┆ text                         ┆ label    ┆ _row_hash                        │
// │ ---      ┆ ---                          ┆ ---      ┆ ---                              │
// │ u32      ┆ str                          ┆ str      ┆ str                              │
// ╞══════════╪══════════════════════════════╪══════════╪══════════════════════════════════╡
// │ 0        ┆ My tummy hurts               ┆ negative ┆ 2036786c460064e4f6e6e04130fec79  │
// ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
// │ 1        ┆ I have a headache            ┆ negative ┆ cc1668083355fd32f615c9b61157832e │
// ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
// │ 2        ┆ loving the sunshine          ┆ positive ┆ 6332dba68bfbc9958c21a6a7c117dc20 │
// ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
// │ 3        ┆ And another unique one       ┆ positive ┆ 9d5c310dedfc1f4a40673915bde497ca │
// ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
// │ 4        ┆ I am a lonely example        ┆ negative ┆ 9fc377d8bd34d6b1da0fa38d54f2788  │
// ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
// │ 5        ┆ I am adding more examples    ┆ positive ┆ 2a0003e6d101f07baebc99dbc9e2a064 │
// ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
// │ 6        ┆ One more time                ┆ positive ┆ 65202a577c5f2636f1a0e69955264ef7 │
// ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
// │ 7        ┆ I am a great testing example ┆ positive ┆ 66e0ee400afea38ddde0d88cfa5feb60 │
// └──────────┴──────────────────────────────┴──────────┴──────────────────────────────────┘";

//         assert_eq!(result, str_val);

//         Ok(())
//     })
// }

// Make sure we can pull and unpack data from CADF with duplicates
// #[tokio::test]
// async fn test_push_pull_cadf_with_duplicates() -> Result<(), OxenError> {
//     test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
//         // Track a dir
//         let train_path = repo.path.join("nlp");
//         command::add(&repo, &train_path)?;
//         command::commit(&repo, "Adding nlp dir")?.unwrap();

//         // Create a schema name, so that we can test pull works
//         let schema_name = "text_classification";
//         command::schema_name(&repo, "34a3b58f5471d7ae9580ebcf2582be2f", schema_name)?;

//         let schema = command::schema_get_from_head(&repo, schema_name)?.unwrap();
//         let cadf_path = util::fs::schema_df_path(&repo, &schema);
//         let og_cadf = tabular::read_df(&cadf_path, DFOpts::empty())?;
//         println!("{}", og_cadf);

//         // Set the proper remote
//         let remote = test::repo_remote_url_from(&repo.dirname());
//         command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

//         // Create Remote
//         let remote_repo = test::create_remote_repo(&repo).await?;

//         // Push it
//         command::push(&repo).await?;

//         // run another test with a new repo dir that we are going to sync to
//         test::run_empty_dir_test_async(|new_repo_dir| async move {
//             let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
//             command::pull(&cloned_repo).await?;

//             let schema = command::schema_get_from_head(&cloned_repo, schema_name)?.unwrap();

//             let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
//             assert_eq!(2, cloned_num_files);

//             let cadf_path = util::fs::schema_df_path(&cloned_repo, &schema);
//             let cloned_cadf = tabular::read_df(&cadf_path, DFOpts::empty())?;
//             println!("OG: {}", og_cadf);
//             println!("Cloned: {}", cloned_cadf);

//             assert_eq!(cloned_cadf.height(), og_cadf.height());
//             assert_eq!(cloned_cadf.width(), og_cadf.width());
//             assert_eq!(format!("{}", cloned_cadf), format!("{}", og_cadf));

//             api::remote::repositories::delete(&remote_repo).await?;

//             Ok(new_repo_dir)
//         })
//         .await
//     })
//     .await
// }

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
        let commit = command::commit(&repo, "adding data with duplicates")?.unwrap();

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
        let commit = command::commit(&repo, "adding data with duplicates")?.unwrap();

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
        let schemas = command::schema_list(&repo, None)?;
        assert_eq!(schemas.len(), 3);

        let schema = command::schema_get_from_head(&repo, "bounding_box")?.unwrap();

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
fn test_command_merge_dataframe_conflict_both_added_rows_checkout_theirs() -> Result<(), OxenError>
{
    test::run_training_data_repo_test_fully_committed(|repo| {
        let og_branch = command::current_branch(&repo)?.unwrap();

        // Add a more rows on this branch
        let branch_name = "ox-add-rows";
        command::create_checkout_branch(&repo, branch_name)?;

        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);
        let bbox_file =
            test::append_line_txt_file(bbox_file, "train/cat_3.jpg,cat,41.0,31.5,410,427")?;
        let their_branch_contents = util::fs::read_from_path(&bbox_file)?;
        let their_df = tabular::read_df(&bbox_file, DFOpts::empty())?;
        println!("their df {}", their_df);

        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

        // Add a more rows on the main branch
        command::checkout(&repo, og_branch.name)?;

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
        println!("restored df {}", restored_df);

        let file_contents = util::fs::read_from_path(&bbox_file)?;

        assert_eq!(file_contents, their_branch_contents);

        Ok(())
    })
}

#[test]
fn test_command_merge_dataframe_conflict_both_added_rows_combine_uniq() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let og_branch = command::current_branch(&repo)?.unwrap();

        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);

        // Add a more rows on this branch
        let branch_name = "ox-add-rows";
        command::create_checkout_branch(&repo, branch_name)?;

        // Add in a line in this branch
        let row_from_branch = "train/cat_3.jpg,cat,41.0,31.5,410,427";
        let bbox_file = test::append_line_txt_file(bbox_file, row_from_branch)?;

        // Add the changes
        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

        // Add a more rows on the main branch
        command::checkout(&repo, og_branch.name)?;

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
}

#[test]
fn test_command_merge_dataframe_conflict_error_added_col() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let og_branch = command::current_branch(&repo)?.unwrap();

        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);

        // Add a more columns on this branch
        let branch_name = "ox-add-column";
        command::create_checkout_branch(&repo, branch_name)?;

        // Add in a column in this branch
        let mut opts = DFOpts::empty();
        opts.add_col = Some(String::from("random_col:unknown:str"));
        let df = tabular::scan_df(&bbox_file)?;
        let mut df = tabular::transform_df(df, opts)?;
        println!("WRITE DF IN BRANCH {:?}", df);
        tabular::write_df(&mut df, &bbox_file)?;

        // Add the changes
        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new column as an Ox on a branch.")?;

        // Add a more rows on the main branch
        command::checkout(&repo, og_branch.name)?;

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
        println!("{:?}", result);
        assert!(result.is_err());

        Ok(())
    })
}

#[test]
fn test_diff_tabular_add_col() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);

        let mut opts = DFOpts::empty();
        // Add Column
        opts.add_col = Some(String::from("is_cute:unknown:str"));
        // Save to Output
        opts.output = Some(bbox_file.clone());
        // Perform df transform
        command::df(bbox_file, opts)?;

        let diff = command::diff(&repo, None, &bbox_filename);
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
├╌╌╌╌╌╌╌╌╌┤
│ unknown │
├╌╌╌╌╌╌╌╌╌┤
│ unknown │
├╌╌╌╌╌╌╌╌╌┤
│ unknown │
├╌╌╌╌╌╌╌╌╌┤
│ unknown │
├╌╌╌╌╌╌╌╌╌┤
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
        let bbox_file = repo.path.join(&bbox_filename);

        let mut opts = DFOpts::empty();
        // Add Row
        opts.add_row = Some(String::from("train/cat_100.jpg,cat,100.0,100.0,100,100"));
        // Save to Output
        opts.output = Some(bbox_file.clone());
        // Perform df transform
        command::df(bbox_file, opts)?;

        match command::diff(&repo, None, &bbox_filename) {
            Ok(diff) => {
                println!("{}", diff);

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
        let bbox_file = repo.path.join(&bbox_filename);

        // Remove a row
        test::modify_txt_file(
            bbox_file,
            r"
file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
train/dog_2.jpg,dog,7.0,29.5,246,247
train/cat_2.jpg,cat,30.5,44.0,333,396
",
        )?;

        match command::diff(&repo, None, &bbox_filename) {
            Ok(diff) => {
                println!("{}", diff);

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
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/dog_3.jpg ┆ dog   ┆ 19.0  ┆ 63.5  ┆ 376   ┆ 421    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
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

#[test]
fn test_status_rm_regular_file() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        // Move the file to a new name
        let og_basename = PathBuf::from("README.md");
        let og_file = repo.path.join(&og_basename);
        std::fs::remove_file(&og_file)?;

        let status = command::status(&repo)?;
        status.print_stdout();

        assert_eq!(status.removed_files.len(), 1);

        command::rm(&repo, &og_file)?;
        let status = command::status(&repo)?;
        status.print_stdout();

        assert_eq!(status.added_files.len(), 1);
        assert_eq!(
            status.added_files[&og_basename].status,
            StagedEntryStatus::Removed
        );

        Ok(())
    })
}
