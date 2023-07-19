use liboxen::api;
use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::StagedEntryStatus;
use liboxen::test;
use liboxen::util;

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
async fn test_command_commit_second_level_dir_then_revert() -> Result<(), OxenError> {
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
fn test_command_commit_removed_dir() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // (dir already created in helper)
        let dir_to_remove = repo.path.join("train");
        let og_file_count = util::fs::rcount_files_in_dir(&dir_to_remove);

        command::add(&repo, &dir_to_remove)?;
        command::commit(&repo, "Adding train directory")?;

        // Delete the directory
        util::fs::remove_dir_all(&dir_to_remove)?;

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
