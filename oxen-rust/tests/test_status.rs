use liboxen::api;
use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::StagedEntryStatus;
use liboxen::opts::RmOpts;
use liboxen::test;
use liboxen::util;

use std::path::Path;
use std::path::PathBuf;

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
        util::fs::write_to_path(labels_path, "changing this guy, but not committing")?;

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
        util::fs::write_to_path(hello_file, "Hello World")?;

        // Get status
        let repo_status = command::status(&repo)?;
        assert_eq!(repo_status.added_dirs.len(), 0);
        assert_eq!(repo_status.added_files.len(), 0);
        assert_eq!(repo_status.untracked_files.len(), 1);
        assert_eq!(repo_status.untracked_dirs.len(), 0);

        Ok(())
    })
}

#[tokio::test]
async fn test_merge_conflict_shows_in_status() -> Result<(), OxenError> {
    test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
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
async fn test_status_rm_regular_file() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        // Move the file to a new name
        let og_basename = PathBuf::from("README.md");
        let og_file = repo.path.join(&og_basename);
        util::fs::remove_file(og_file)?;

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
        util::fs::remove_file(og_file)?;

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
