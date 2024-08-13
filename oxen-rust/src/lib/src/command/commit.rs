//! # oxen commit
//!
//! Commit the staged data
//!

use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;

/// # Commit the staged files in the repo
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::test;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_commit");
/// let repo = command::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// command::add(&repo, &hello_file)?;
///
/// // Commit staged
/// command::commit(&repo, "My commit message")?;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn commit(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    repositories::commits::commit(repo, message)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::command;
    use crate::core::v0_10_0::index::CommitEntryReader;
    use crate::error::OxenError;
    use crate::model::StagedEntryStatus;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_commit_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            command::add(&repo, &hello_file)?;
            // Commit the file
            let commit = command::commit(&repo, "My message")?;
            assert_eq!(commit.message, "My message");

            // Get status and make sure it is removed from the untracked and added
            let repo_status = command::status(&repo)?;
            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_commit_removed_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            command::add(&repo, &hello_file)?;

            // Remove the file
            util::fs::remove_file(&hello_file)?;

            // Commit the file
            command::commit(&repo, "My message")?;

            // Get status and make sure the file was not committed
            let head = repositories::commits::head_commit(&repo)?;
            let commit_reader = CommitEntryReader::new(&repo, &head)?;
            let commit_list = commit_reader.list_files()?;
            assert_eq!(commit_list.len(), 0);

            // Test subsequent commit
            let goodbye_file = repo.path.join("goodbye.txt");
            util::fs::write_to_path(&goodbye_file, "Goodbye World")?;

            command::add(&repo, &goodbye_file)?;

            util::fs::remove_file(&goodbye_file)?;

            command::commit(&repo, "Second Message")?;

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
            repo_status.print();
            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 2);
            assert_eq!(repo_status.untracked_dirs.len(), 4);

            let commits = repositories::commits::list(&repo)?;
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
            repo_status.print();

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 2);
            assert_eq!(repo_status.untracked_dirs.len(), 4);

            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 2);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_command_commit_top_level_dir_then_revert() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a branch to make the changes
            let branch_name = "feature/adding-train";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Track & commit (train dir already created in helper)
            let train_path = repo.path.join("train");
            let og_num_files = util::fs::rcount_files_in_dir(&train_path);

            // Add directory
            command::add(&repo, &train_path)?;
            // Make sure we can get the status
            let status = command::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 1);

            // Commit changes
            command::commit(&repo, "Adding train dir")?;
            // Make sure we can get the status and they are no longer added
            let status = command::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 0);

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
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a branch to make the changes
            let branch_name = "feature/adding-annotations";
            repositories::branches::create_checkout(&repo, branch_name)?;

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
            assert_eq!(status.staged_files.len(), og_file_count);
            assert_eq!(
                status.staged_files.iter().next().unwrap().1.status,
                StagedEntryStatus::Removed
            );

            // Make sure they don't show up in the status
            assert_eq!(status.removed_files.len(), 0);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_commit_after_merge_conflict() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            let labels_path = repo.path.join("labels.txt");
            command::add(&repo, &labels_path)?;
            command::commit(&repo, "adding initial labels file")?;

            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Add a "none" category on a branch
            let branch_name = "change-labels";
            repositories::branches::create_checkout(&repo, branch_name)?;

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
            let history = repositories::commits::list(&repo)?;
            assert_eq!(history.len(), 5);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_hash_on_modified_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Add a text file
            let text_path = repo.path.join("text.txt");
            util::fs::write_to_path(&text_path, "Hello World")?;

            // Get the hash of the file at this timestamp
            let hash_when_add = util::hasher::hash_file_contents(&text_path)?;
            command::add(&repo, &text_path)?;

            // Modify the text file
            util::fs::write_to_path(&text_path, "Goodbye, world!")?;

            // Get the new hash
            let hash_after_modification = util::hasher::hash_file_contents(&text_path)?;

            // Commit the file
            command::commit(&repo, "My message")?;

            // Get the most recent commit - the new head commit
            let head = repositories::commits::head_commit(&repo)?;

            // Initialize a commit entry reader here
            let commit_reader = CommitEntryReader::new(&repo, &head)?;

            // Get the commit entry for the text file
            let text_entry = commit_reader.get_entry(Path::new("text.txt"))?.unwrap();

            // Hashes should be different
            assert_ne!(hash_when_add, hash_after_modification);

            // Hash should match new hash
            assert_eq!(text_entry.hash, hash_after_modification);

            Ok(())
        })
    }
}
