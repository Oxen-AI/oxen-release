//! # oxen push
//!
//! Push data from your local machine to a remote.
//!

use crate::api;
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::core::index::{pusher, EntryIndexer};
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, RemoteBranch, RemoteRepository};

/// # Get a log of all the commits
///
/// ```
/// # use liboxen::api;
/// # use liboxen::test;
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # #[tokio::main]
/// # async fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_push");
/// let mut repo = command::init(base_dir)?;
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
/// // Set the remote server
/// command::config::set_remote(&mut repo, "origin", "http://localhost:3000/repositories/hello");
///
/// let remote_repo = api::remote::repositories::create(&repo, "repositories", "hello", "localhost:3000").await?;
///
/// // Push the file
/// command::push(&repo).await;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # api::remote::repositories::delete(&remote_repo).await?;
/// # Ok(())
/// # }
/// ```
pub async fn push(repo: &LocalRepository) -> Result<Branch, OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let mut remote_branch = RemoteBranch::default();

    // Push the currently checked out branch
    let Some(local_branch) = api::local::branches::current_branch(repo)? else {
        return Err(OxenError::local_branch_not_found(DEFAULT_BRANCH_NAME));
    };

    let local_branch_cpy = local_branch.clone();
    remote_branch.branch = local_branch_cpy.clone().name;
    indexer.push(local_branch_cpy, remote_branch).await?;
    Ok(local_branch)
}

/// Push to a specific remote branch on the default remote repository
pub async fn push_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch_name: &str,
) -> Result<Branch, OxenError> {
    let Some(local_branch) = api::local::branches::get_by_name(repo, branch_name)? else {
        return Err(OxenError::local_branch_not_found(branch_name));
    };

    let indexer = EntryIndexer::new(repo)?;
    let remote_branch = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch_name),
    };
    indexer.push(local_branch.clone(), remote_branch).await?;
    Ok(local_branch)
}

/// Push to a specific remote repository
pub async fn push_remote_repo_branch(
    local_repo: LocalRepository,
    remote_repo: RemoteRepository,
    branch: Branch,
) -> Result<RemoteRepository, OxenError> {
    pusher::push_remote_repo(&local_repo, remote_repo, branch).await
}

/// Push to a specific remote repository, given a branch name
pub async fn push_remote_repo_branch_name(
    local_repo: LocalRepository,
    remote_repo: RemoteRepository,
    branch_name: &str,
) -> Result<RemoteRepository, OxenError> {
    let branch = api::local::branches::get_by_name(&local_repo, branch_name)?
        .ok_or(OxenError::local_branch_not_found(branch_name))?;
    push_remote_repo_branch(local_repo, remote_repo, branch).await
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::api;
    use crate::command;
    use crate::constants;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core::index::CommitEntryReader;

    use crate::error::OxenError;
    use crate::test;
    use crate::util;
    use futures::future;

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
                api::remote::dir::list(&remote_repo, &commit.id, "train", page_num, page_size)
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
                api::remote::dir::list(&remote_repo, &commit.id, "/train", page_num, page_size)
                    .await?;
            let test_entries =
                api::remote::dir::list(&remote_repo, &commit.id, "/test", page_num, page_size)
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
                api::remote::dir::list(&remote_repo, &commit.id, ".", page_num, 10).await?;
            assert_eq!(entries.total_entries, 2);
            assert_eq!(entries.entries.len(), 2);

            let page_size = num_test_files + 10;
            let entries =
                api::remote::dir::list(&remote_repo, &commit.id, "test", page_num, page_size)
                    .await?;
            assert_eq!(entries.total_entries, num_test_files);
            assert_eq!(entries.entries.len(), num_test_files);

            api::remote::repositories::delete(&remote_repo).await?;

            future::ok::<(), OxenError>(()).await
        })
        .await
    }

    #[tokio::test]
    async fn test_latest_commit_is_computed_properly() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async {
            // Make mutable copy so we can set remote
            let mut repo = repo;

            /*
            Create a directory structure with one file per nested dir
              (This was really slow in post commit actions, want to optimize)

            README.md
            data/
              file.txt
              1/
                file.txt
              2/
                file.txt
              3/
                file.txt
              4/
                file.txt
              5/
                file.txt
            */

            // Create README
            let readme_path = repo.path.join("README.md");
            let readme_path = test::write_txt_file_to_path(readme_path, "README")?;
            command::add(&repo, &readme_path)?;
            let first_commit_id = command::commit(&repo, "Adding README")?;

            // Create the data dir
            let data_dir = repo.path.join("data");
            util::fs::create_dir_all(&data_dir)?;

            // Create subdirs with files
            let num_dirs = 5;
            for i in 0..num_dirs {
                let dir_path = data_dir.join(format!("{}", i));
                util::fs::create_dir_all(&dir_path)?;
                let file_path = dir_path.join("file.txt");
                let file_path = test::write_txt_file_to_path(file_path, format!("file -> {}", i))?;
                command::add(&repo, &file_path)?;
                command::commit(&repo, &format!("Adding file -> data/{}/file.txt", i))?;
            }

            // modify the 3rd file
            let file_path = data_dir.join("2").join("file.txt");
            let file_path = test::write_txt_file_to_path(file_path, "modified file")?;
            command::add(&repo, &file_path)?;
            let last_commit = command::commit(&repo, "Modifying file again")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create the remote repo
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push the files
            command::push(&repo).await?;

            // Make sure we get the correct latest commit messages
            let page_num = 1;
            let entries =
                api::remote::dir::list(&remote_repo, &last_commit.id, ".", page_num, 10).await?;
            assert_eq!(entries.total_entries, 2);
            assert_eq!(entries.entries.len(), 2);

            // find the data entry, and make sure the latest_commit matches the last commit
            let data_entry = entries.entries.iter().find(|e| e.filename == "data");
            assert!(data_entry.is_some());
            let data_entry = data_entry.unwrap();
            assert_eq!(data_entry.filename, "data");
            assert_eq!(
                data_entry.latest_commit.as_ref().unwrap().id,
                last_commit.id
            );

            // find the README entry, and make sure latest_commit matches the first commit
            let readme_entry = entries.entries.iter().find(|e| e.filename == "README.md");
            assert!(readme_entry.is_some());
            let readme_entry = readme_entry.unwrap();
            assert_eq!(readme_entry.filename, "README.md");
            assert_eq!(
                readme_entry.latest_commit.as_ref().unwrap().id,
                first_commit_id.id
            );

            // Check the latest commit in a subdir
            let page_num = 1;
            let entries =
                api::remote::dir::list(&remote_repo, &last_commit.id, "data/3", page_num, 10)
                    .await?;
            assert_eq!(entries.total_entries, 1);
            assert_eq!(entries.entries.len(), 1);

            let entry = entries.entries.first().unwrap();
            assert_eq!(entry.filename, "file.txt");
            assert_eq!(
                entry.latest_commit.as_ref().unwrap().message,
                "Adding file -> data/3/file.txt"
            );

            api::remote::repositories::delete(&remote_repo).await?;

            future::ok::<(), OxenError>(()).await
        })
        .await
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
                api::remote::dir::list(&remote_repo, &commit.id, ".", page_num, page_size).await?;
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
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, new_branch_name)
                .await?;

            let remote_branches = api::remote::branches::list(&remote_repo).await?;
            assert_eq!(2, remote_branches.len());

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cannot_push_two_separate_empty_roots() -> Result<(), OxenError> {
        test::run_no_commit_remote_repo_test(|remote_repo| async move {
            let ret_repo = remote_repo.clone();

            // Clone the first repo
            test::run_empty_dir_test_async(|first_repo_dir| async move {
                let first_cloned_repo =
                    command::clone_url(&remote_repo.remote.url, &first_repo_dir.join("first_repo"))
                        .await?;

                // Clone the second repo
                test::run_empty_dir_test_async(|second_repo_dir| async move {
                    let second_cloned_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &second_repo_dir.join("second_repo"),
                    )
                    .await?;

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

    #[tokio::test]
    async fn test_push_many_commits_default_branch() -> Result<(), OxenError> {
        test::run_many_local_commits_empty_sync_remote_test(|local_repo, remote_repo| async move {
            // Current local head
            let local_head = api::local::commits::head_commit(&local_repo)?;

            // Branch name

            // Nothing should be synced on remote and no commit objects created except root
            let history =
                api::remote::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history.len(), 1);

            // Push all to remote
            command::push(&local_repo).await?;

            // Should now have 25 commits on remote
            let history =
                api::remote::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history.len(), 25);

            // Latest commit synced should be == local head, with no unsynced commits
            let sync_response =
                api::remote::commits::latest_commit_synced(&remote_repo, &local_head.id).await?;
            assert_eq!(sync_response.num_unsynced, 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_many_commits_new_branch() -> Result<(), OxenError> {
        test::run_many_local_commits_empty_sync_remote_test(|local_repo, remote_repo| async move {
            // Current local head
            let local_head = api::local::commits::head_commit(&local_repo)?;

            // Nothing should be synced on remote and no commit objects created except root
            let history =
                api::remote::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history.len(), 1);

            // Create new local branch
            let new_branch_name = "my-branch";
            api::local::branches::create_checkout(&local_repo, new_branch_name)?;

            // New commit
            let new_file = "new_file.txt";
            let new_file_path = local_repo.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
            command::add(&local_repo, &new_file_path)?;
            command::commit(&local_repo, "Adding first file path.")?;

            // Push new branch to remote without first syncing main
            command::push_remote_branch(
                &local_repo,
                constants::DEFAULT_REMOTE_NAME,
                new_branch_name,
            )
            .await?;

            // Should now have 26 commits on remote on new branch, 1 on main
            let history_new =
                api::remote::commits::list_commit_history(&remote_repo, new_branch_name).await?;
            let history_main =
                api::remote::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;

            assert_eq!(history_new.len(), 26);
            assert_eq!(history_main.len(), 1);

            // Back to main
            command::checkout(&local_repo, DEFAULT_BRANCH_NAME).await?;

            // Push to remote
            command::push(&local_repo).await?;

            // 25 on main
            let history_main =
                api::remote::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history_main.len(), 25);

            // 0 unsynced on main
            let sync_response =
                api::remote::commits::latest_commit_synced(&remote_repo, &local_head.id).await?;
            assert_eq!(sync_response.num_unsynced, 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_cannot_push_while_another_user_is_pushing() -> Result<(), OxenError> {
        test::run_no_commit_remote_repo_test(|remote_repo| async move {
            let ret_repo = remote_repo.clone();

            // Clone the first repo
            test::run_empty_dir_test_async(|first_repo_dir| async move {
                let first_cloned_repo =
                    command::clone_url(&remote_repo.remote.url, &first_repo_dir.join("first_repo"))
                        .await?;

                // Clone the second repo
                test::run_empty_dir_test_async(|second_repo_dir| async move {
                    let second_cloned_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &second_repo_dir.join("second_repo"),
                    )
                    .await?;

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

    // Test that we cannot clone separate repos with separate histories, then push to the same history
    // 1) Clone repo A with data
    // 2) Clone repo B with data
    // 3) Push Repo A
    // 4) Push repo B to repo A and fail
    #[tokio::test]
    async fn test_tree_cannot_push_two_separate_cloned_repos() -> Result<(), OxenError> {
        // Push the first repo with data
        test::run_training_data_fully_sync_remote(|_, remote_repo_1| async move {
            let remote_repo_1_copy = remote_repo_1.clone();

            // Push the second repo with data
            test::run_training_data_fully_sync_remote(|_, remote_repo_2| async move {
                let remote_repo_2_copy = remote_repo_2.clone();
                // Clone the first repo
                test::run_empty_dir_test_async(|first_repo_dir| async move {
                    let first_cloned_repo = command::clone_url(
                        &remote_repo_1.remote.url,
                        &first_repo_dir.join("first_repo_dir"),
                    )
                    .await?;

                    // Clone the second repo
                    test::run_empty_dir_test_async(|second_repo_dir| async move {
                        let mut second_cloned_repo = command::clone_url(
                            &remote_repo_2.remote.url,
                            &second_repo_dir.join("second_repo_dir"),
                        )
                        .await?;

                        // Add to the first repo, after we have the second repo cloned
                        let new_file = "new_file.txt";
                        let new_file_path = first_cloned_repo.path.join(new_file);
                        let new_file_path =
                            test::write_txt_file_to_path(new_file_path, "new file")?;
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
                        let new_file_path =
                            test::write_txt_file_to_path(new_file_path, "new file 2")?;
                        command::add(&second_cloned_repo, &new_file_path)?;
                        command::commit(&second_cloned_repo, "Adding second file path.")?;

                        let new_file = "new_file_3.txt";
                        let new_file_path = second_cloned_repo.path.join(new_file);
                        let new_file_path =
                            test::write_txt_file_to_path(new_file_path, "new file 3")?;
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

    // Test that we cannot push when the remote repo is ahead
    // 1) Clone repo to user A
    // 2) Clone repo to user B
    // 3) User A makes commit with `new_file.txt`` and pushes
    // 4) User B makes commit with `another_file.txt` pushes and succeeds
    // 5) User B pulls user A's changes, pushes and succeeds
    #[tokio::test]
    async fn test_tree_can_push_when_remote_repo_is_ahead_new_file() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo = command::clone_url(
                    &remote_repo.remote.url,
                    &user_a_repo_dir_copy.join("new_repo"),
                )
                .await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");

                    let user_b_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir_copy.join("New_repo"),
                    )
                    .await?;

                    // User A adds a file and pushes
                    let new_file = "new_file.txt";
                    let new_file_path = user_a_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    command::add(&user_a_repo, &new_file_path)?;
                    command::commit(&user_a_repo, "Adding first file path.")?;
                    command::push(&user_a_repo).await?;

                    // User B adds a different file and pushes
                    let different_file = "another_file.txt";
                    let new_file_path = user_b_repo.path.join(different_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "newer file")?;
                    command::add(&user_b_repo, &new_file_path)?;
                    command::commit(&user_b_repo, "Adding second file path.")?;

                    // Push should succeed now!!! there are no conflicts
                    let result = command::push(&user_b_repo).await;
                    assert!(result.is_ok());

                    // Pull should succeed
                    command::pull(&user_b_repo).await?;

                    // Push should now succeed
                    command::push(&user_b_repo).await?;

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // Test that we cannot push when the remote repo is ahead
    // * Clone repo to user A
    // * Clone repo to user B
    // * User A makes commit modifying `README.md` and pushes
    // * User B makes commit modifying `README.md` pushes and fails
    // * User B pulls user A's changes and there is a conflict
    // * User B fixes the conflict and pushes and succeeds
    #[tokio::test]
    async fn test_tree_cannot_push_when_remote_repo_is_ahead_same_file() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo = command::clone_url(
                    &remote_repo.remote.url,
                    &user_a_repo_dir_copy.join("new_repo"),
                )
                .await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");

                    let user_b_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir_copy.join("New_repo"),
                    )
                    .await?;

                    // User A modifies the README.md and pushes
                    let mod_file = "README.md";
                    let a_mod_file_path = user_a_repo.path.join(mod_file);
                    let a_mod_file_path =
                        test::write_txt_file_to_path(a_mod_file_path, "I am the README now")?;
                    command::add(&user_a_repo, &a_mod_file_path)?;
                    command::commit(&user_a_repo, "User A modifying the README.")?;
                    command::push(&user_a_repo).await?;

                    // User B tries to modify the same README.md and push
                    let b_mod_file_path = user_b_repo.path.join(mod_file);
                    let b_mod_file_path =
                        test::write_txt_file_to_path(b_mod_file_path, "I be the README now.")?;
                    command::add(&user_b_repo, &b_mod_file_path)?;
                    command::commit(&user_b_repo, "User B modifying the README.")?;

                    // Push should fail! Remote is ahead
                    let first_push_result = command::push(&user_b_repo).await;
                    assert!(first_push_result.is_err());

                    // Pull should succeed
                    command::pull(&user_b_repo).await?;

                    // There should be conflicts
                    let status = command::status(&user_b_repo)?;
                    assert!(status.has_merge_conflicts());

                    // User B resolves conflicts
                    let b_mod_file_path = user_b_repo.path.join(mod_file);
                    let b_mod_file_path = test::write_txt_file_to_path(
                        b_mod_file_path,
                        "No for real. I be the README now.",
                    )?;
                    command::add(&user_b_repo, &b_mod_file_path)?;
                    command::commit(&user_b_repo, "User B resolving conflicts.")?;

                    // Push should now succeed
                    let third_push_result = command::push(&user_b_repo).await;
                    assert!(third_push_result.is_ok());

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // Test that we cannot push when the remote repo multiple commits is ahead
    // * Create repo for user A
    // * Add data for user A
    // * Push data for user A
    // * Clone repo to user B
    // * User A makes commit modifying `README.md` and pushes
    // * User A makes commit modifying `annotations/train/bounding_box.csv` and pushes
    // * User B makes commit modifying `README.md` pushes and fails
    // * User B pulls user A's changes and there is a conflict
    // * User B fixes the conflict and pushes and succeeds
    #[tokio::test]
    async fn test_tree_cannot_push_when_remote_repo_is_2_commits_ahead_same_file(
    ) -> Result<(), OxenError> {
        // Create Repo for User A
        test::run_empty_dir_test_async(|user_a_repo_dir| async move {
            let mut user_a_repo = command::init(&user_a_repo_dir)?;

            // Add data for User A
            let mod_file = "README.md";
            let a_mod_file_path = user_a_repo.path.join(mod_file);
            let a_mod_file_path =
                test::write_txt_file_to_path(a_mod_file_path, "I am the original README")?;

            // Make a directory for the of random data
            let random_data_dir = user_a_repo.path.join("random_data");
            util::fs::create_dir_all(&random_data_dir)?;
            // add 10 text files to the random data directory
            for i in 0..10 {
                let file_name = format!("random_file_{}.txt", i);
                let file_path = random_data_dir.join(file_name);
                let file_path = test::write_txt_file_to_path(file_path, "random data")?;
                command::add(&user_a_repo, &file_path)?;
            }

            command::add(&user_a_repo, &a_mod_file_path)?;
            command::commit(&user_a_repo, "User A adding the README.")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&user_a_repo.dirname());
            command::config::set_remote(&mut user_a_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create the remote repo
            let remote_repo = test::create_remote_repo(&user_a_repo).await?;

            // Push data for User A
            println!("Pushing README.md for user A...");
            command::push(&user_a_repo).await?;

            // Clone Repo to User B
            test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");

                let user_b_repo = command::clone_url(
                    &remote_repo.remote.url,
                    &user_b_repo_dir_copy.join("New_repo"),
                )
                .await?;

                // User A modifies the bounding_box.csv and pushes
                let mod_file = PathBuf::from("bounding_box.csv");
                let a_mod_file_path = user_a_repo.path.join(mod_file);
                let a_mod_file_path =
                    test::write_txt_file_to_path(a_mod_file_path, "path,annotation")?;
                command::add(&user_a_repo, &a_mod_file_path)?;
                command::commit(&user_a_repo, "User A adds bounding_box.csv.")?;
                println!("Pushing bounding_box.csv for user A...");
                command::push(&user_a_repo).await?;

                // User B modifies the README.md and pushes
                let mod_file = "README.md";
                let b_mod_file_path = user_b_repo.path.join(mod_file);
                let b_mod_file_path =
                    test::write_txt_file_to_path(b_mod_file_path, "I be the README now.")?;
                command::add(&user_b_repo, &b_mod_file_path)?;
                println!("Pushing README.md for user B...");
                command::commit(&user_b_repo, "User B modifying the README.")?;

                // Push from B should succeed!
                let first_push_result = command::push(&user_b_repo).await;
                assert!(first_push_result.is_ok());

                // User A modifies tries to modify the same README.md and pushes
                let a_mod_file_path = user_a_repo.path.join(mod_file);
                let a_mod_file_path =
                    test::write_txt_file_to_path(a_mod_file_path, "I am the README now")?;
                command::add(&user_a_repo, &a_mod_file_path)?;
                command::commit(&user_a_repo, "User A modifying the README.")?;

                // Push should fail! Remote is ahead
                println!("Pushing README.md for user A...");
                let second_push_a = command::push(&user_a_repo).await;
                assert!(second_push_a.is_err());

                // Try it again - I don't know why this is succeeding the second time
                let second_push_again = command::push(&user_a_repo).await;
                assert!(second_push_again.is_err());

                // Pull A should succeed
                let pull_a = command::pull(&user_a_repo).await;
                assert!(pull_a.is_ok());

                // There should be conflicts in A
                let status = command::status(&user_a_repo)?;
                assert!(status.has_merge_conflicts());

                // User A resolves conflicts
                let a_mod_file_path = user_a_repo.path.join(mod_file);
                let a_mod_file_path = test::write_txt_file_to_path(
                    a_mod_file_path,
                    "No for real. I am the README now.",
                )?;
                command::add(&user_a_repo, &a_mod_file_path)?;
                command::commit(&user_a_repo, "User A resolving conflicts.")?;

                // Push should now succeed
                println!("Final pushing README.md for user A...");
                let third_push_result = command::push(&user_a_repo).await;
                assert!(third_push_result.is_ok());

                // Return repo B because that is the closure we are in
                Ok(user_b_repo_dir_copy)
            })
            .await?;

            Ok(user_a_repo_dir)
        })
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_tree_can_push_when_remote_repo_is_many_commits_ahead_new_file(
    ) -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo = command::clone_url(
                    &remote_repo.remote.url,
                    &user_a_repo_dir_copy.join("new_repo"),
                )
                .await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");

                    let user_b_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir_copy.join("new_repo"),
                    )
                    .await?;

                    // User A adds a file and pushes
                    let new_file = "new_file.txt";
                    let new_file_path = user_a_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    command::add(&user_a_repo, &new_file_path)?;
                    command::commit(&user_a_repo, "Adding first file path.")?;
                    command::push(&user_a_repo).await?;

                    // User A adds a file and pushes
                    let new_file = "new_file_2.txt";
                    let new_file_path = user_a_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    command::add(&user_a_repo, &new_file_path)?;
                    command::commit(&user_a_repo, "Adding second file path.")?;
                    command::push(&user_a_repo).await?;

                    // User A adds a file and pushes
                    let new_file = "new_file_3.txt";
                    let new_file_path = user_a_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    command::add(&user_a_repo, &new_file_path)?;
                    command::commit(&user_a_repo, "Adding third file path.")?;
                    command::push(&user_a_repo).await?;

                    // User B adds a different file and pushes
                    let different_file = "another_file.txt";
                    let new_file_path = user_b_repo.path.join(different_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "newer file")?;
                    command::add(&user_b_repo, &new_file_path)?;
                    command::commit(&user_b_repo, "User B adding second file path.")?;
                    // This should now succeed! Used to fail, but auto-merges now.
                    log::debug!("pushing b...");
                    let result = command::push(&user_b_repo).await;
                    assert!(result.is_ok());

                    command::pull(&user_b_repo).await?;

                    command::push(&user_b_repo).await?;

                    // Full pull
                    command::pull_all(&user_b_repo).await?;

                    // Push should now succeed
                    command::push(&user_b_repo).await?;

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_tree_can_push_when_remote_is_many_commits_ahead_no_tree_conflicts(
    ) -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo =
                    command::clone_url(&remote_repo.remote.url, &user_a_repo_dir.join("new_repo"))
                        .await?;

                // Log out all files in this directory with fs
                let files = util::fs::rlist_paths_in_dir(&user_a_repo_dir);
                for item in files {
                    log::debug!("\nfile or dir: {:?}\n", item)
                }

                // User A: add files in `nlp`
                // User B: add files in `annotations`

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_a_modify_dir = "nlp";
                    let user_b_modify_dir = "annotations";

                    let user_b_repo_dir_copy = user_b_repo_dir.clone();

                    let user_b_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir.join("new_repo"),
                    )
                    .await?;

                    // User A adds a file and pushes
                    let new_file = "new_file.txt";
                    let new_file_path = user_a_repo.path.join(user_a_modify_dir).join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    command::add(&user_a_repo, &new_file_path)?;
                    command::commit(&user_a_repo, "Adding first file path.")?;
                    command::push(&user_a_repo).await?;

                    // User A adds a file and pushes
                    let new_file = "new_file_2.txt";
                    let new_file_path = user_a_repo.path.join(user_a_modify_dir).join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    command::add(&user_a_repo, &new_file_path)?;
                    command::commit(&user_a_repo, "Adding second file path.")?;
                    command::push(&user_a_repo).await?;

                    // User A adds a file and pushes
                    let new_file = "new_file_3.txt";
                    let new_file_path = user_a_repo.path.join(user_a_modify_dir).join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    command::add(&user_a_repo, &new_file_path)?;
                    command::commit(&user_a_repo, "Adding third file path.")?;
                    command::push(&user_a_repo).await?;

                    // User B adds a different file and pushes
                    let different_file = "another_file.txt";
                    let new_file_path = user_b_repo
                        .path
                        .join(user_b_modify_dir)
                        .join(different_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "newer file")?;
                    command::add(&user_b_repo, &new_file_path)?;
                    command::commit(&user_b_repo, "User B adding second file path.")?;

                    // Push should succeed - different dirs!
                    command::push(&user_b_repo).await?;

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
    #[tokio::test]
    async fn test_tree_cannot_push_when_remote_is_many_commits_ahead_tree_conflicts(
    ) -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo =
                    command::clone_url(&remote_repo.remote.url, &user_a_repo_dir.join("new_repo"))
                        .await?;

                // Log out all files in this directory with fs
                let files = util::fs::rlist_paths_in_dir(&user_a_repo_dir);
                for item in files {
                    log::debug!("\nfile or dir: {:?}\n", item)
                }

                // User A: add files in `nlp`
                // User B: add files in `annotations`

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.clone();

                    let user_b_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir.join("new_repo"),
                    )
                    .await?;

                    // User A adds a file and pushes
                    let modify_path_a = user_a_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt");
                    let modify_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt");
                    test::write_txt_file_to_path(&modify_path_a, "new file")?;
                    command::add(&user_a_repo, &modify_path_a)?;
                    command::commit(&user_a_repo, "Adding first file path.")?;

                    command::push(&user_a_repo).await?;

                    // User B adds a different file and pushe
                    test::write_txt_file_to_path(&modify_path_b, "newer file")?;
                    command::add(&user_b_repo, &modify_path_b)?;
                    command::commit(&user_b_repo, "User B adding second file path.")?;

                    // Push should fail - this creates a merge conflict.
                    let res = command::push(&user_b_repo).await;
                    assert!(res.is_err());

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_tree_can_push_tree_no_conflict_added_file() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo =
                    command::clone_url(&remote_repo.remote.url, &user_a_repo_dir.join("new_repo"))
                        .await?;

                // Log out all files in this directory with fs
                let files = util::fs::rlist_paths_in_dir(&user_a_repo_dir);
                for item in files {
                    log::debug!("\nfile or dir: {:?}\n", item)
                }

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.clone();

                    log::debug!("About to clone b...");
                    let user_b_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir.join("new_repo"),
                    )
                    .await?;
                    log::debug!("finished cloning b.");

                    // User A adds a file and pushes
                    let modify_path_a = user_a_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("averynewfile.txt");
                    let modify_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("anothernewfile.txt");
                    test::write_txt_file_to_path(&modify_path_a, "new file")?;
                    command::add(&user_a_repo, &modify_path_a)?;
                    command::commit(&user_a_repo, "Adding first file path.")?;
                    command::push(&user_a_repo).await?;

                    // User B adds a different file and pushe
                    test::write_txt_file_to_path(&modify_path_b, "newer file")?;
                    command::add(&user_b_repo, &modify_path_b)?;
                    command::commit(&user_b_repo, "User B adding second file path.")?;

                    // Push should succeed - different dirs!
                    command::push(&user_b_repo).await?;

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_tree_cannot_push_tree_conflict_deleted_file() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo =
                    command::clone_url(&remote_repo.remote.url, &user_a_repo_dir.join("new_repo"))
                        .await?;

                // Log out all files in this directory with fs
                let files = util::fs::rlist_paths_in_dir(&user_a_repo_dir);
                for item in files {
                    log::debug!("\nfile or dir: {:?}\n", item)
                }

                // User A: add files in `nlp`
                // User B: add files in `annotations`

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.clone();

                    let user_b_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir.join("new_repo"),
                    )
                    .await?;

                    // User A deletes the file and commits
                    let modify_path_a = user_a_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt");
                    let modify_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt");

                    let _add_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("averynewfile.txt");

                    // print all files in annotations/train
                    let files = util::fs::rlist_paths_in_dir(
                        &user_b_repo.path.join("annotations").join("train"),
                    );
                    for item in files {
                        log::debug!("\npre file or dir: {:?}\n", item)
                    }
                    // User A modifies
                    test::write_txt_file_to_path(&modify_path_a, "fancy new file contents")?;
                    command::add(&user_a_repo, &modify_path_a)?;
                    let commit_a = command::commit(&user_a_repo, "modifying first file path.")?;
                    command::push(&user_a_repo).await?;

                    // User B deletes at user a path A modified, causing conflicts.
                    util::fs::remove_file(&modify_path_b)?;
                    let files = util::fs::rlist_paths_in_dir(
                        &user_b_repo.path.join("annotations").join("train"),
                    );
                    for item in files {
                        log::debug!("\npost file or dir: {:?}\n", item)
                    }
                    command::add(&user_b_repo, &modify_path_b)?;
                    // also add a file
                    // test::write_txt_file_to_path(&add_path_b, "new file")?;
                    // command::add(&user_b_repo, &add_path_b)?;

                    // Before this commit, init a reader at b's head
                    let pre_b = CommitEntryReader::new_from_head(&user_b_repo)?;
                    // get head commit
                    let head = api::local::commits::head_commit(&user_b_repo)?;
                    log::debug!("b head before is {:?}", head);

                    let maybe_b_entry = pre_b.get_entry(
                        &PathBuf::from("annotations")
                            .join("train")
                            .join("annotations.txt"),
                    )?;

                    log::debug!("maybe_b_entry before commit is {:?}", maybe_b_entry);

                    let commit_b = command::commit(&user_b_repo, "user B deleting file path.")?;

                    let post_b = CommitEntryReader::new_from_head(&user_b_repo)?;
                    let maybe_b_entry = post_b.get_entry(
                        &PathBuf::from("annotations")
                            .join("train")
                            .join("annotations.txt"),
                    )?;

                    log::debug!("maybe_b_entry after commitis {:?}", maybe_b_entry);

                    log::debug!("commit_a is {:?}", commit_a);
                    log::debug!("commit_b is {:?}", commit_b);

                    let commit_a =
                        api::local::commits::get_by_id(&user_a_repo, &commit_a.id)?.unwrap();
                    let commit_b =
                        api::local::commits::get_by_id(&user_b_repo, &commit_b.id)?.unwrap();

                    log::debug!("commit_a pre is {:?}", commit_a);
                    log::debug!("commit_b pre is {:?}", commit_b);

                    // Push should fail
                    let res = command::push(&user_b_repo).await;

                    log::debug!("here's the result and why it failed: {:?}", res);

                    assert!(res.is_err());

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_tree_can_push_tree_no_conflict_deleted_file() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo =
                    command::clone_url(&remote_repo.remote.url, &user_a_repo_dir.join("new_repo"))
                        .await?;

                // Log out all files in this directory with fs
                let files = util::fs::rlist_paths_in_dir(&user_a_repo_dir);
                for item in files {
                    log::debug!("\nfile or dir: {:?}\n", item)
                }

                // User A: add files in `nlp`
                // User B: add files in `annotations`

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.clone();

                    let user_b_repo = command::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir.join("new_repo"),
                    )
                    .await?;

                    // User A adds a file and pushes
                    let modify_path_a = user_a_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("averynewfile.txt");
                    let modify_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("anothernewfile.txt");
                    test::write_txt_file_to_path(&modify_path_a, "new file")?;
                    command::add(&user_a_repo, &modify_path_a)?;
                    command::commit(&user_a_repo, "Adding first file path.")?;
                    command::push(&user_a_repo).await?;

                    // User B adds a different file and pushe
                    test::write_txt_file_to_path(&modify_path_b, "newer file")?;
                    command::add(&user_b_repo, &modify_path_b)?;
                    command::commit(&user_b_repo, "User B adding second file path.")?;

                    // Push should succeed - different dirs!
                    command::push(&user_b_repo).await?;

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;
            Ok(remote_repo_copy)
        })
        .await
    }
    #[tokio::test]
    async fn test_tree_merge_on_push_to_branch() -> Result<(), OxenError> {
        let new_branch = "new_branch";
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo = command::deep_clone_url(
                    &remote_repo.remote.url,
                    &user_a_repo_dir.join("new_repo"),
                )
                .await?;

                // Save the current head of main
                let main_head = api::local::commits::head_commit(&user_a_repo)?;

                // User a checkout a branch
                command::create_checkout(&user_a_repo, new_branch)?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.clone();

                    let user_b_repo = command::deep_clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir.join("new_repo"),
                    )
                    .await?;

                    // User b checkout the same branch
                    command::create_checkout(&user_b_repo, new_branch)?;

                    // User A adds a file and pushes
                    let modify_path_a = user_a_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("averynewfile.txt");
                    let modify_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("anothernewfile.txt");
                    test::write_txt_file_to_path(&modify_path_a, "new file")?;
                    command::add(&user_a_repo, &modify_path_a)?;
                    command::commit(&user_a_repo, "Adding first file path.")?;
                    command::push(&user_a_repo).await?;

                    // User B adds a different file and pushe
                    test::write_txt_file_to_path(&modify_path_b, "newer file")?;
                    command::add(&user_b_repo, &modify_path_b)?;

                    command::commit(&user_b_repo, "User B adding second file path.")?;

                    // Push should succeed - different dirs!
                    command::push(&user_b_repo).await?;

                    // Get the new branch head
                    let new_main =
                        api::remote::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                            .await?
                            .unwrap();
                    let new_branch = api::remote::branches::get_by_name(&remote_repo, new_branch)
                        .await?
                        .unwrap();

                    // Assert commits have updated in the right place
                    assert_eq!(new_main.commit_id, main_head.id);

                    // Head at new_branch should be a merge commit
                    let new_branch_head =
                        api::remote::commits::get_by_id(&remote_repo, &new_branch.commit_id)
                            .await?
                            .unwrap();

                    // Must be a merge commit
                    assert_eq!(new_branch_head.parent_ids.len(), 2);

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;
            Ok(remote_repo_copy)
        })
        .await
    }
}
