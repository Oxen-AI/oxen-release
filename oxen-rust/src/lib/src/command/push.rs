//! # oxen push
//!
//! Push data from your local machine to a remote.
//!

use crate::api;
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
pub async fn push(repo: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer.push(&rb).await
}

/// Push to a specific remote branch on the default remote repository
pub async fn push_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
) -> Result<RemoteRepository, OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch),
    };
    indexer.push(&rb).await
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
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::test;

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
                    let first_cloned_repo =
                        command::clone_url(&remote_repo_1.remote.url, &first_repo_dir).await?;

                    // Clone the second repo
                    test::run_empty_dir_test_async(|second_repo_dir| async move {
                        let mut second_cloned_repo =
                            command::clone_url(&remote_repo_2.remote.url, &second_repo_dir).await?;

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
    // 3) User A makes commit and pushes
    // 4) User B makes commit, pushes and fails
    // 5) User B pulls user A's changes, pushes and succeeds
    #[tokio::test]
    async fn test_cannot_push_when_remote_repo_is_ahead_new_file() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo =
                    command::clone_url(&remote_repo.remote.url, &user_a_repo_dir).await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.clone();

                    let user_b_repo =
                        command::clone_url(&remote_repo.remote.url, &user_b_repo_dir).await?;

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

                    // Push should fail
                    let result = command::push(&user_b_repo).await;
                    assert!(result.is_err());

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
}
