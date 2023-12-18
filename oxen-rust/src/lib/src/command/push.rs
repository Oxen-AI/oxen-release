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
    let mut rb = RemoteBranch::default();

    // Push the currently checked out branch
    if let Some(current_branch) = api::local::branches::current_branch(repo)? {
        rb.branch = current_branch.name;
    }

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
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::core::db::path_db;
    use crate::core::db::tree_db::TreeObject;
    use crate::core::index::CommitEntryWriter;
    use crate::error::OxenError;
    use crate::test;
    use crate::util;

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
    // 3) User A makes commit and pushes
    // 4) User B makes commit, pushes and fails
    // 5) User B pulls user A's changes, pushes and succeeds
    #[tokio::test]
    async fn test_tree_cannot_push_when_remote_repo_is_ahead_new_file() -> Result<(), OxenError> {
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

                    // Push should succeed now! there are no conflicts
                    log::debug!("pushing b...");
                    let result = command::push(&user_b_repo).await;
                    log::debug!("done pushing b, here's result: {:?}", result);
                    assert!(result.is_ok());

                    log::debug!("about to pull just a little bit");

                    command::pull(&user_b_repo).await?;

                    log::debug!("done pulling just a little bit");

                    command::push(&user_b_repo).await?;
                    log::debug!("not sure what we even pushed there");

                    // Full pull
                    command::pull_all(&user_b_repo).await?;

                    log::debug!("pulled full");

                    // Push should now succeed
                    command::push(&user_b_repo).await?;

                    log::debug!("pushed full but again not with anything at all");

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

                    let add_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("averynewfile.txt");


                    // print all files in annotations/train 
                    let files = util::fs::rlist_paths_in_dir(&user_b_repo.path.join("annotations").join("train"));
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
                    let files = util::fs::rlist_paths_in_dir(&user_b_repo.path.join("annotations").join("train"));
                    for item in files {
                        log::debug!("\npost file or dir: {:?}\n", item)
                    }
                    command::add(&user_b_repo, &modify_path_b)?;
                    // also add a file 
                    test::write_txt_file_to_path(&add_path_b, "new file")?;
                    // command::add(&user_b_repo, &add_path_b)?;
                    let commit_b = command::commit(&user_b_repo, "user B deleting file path.")?;
                    
                    log::debug!("commit_a is {:?}", commit_a);
                    log::debug!("commit_b is {:?}", commit_b);

                    let commit_a = api::local::commits::get_by_id(&user_a_repo, &commit_a.id)?.unwrap();
                    let commit_b = api::local::commits::get_by_id(&user_b_repo, &commit_b.id)?.unwrap();

                    log::debug!("commit_a pre is {:?}", commit_a);
                    log::debug!("commit_b pre is {:?}", commit_b);
                    

                    // // Get the root hash of each dir 
                    // let root_hash_a: String = path_db::get_entry(&a_writer.dir_hashes_db, "")?.unwrap();
                    // let root_hash_b: String = path_db::get_entry(&b_writer.dir_hashes_db, "")?.unwrap();

                    // // Get the root node of each dir
                    // let root_node_a: TreeObject = path_db::get_entry(&a_writer.dirs_db, &root_hash_a)?.unwrap();
                    // let root_node_b: TreeObject = path_db::get_entry(&b_writer.dirs_db, &root_hash_b)?.unwrap();

                    // log::debug!("root hash a is {:?}", root_hash_a);
                    // log::debug!("root hash b is {:?}", root_hash_b);

                    // log::debug!("root node a is {:?}", root_node_a);
                    // log::debug!("root node b is {:?}", root_node_b);



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
}
