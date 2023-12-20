//! # oxen pull
//!
//! Pull data from a remote branch
//!

use crate::core::index::EntryIndexer;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteBranch};
use crate::opts::PullOpts;

/// Pull a repository's data from default branches origin/main
/// Defaults defined in
/// `constants::DEFAULT_REMOTE_NAME` and `constants::DEFAULT_BRANCH_NAME`
pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer
        .pull(
            &rb,
            PullOpts {
                should_pull_all: false,
                should_update_head: true,
            },
        )
        .await
}

pub async fn pull_shallow(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer
        .pull(
            &rb,
            PullOpts {
                should_pull_all: false,
                should_update_head: true,
            },
        )
        .await
}

pub async fn pull_all(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer
        .pull(
            &rb,
            PullOpts {
                should_pull_all: true,
                should_update_head: true,
            },
        )
        .await
}

/// Pull a specific remote and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
    all: bool,
) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch),
    };
    indexer
        .pull(
            &rb,
            PullOpts {
                should_pull_all: all,
                should_update_head: true,
            },
        )
        .await
}

#[cfg(test)]
mod tests {
    use crate::core::index::CommitEntryReader;
    use crate::{command, api};
    use crate::error::OxenError;
    use crate::test;

    // Deal with merge conflicts on pull
    // 1) Clone repo to user A
    // 2) Clone repo to user B
    // 3) User A changes file commit and pushes
    // 4) User B changes same file, commites, and pushes and fails
    // 5) User B pulls user A's changes, there is a merge conflict
    // 6) User B cannot push until merge conflict is resolved
    #[tokio::test]
    async fn test_flags_merge_conflict_on_pull() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo =
                    command::clone_url(&remote_repo.remote.url, &user_a_repo_dir_copy).await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");

                    let user_b_repo =
                        command::clone_url(&remote_repo.remote.url, &user_b_repo_dir_copy).await?;

                    // User A adds a file and pushes
                    let new_file = "new_file.txt";
                    let new_file_path = user_a_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    command::add(&user_a_repo, &new_file_path)?;
                    command::commit(&user_a_repo, "User A changing file.")?;
                    command::push(&user_a_repo).await?;

                    // User B changes the same file and pushes
                    let new_file_path = user_b_repo.path.join(new_file);
                    let new_file_path =
                        test::write_txt_file_to_path(new_file_path, "I am user B, try to stop me")?;
                    command::add(&user_b_repo, &new_file_path)?;
                    command::commit(&user_b_repo, "User B changing file.")?;

                    // Push should fail
                    let result = command::push(&user_b_repo).await;
                    assert!(result.is_err());

                    // Pull
                    command::pull(&user_b_repo).await?;

                    // Check for merge conflict
                    let status = command::status(&user_b_repo)?;
                    assert!(!status.merge_conflicts.is_empty());
                    status.print_stdout();

                    // Checkout your version and add the changes
                    command::checkout_ours(&user_b_repo, new_file)?;
                    command::add(&user_b_repo, &new_file_path)?;
                    // Commit the changes
                    command::commit(&user_b_repo, "Taking my changes")?;

                    // Push should succeed
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
    async fn test_pull_does_not_remove_local_files() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_empty_sync_repo_test(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo =
                    command::clone_url(&remote_repo.remote.url, &user_a_repo_dir_copy).await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");
                    let user_b_repo =
                        command::clone_url(&remote_repo.remote.url, &user_b_repo_dir_copy).await?;

                    // Add file_1 and file_2 to user A repo
                    let file_1 = "file_1.txt";
                    test::write_txt_file_to_path(user_a_repo.path.join(file_1), "File 1")?;
                    let file_2 = "file_2.txt";
                    test::write_txt_file_to_path(user_a_repo.path.join(file_2), "File 2")?;

                    command::add(&user_a_repo, user_a_repo.path.join(file_1))?;
                    command::add(&user_a_repo, user_a_repo.path.join(file_2))?;

                    let a_commit = command::commit(&user_a_repo, "Adding file_1 and file_2")?;

                    // Push
                    command::push(&user_a_repo).await?;

                    // Add file_3 to user B repo
                    let file_3 = "file_3.txt";
                    test::write_txt_file_to_path(user_b_repo.path.join(file_3), "File 3")?;

                    command::add(&user_b_repo, user_b_repo.path.join(file_3))?;
                    let b_commit = command::commit(&user_b_repo, "Adding file_3")?;

                    // Pull changes without pushing first - fine since no conflict
                    command::pull(&user_b_repo).await?;

                    // Get new  head commit of the pulled repo 
                    let head_commit = api::local::commits::head_commit(&user_b_repo)?;
                    log::debug!("a commit is {:#?}", a_commit);
                    log::debug!("b commit is {:#?}", b_commit);
                    log::debug!("head commit is {:#?}", head_commit);

                    // commit entry reader here 
                    let commit_entry_reader = CommitEntryReader::new(&user_b_repo, &head_commit)?;

                    // list entries
                    let entries = commit_entry_reader.list_entries()?;


                    // UHHH THERE ARE NO ENTRIES IN THE HEAD COMMIT FOR REPO B LOL 

                    log::debug!("entries in repo b are {:#?}", entries);
                    for entry in entries {
                        log::debug!("entry in repo b is {:#?}", entry);
                    }


                    // Make sure we now have all three files
                    assert!(user_b_repo.path.join(file_1).exists());
                    assert!(user_b_repo.path.join(file_2).exists());
                    assert!(user_b_repo.path.join(file_3).exists());

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
