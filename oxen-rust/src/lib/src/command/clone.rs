//! # oxen clone
//!
//! Clone data from a remote repository
//!

use std::path::Path;

use crate::config::repository_config::RepositoryConfig;
use crate::constants::{
    DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME, MIN_OXEN_VERSION, REPO_CONFIG_FILENAME,
};
use crate::core::v0_10_0::index::EntryIndexer;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote, RemoteBranch, RemoteRepository};
use crate::opts::{CloneOpts, PullOpts};
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{api, util};

pub async fn clone(opts: &CloneOpts) -> Result<LocalRepository, OxenError> {
    match clone_remote(opts).await {
        Ok(Some(repo)) => Ok(repo),
        Ok(None) => Err(OxenError::remote_repo_not_found(&opts.url)),
        Err(err) => Err(err),
    }
}

pub async fn clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let shallow = false;
    let all = false;
    _clone(url, dst, shallow, all).await
}

pub async fn shallow_clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let shallow = true;
    let all = false;
    _clone(url, dst, shallow, all).await
}

pub async fn deep_clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let shallow = false;
    let all = true;
    _clone(url, dst, shallow, all).await
}

async fn _clone(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
    shallow: bool,
    all: bool,
) -> Result<LocalRepository, OxenError> {
    let opts = CloneOpts {
        url: url.as_ref().to_string(),
        dst: dst.as_ref().to_owned(),
        shallow,
        all,
        branch: DEFAULT_BRANCH_NAME.to_string(),
    };
    clone(&opts).await
}

async fn clone_remote(opts: &CloneOpts) -> Result<Option<LocalRepository>, OxenError> {
    log::debug!(
        "clone_remote {} -> {:?} -> shallow? {} -> all? {}",
        opts.url,
        opts.dst,
        opts.shallow,
        opts.all
    );

    let remote = Remote {
        name: String::from(DEFAULT_REMOTE_NAME),
        url: opts.url.to_owned(),
    };
    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or_else(|| OxenError::remote_repo_not_found(&opts.url))?;
    let repo = clone_repo(remote_repo, opts).await?;
    Ok(Some(repo))
}

async fn clone_repo(
    remote_repo: RemoteRepository,
    opts: &CloneOpts,
) -> Result<LocalRepository, OxenError> {
    api::client::repositories::pre_clone(&remote_repo).await?;

    // if directory already exists -> return Err
    let repo_path = &opts.dst;
    if repo_path.exists() {
        let err = format!("Directory already exists: {}", remote_repo.name);
        return Err(OxenError::basic_str(err));
    }

    // if directory does not exist, create it
    std::fs::create_dir_all(repo_path)?;

    // if create successful, create .oxen directory
    let oxen_hidden_path = util::fs::oxen_hidden_dir(repo_path);
    std::fs::create_dir(&oxen_hidden_path)?;

    // save LocalRepository in .oxen directory
    let repo_config_file = oxen_hidden_path.join(Path::new(REPO_CONFIG_FILENAME));
    let mut local_repo = LocalRepository::from_remote(remote_repo.clone(), repo_path)?;
    repo_path.clone_into(&mut local_repo.path);
    local_repo.set_remote(DEFAULT_REMOTE_NAME, &remote_repo.remote.url);

    // Save remote config in .oxen/config.toml
    let remote_cfg = RepositoryConfig {
        remote_name: Some(DEFAULT_REMOTE_NAME.to_string()),
        remotes: vec![remote_repo.remote.clone()],
        min_version: Some(MIN_OXEN_VERSION.to_string()),
    };

    let toml = toml::to_string(&remote_cfg)?;
    util::fs::write_to_path(&repo_config_file, &toml)?;

    // Pull all commit objects, but not entries
    let rb = RemoteBranch::from_branch(&opts.branch);
    let indexer = EntryIndexer::new(&local_repo)?;
    maybe_pull_entries(&local_repo, &remote_repo, &indexer, &rb, opts).await?;

    if opts.all {
        log::debug!("pulling all entries");
        let remote_branches = api::client::branches::list(&remote_repo).await?;
        if remote_branches.len() > 1 {
            println!(
                "🐂 Pre-fetching {} additional remote branches...",
                remote_branches.len() - 1
            );
        }

        let n_other_branches: u64 = if remote_branches.len() > 1 {
            (remote_branches.len() - 1) as u64
        } else {
            0
        };

        let bar = oxen_progress_bar(n_other_branches as u64, ProgressBarType::Counter);

        for branch in remote_branches {
            // We've already pulled the target branch in full
            if branch.name == rb.branch {
                continue;
            }

            let remote_branch = RemoteBranch::from_branch(&branch.name);
            indexer
                .pull_most_recent_commit_object(&remote_repo, &remote_branch, false)
                .await?;
            bar.inc(1);
        }
        bar.finish_and_clear();
    }

    println!(
        "\n🎉 cloned {} to {}/\n",
        remote_repo.remote.url, remote_repo.name
    );
    api::client::repositories::post_clone(&remote_repo).await?;

    Ok(local_repo)
}

async fn maybe_pull_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    indexer: &EntryIndexer,
    rb: &RemoteBranch,
    opts: &CloneOpts,
) -> Result<(), OxenError> {
    // Shallow means we will not pull the actual data until a user tells us to
    if opts.shallow {
        indexer
            .pull_most_recent_commit_object(remote_repo, rb, true)
            .await?;
        local_repo.write_is_shallow(true)?;
    } else {
        // Pull all entries
        indexer
            .pull(
                rb,
                PullOpts {
                    should_pull_all: opts.all,
                    should_update_head: true,
                },
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::error::OxenError;
    use crate::model::RepoNew;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use super::*;

    #[tokio::test]
    async fn test_clone_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            // Create remote repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            log::debug!("created the remote repo");

            test::run_empty_dir_test_async(|dir| async move {
                let opts = CloneOpts::new(remote_repo.remote.url.to_owned(), dir.join("new_repo"));

                log::debug!("about to clone the remote");
                let local_repo = clone_remote(&opts).await?.unwrap();
                log::debug!("succeeded");
                let cfg_fname = ".oxen/config.toml".to_string();
                let config_path = local_repo.path.join(&cfg_fname);
                assert!(config_path.exists());

                let repository = LocalRepository::from_dir(&local_repo.path);
                assert!(repository.is_ok());

                let repository = repository.unwrap();
                let status = command::status(&repository)?;
                assert!(status.is_clean());

                // Cleanup
                api::client::repositories::delete(&remote_repo).await?;

                Ok(dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_move_local_repo_path_valid() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            // Create remote repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            test::run_empty_dir_test_async(|dir| async move {
                let opts = CloneOpts::new(remote_repo.remote.url.to_owned(), dir.join("new_repo"));
                let local_repo = clone_remote(&opts).await?.unwrap();

                api::client::repositories::delete(&remote_repo).await?;

                command::status(&local_repo)?;

                let new_path = dir.join("new_path");

                util::fs::rename(&local_repo.path, &new_path)?;

                let new_repo = LocalRepository::from_dir(&new_path)?;
                command::status(&new_repo)?;
                assert_eq!(new_repo.path, new_path);

                Ok(dir)
            })
            .await
        })
        .await
    }

    // Test for clone --all that checks to make sure we have all commits, all deleted files, etc
    #[tokio::test]
    async fn test_clone_dash_all() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            // Create additional branch on remote repo before clone
            let branch_name = "test-branch";
            api::client::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let cloned_remote = remote_repo.clone();
            let og_commits = repositories::commits::list_all(&local_repo)?;
            let og_branches = api::client::branches::list(&remote_repo).await?;

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = command::deep_clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;

                // Make sure we have all the commit objects
                let cloned_commits = repositories::commits::list_all(&cloned_repo)?;
                assert_eq!(og_commits.len(), cloned_commits.len());

                // Make sure we have all branches
                let cloned_branches = repositories::branches::list(&cloned_repo)?;
                assert_eq!(og_branches.len(), cloned_branches.len());

                // Make sure we set the HEAD file
                let head_commit = repositories::commits::head_commit(&cloned_repo);
                assert!(head_commit.is_ok());

                // We remove the test/ directory in one of the commits, so make sure we can go
                // back in the history to that commit
                let test_dir_path = cloned_repo.path.join("test");
                println!("test_clone_dash_all test_dir_path: {:?}", test_dir_path);
                let commit = repositories::commits::first_by_message(&cloned_repo, "Adding test/")?;
                assert!(commit.is_some());
                assert!(!test_dir_path.exists());

                // checkout the commit
                command::checkout(&cloned_repo, &commit.unwrap().id).await?;
                // Make sure we restored the directory
                assert!(test_dir_path.exists());

                // list files in test_dir_path
                let test_dir_files = util::fs::list_files_in_dir(&test_dir_path);
                println!("test_dir_files: {:?}", test_dir_files.len());
                for file in test_dir_files.iter() {
                    println!("file: {:?}", file);
                }
                assert_eq!(test_dir_files.len(), 2);

                assert!(test_dir_path.join("1.jpg").exists());
                assert!(test_dir_path.join("2.jpg").exists());

                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    // Test for clone --all that checks to make sure we have all commits, all deleted files, etc
    #[tokio::test]
    async fn test_clone_all_push_all() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let mut cloned_repo =
                    command::deep_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

                let repo_name = format!("new_remote_repo_name_{}", uuid::Uuid::new_v4());
                let remote_url = test::repo_remote_url_from(&repo_name);
                let remote_name = "different";

                // Create a different repo
                let repo_new = RepoNew::from_namespace_name_host(
                    constants::DEFAULT_NAMESPACE,
                    repo_name,
                    test::test_host(),
                );
                api::client::repositories::create_from_local(&cloned_repo, repo_new).await?;

                command::config::set_remote(&mut cloned_repo, remote_name, &remote_url)?;

                // Should be able to push all data successfully
                command::push_remote_branch(&cloned_repo, remote_name, "main").await?;

                // TODO: figure out how to repro why the Pets Dataset we could not clone --all and push to staging

                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_all_push_all_modified_deleted_files() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Create a new text file
            let filename = "file_to_modify.txt";
            let filepath = local_repo.path.join(filename);
            test::write_txt_file_to_path(&filepath, "Content before modification")?;
            command::add(&local_repo, &filepath)?;
            command::commit(&local_repo, "Adding file_to_modify.txt")?;

            // Change the file's contents - different hash
            test::write_txt_file_to_path(&filepath, "A whole new hash now!")?;
            command::add(&local_repo, &filepath)?;
            command::commit(&local_repo, "Modifying file_to_modify.txt")?;

            test::write_txt_file_to_path(&filepath, "Changing againnnnn")?;
            command::add(&local_repo, &filepath)?;
            command::commit(&local_repo, "Modifying file_to_modify.txt")?;

            // Delete file
            std::fs::remove_file(&filepath)?;
            command::add(&local_repo, &filepath)?;
            command::commit(&local_repo, "Deleting file_to_modify.txt")?;

            // Add back new
            test::write_txt_file_to_path(&filepath, "Adding back new")?;
            command::add(&local_repo, &filepath)?;
            command::commit(&local_repo, "Adding back file_to_modify.txt")?;

            command::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, DEFAULT_BRANCH_NAME)
                .await?;

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let mut cloned_repo =
                    command::deep_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

                let repo_name = format!("new_remote_repo_name_{}", uuid::Uuid::new_v4());
                let remote_url = test::repo_remote_url_from(&repo_name);
                let remote_name = "different";

                // Create a different repo
                let repo_new = RepoNew::from_namespace_name_host(
                    constants::DEFAULT_NAMESPACE,
                    repo_name,
                    test::test_host(),
                );
                api::client::repositories::create_empty(repo_new).await?;

                command::config::set_remote(&mut cloned_repo, remote_name, &remote_url)?;

                // Should be able to push all data successfully
                command::push_remote_branch(&cloned_repo, remote_name, "main").await?;

                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_shallow_cannot_push_all() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let mut cloned_repo =
                    command::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

                let repo_name = format!("new_remote_repo_name_{}", uuid::Uuid::new_v4());
                let remote_url = test::repo_remote_url_from(&repo_name);
                let remote_name = "different";

                // Create a different repo
                let repo_new = RepoNew::from_namespace_name_host(
                    constants::DEFAULT_NAMESPACE,
                    repo_name,
                    test::test_host(),
                );
                api::client::repositories::create_from_local(&cloned_repo, repo_new).await?;

                command::config::set_remote(&mut cloned_repo, remote_name, &remote_url)?;

                // Should fail
                let push_res = command::push_remote_branch(&cloned_repo, remote_name, "main").await;
                assert!(push_res.is_err());
                // TODO: figure out how to repro why the Pets Dataset we could not clone --all and push to staging

                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
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
                let cloned_repo =
                    command::clone_url(&remote_repo.remote.url, &new_repo_dir.join("new_repo"))
                        .await?;
                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
                // 2 test, 5 train, 1 labels
                assert_eq!(8, cloned_num_files);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_oxen_clone_empty_repo() -> Result<(), OxenError> {
        test::run_no_commit_remote_repo_test(|remote_repo| async move {
            let ret_repo = remote_repo.clone();

            // Create a new repo to clone to, then clean it up
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo =
                    command::clone_url(&remote_repo.remote.url, &new_repo_dir.join("new_repo"))
                        .await?;

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
                let cloned_repo =
                    command::clone_url(&remote_repo.remote.url, &new_repo_dir.join("new_repo"))
                        .await?;

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
}
