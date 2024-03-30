//! # oxen remote add
//!
//! Stage a file on a remote repository branch
//!

use std::path::{Path, PathBuf};

use crate::api;
use crate::config::UserConfig;
use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core::index::oxenignore;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteBranch};
use crate::opts::AddOpts;
use crate::util;

pub async fn add<P: AsRef<Path>>(
    repo: &LocalRepository,
    path: P,
    opts: &AddOpts,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    // * make sure we are on a branch
    let branch = api::local::branches::current_branch(repo)?;
    if branch.is_none() {
        return Err(OxenError::must_be_on_valid_branch());
    }

    // * make sure file is not in .oxenignore
    let ignore = oxenignore::create(repo);
    if let Some(ignore) = ignore {
        if ignore.matched(path, path.is_dir()).is_ignore() {
            return Ok(());
        }
    }

    // * read in file and post it to remote
    let branch = branch.unwrap();
    let rb = RemoteBranch {
        remote: DEFAULT_REMOTE_NAME.to_string(),
        branch: branch.name.to_owned(),
    };
    let remote = repo
        .get_remote(&rb.remote)
        .ok_or(OxenError::remote_not_set(&rb.remote))?;

    log::debug!("Pushing to remote {:?}", remote);
    // Repo should be created before this step
    let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    let (remote_directory, resolved_path) = resolve_remote_add_file_path(repo, path, opts)?;
    let directory_name = remote_directory.to_string_lossy().to_string();

    let user_id = UserConfig::identifier()?;
    let result = api::remote::staging::add_file(
        &remote_repo,
        &branch.name,
        &user_id,
        &directory_name,
        resolved_path,
    )
    .await?;

    println!("{}", result.to_string_lossy());

    Ok(())
}

/// Returns (remote_directory, resolved_path)
fn resolve_remote_add_file_path(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    opts: &AddOpts,
) -> Result<(PathBuf, PathBuf), OxenError> {
    let path = path.as_ref();
    match dunce::canonicalize(path) {
        Ok(path) => {
            if util::fs::file_exists_in_directory(&repo.path, &path) {
                // Path is in the repo, so we get the remote directory from the repo path
                let relative_to_repo = util::fs::path_relative_to_dir(&path, &repo.path)?;
                let remote_directory = relative_to_repo
                    .parent()
                    .ok_or_else(|| OxenError::file_has_no_parent(&path))?;
                Ok((remote_directory.to_path_buf(), path))
            } else if opts.directory.is_some() {
                // We have to get the remote directory from the opts
                Ok((opts.directory.clone().unwrap(), path))
            } else {
                return Err(OxenError::remote_add_file_not_in_repo(path));
            }
        }
        Err(err) => {
            log::error!("Err: {err:?}");
            Err(OxenError::entry_does_not_exist(path))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::model::staged_data::StagedDataOpts;
    use crate::model::ContentType;
    use crate::opts::DFOpts;
    use crate::test;
    use polars::prelude::AnyValue;

    // #[tokio::test]
    // async fn test_remote_stage_add_row_commit_clears_remote_status() -> Result<(), OxenError> {
    //     test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
    //         let remote_repo_copy = remote_repo.clone();

    //         test::run_empty_dir_test_async(|repo_dir| async move {
    //             let repo_dir = repo_dir.join("new_repo");
    //             let cloned_repo =
    //                 command::shallow_clone_url(&remote_repo.remote.url, &repo_dir).await?;

    //             // Remote add row
    //             let path = test::test_nlp_classification_csv();
    //             let mut opts = DFOpts::empty();
    //             opts.add_row = Some("I am a new row,neutral".to_string());
    //             opts.content_type = ContentType::Csv;
    //             command::remote::df(&cloned_repo, path, opts).await?;

    //             // Make sure it is listed as modified
    //             let branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
    //             let directory = Path::new("");
    //             let opts = StagedDataOpts {
    //                 is_remote: true,
    //                 ..Default::default()
    //             };
    //             let status =
    //                 command::remote::status(&remote_repo, &branch, directory, &opts).await?;
    //             assert_eq!(status.staged_files.len(), 1);

    //             // Commit it
    //             command::remote::commit(&cloned_repo, "Remotely committing").await?;

    //             // Now status should be empty
    //             let status =
    //                 command::remote::status(&remote_repo, &branch, directory, &opts).await?;
    //             assert_eq!(status.staged_files.len(), 0);

    //             Ok(repo_dir)
    //         })
    //         .await?;

    //         Ok(remote_repo_copy)
    //     })
    //     .await
    // }

    #[tokio::test]
    async fn test_remote_stage_delete_row_clears_remote_status() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|repo_dir| async move {
                let repo_dir = repo_dir.join("new_repo");

                let cloned_repo =
                    command::shallow_clone_url(&remote_repo.remote.url, &repo_dir).await?;

                // Remote add row
                let path = test::test_nlp_classification_csv();
                let mut opts = DFOpts::empty();
                opts.add_row = Some("I am a new row,neutral".to_string());
                opts.content_type = ContentType::Csv;
                // Grab ID from the row we just added
                let df = command::remote::df(&cloned_repo, &path, opts).await?;
                let uuid = match df.get(0).unwrap().first().unwrap() {
                    AnyValue::String(s) => s.to_string(),
                    _ => panic!("Expected string"),
                };

                // Make sure it is listed as modified
                let branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
                let directory = Path::new("");
                let opts = StagedDataOpts {
                    is_remote: true,
                    ..Default::default()
                };
                let status =
                    command::remote::status(&remote_repo, &branch, directory, &opts).await?;
                assert_eq!(status.staged_files.len(), 1);

                // Delete it
                let mut delete_opts = DFOpts::empty();
                delete_opts.delete_row = Some(uuid);
                command::remote::df(&cloned_repo, &path, delete_opts).await?;

                // Now status should be empty
                let status =
                    command::remote::status(&remote_repo, &branch, directory, &opts).await?;
                assert_eq!(status.staged_files.len(), 0);

                Ok(repo_dir)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
