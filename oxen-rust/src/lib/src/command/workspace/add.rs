//! # oxen workspace add
//!
//! Stage a file on a remote repository branch
//!

use std::path::{Path, PathBuf};

use crate::api;
use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core::index::oxenignore;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteBranch};
use crate::opts::AddOpts;
use crate::util;

pub async fn add(
    repo: &LocalRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    opts: &AddOpts,
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();
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

    log::debug!("command::workspace::add Resolved path: {:?}", resolved_path);
    log::debug!(
        "command::workspace::add Remote directory: {:?}",
        remote_directory
    );
    log::debug!(
        "command::workspace::add Directory name: {:?}",
        directory_name
    );

    let result = api::remote::workspaces::files::add(
        &remote_repo,
        workspace_id,
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
                return Err(OxenError::workspace_add_file_not_in_repo(path));
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

    use crate::constants::{DEFAULT_BRANCH_NAME, OXEN_ID_COL};
    use crate::error::OxenError;
    use crate::model::staged_data::StagedDataOpts;
    use crate::opts::DFOpts;
    use crate::test;
    use crate::{api, command};
    use polars::prelude::AnyValue;

    #[tokio::test]
    async fn test_remote_stage_delete_row_clears_remote_status() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        };
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|repo_dir| async move {
                let repo_dir = repo_dir.join("new_repo");

                let cloned_repo =
                    command::shallow_clone_url(&remote_repo.remote.url, &repo_dir).await?;

                // Remote add row
                let path = test::test_nlp_classification_csv();

                // Index dataset
                let workspace_id = "my_workspace";
                api::remote::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
                api::remote::workspaces::data_frames::index(&remote_repo, workspace_id, &path)
                    .await?;

                let mut opts = DFOpts::empty();
                opts.add_row =
                    Some("{\"text\": \"I am a new row\", \"label\": \"neutral\"}".to_string());
                // Grab ID from the row we just added
                let df = command::workspace::df(&cloned_repo, workspace_id, &path, opts).await?;
                let uuid = match df.column(OXEN_ID_COL).unwrap().get(0).unwrap() {
                    AnyValue::String(s) => s.to_string(),
                    _ => panic!("Expected string"),
                };

                // Make sure it is listed as modified
                let directory = Path::new("");
                let opts = StagedDataOpts {
                    is_remote: true,
                    ..Default::default()
                };
                let status =
                    command::workspace::status(&remote_repo, workspace_id, directory, &opts)
                        .await?;
                assert_eq!(status.staged_files.len(), 1);

                // Delete it
                let mut delete_opts = DFOpts::empty();
                delete_opts.delete_row = Some(uuid);
                command::workspace::df(&cloned_repo, workspace_id, &path, delete_opts).await?;

                // Now status should be empty
                let status =
                    command::workspace::status(&remote_repo, workspace_id, directory, &opts)
                        .await?;
                assert_eq!(status.staged_files.len(), 0);

                Ok(repo_dir)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
