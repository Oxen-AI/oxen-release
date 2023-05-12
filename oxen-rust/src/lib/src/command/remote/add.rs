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
    match std::fs::canonicalize(path) {
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
