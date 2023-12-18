//! Based on: <https://git-scm.com/docs/git-rm>
//! Remove files matching pathspec from the index, or from the working tree and the index.
//! `oxen rm` will not remove a file from just your working directory.
//! (There is no option to remove a file only from the working tree and yet keep it in the index; use /bin/rm if you want to do that.)
//! When --staged is given, the staged content has to match either the tip of the branch or the file on disk,
//! allowing the file to be removed from just the index.

use crate::api;
use crate::command;
use crate::config::UserConfig;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::util;

use super::CommitEntryReader;
use super::Stager;

use pluralizer::pluralize;
use std::convert::TryInto;
use std::path::Path;
use std::path::PathBuf;

pub async fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    if opts.remote {
        return remove_remote(repo, opts).await;
    }

    // Check if it is a directory and -r was provided
    let path = &opts.path;
    let dir_exists = dir_is_staged_or_committed(repo, path)?;

    log::debug!("got dir_exists: {:?} for path {:?}", dir_exists, path);

    if dir_exists && opts.recursive {
        return rm_dir(repo, opts).await;
    }

    // Error if is a directory and -r was not provided
    if dir_exists && !opts.recursive {
        let error = format!("`oxen rm` on directory {path:?} requires -r");
        return Err(OxenError::basic_str(error));
    }

    rm_file(repo, opts)
}

async fn rm_dir(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();
    if opts.staged {
        return remove_staged(repo, opts);
    }

    // We can only use `oxen rm` on directories that are committed
    if !dir_is_committed(repo, path)? {
        let error = format!("Directory {path:?} does not match any committed directories.");
        return Err(OxenError::basic_str(error));
    }

    // Make sure there are no modified files in directory
    let modifications = list_modified_files_in_dir(repo, path)?;
    if !modifications.is_empty() {
        let num_mods: isize = modifications.len().try_into().unwrap(); // should always be safe to go from usize -> isize
        let error = format!("There are {} with modifications within {path:?}\n\tUse `oxen status` to see the modified files.", pluralize("file", num_mods, true));
        return Err(OxenError::basic_str(error));
    }

    // Remove the directory from disk
    let full_path = repo.path.join(path);
    log::debug!("REMOVING DIRECTORY: {full_path:?}");
    if full_path.exists() {
        // user might have removed dir manually before using `oxen rm`
        util::fs::remove_dir_all(&full_path)?;
    }

    // Stage all the removed files
    command::add(repo, &full_path)?;

    Ok(())
}

fn list_modified_files_in_dir(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Vec<PathBuf>, OxenError> {
    let status = command::status(repo)?;
    let modified: Vec<PathBuf> = status
        .modified_files
        .into_iter()
        .filter(|p| p.starts_with(path))
        .collect();
    Ok(modified)
}

fn dir_is_staged_or_committed(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    Ok(dir_is_staged(repo, path)? || dir_is_committed(repo, path)?)
}

fn dir_is_staged(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    let stager = Stager::new(repo)?;
    Ok(stager.has_staged_dir(path))
}

fn dir_is_committed(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    let commit = api::local::commits::head_commit(repo)?;
    let commit_reader = CommitEntryReader::new(repo, &commit)?;
    Ok(commit_reader.has_dir(path))
}

fn file_is_committed(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    let commit = api::local::commits::head_commit(repo)?;
    let commit_reader = CommitEntryReader::new(repo, &commit)?;

    let all_files = commit_reader.list_files()?;
    // Print out all the files 
    for file in all_files {
        log::debug!("list files found file file: {:?}", file);
    }

    log::debug!("looking to remove file {:?}", path);

    let has_file = commit_reader.has_file(path);

    if has_file == false {
        log::debug!("uh oh file {:?} not found", path);
    } 

    Ok(commit_reader.has_file(path))
}

fn rm_file(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();
    if opts.staged {
        return remove_staged_file(repo, path);
    }

    if !file_is_committed(repo, path)? {
        let error = format!("File {path:?} must be committed to use `oxen rm`");
        return Err(OxenError::basic_str(error));
    }

    // Remove file from disk
    let full_path = repo.path.join(path);
    log::debug!("REMOVING FILE: {full_path:?}");
    if full_path.exists() {
        // user might have removed file manually before using `oxen rm`
        util::fs::remove_file(&full_path)?;
    }

    // Stage the removed file
    command::add(repo, &full_path)?;

    Ok(())
}

async fn remove_remote(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();

    if opts.recursive {
        Err(OxenError::basic_str(
            "`oxen remote rm` does not support removing directories yet",
        ))
    } else {
        remove_remote_staged_file(repo, path).await
    }
}

async fn remove_remote_staged_file(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let branch = api::local::branches::current_branch(repo)?.expect("Must be on branch.");
    let branch_name = branch.name;
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let user_id = UserConfig::identifier()?;
    api::remote::staging::rm_file(&remote_repo, &branch_name, &user_id, path.to_path_buf()).await
}

fn remove_staged(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();

    if opts.recursive {
        remove_staged_dir(repo, path)
    } else {
        remove_staged_file(repo, path)
    }
}

fn remove_staged_file(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    stager.remove_staged_file(path)
}

fn remove_staged_dir(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    stager.remove_staged_dir(path)
}

// unit tests
#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::command;
    use crate::core::index::rm;
    use crate::core::index::CommitEntryReader;
    use crate::error::OxenError;
    use crate::model::StagedEntryStatus;
    use crate::opts::RmOpts;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_rm_staged_file() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("README", |repo| async move {
            // Stage the README.md file
            let path = Path::new("README.md");
            command::add(&repo, repo.path.join(path))?;

            let status = command::status(&repo)?;
            assert_eq!(status.staged_files.len(), 1);
            assert!(status.staged_files.contains_key(path));

            let opts = RmOpts::from_staged_path(path);
            rm::rm(&repo, &opts).await?;

            let status = command::status(&repo)?;
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_dir_without_recursive_flag_should_be_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Stage the data
            let path = Path::new("train");
            command::add(&repo, repo.path.join(path))?;

            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.staged_dirs.len(), 1);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: false, // This should be an error
                remote: false,
            };
            let result = rm::rm(&repo, &opts).await;
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_dir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Stage the data
            let path = Path::new("train");
            command::add(&repo, repo.path.join(path))?;

            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.staged_dirs.len(), 1);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: true, // make sure to pass in recursive
                remote: false,
            };
            rm::rm(&repo, &opts).await?;

            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.staged_dirs.len(), 0);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_dir_with_slash() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Stage the data
            let path = Path::new("train/");
            command::add(&repo, repo.path.join(path))?;

            let status = command::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 1);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: true, // make sure to pass in recursive
                remote: false,
            };
            let result = rm::rm(&repo, &opts).await;
            assert!(result.is_ok());

            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.staged_dirs.len(), 0);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_staged_rm_file() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("README", |repo| async move {
            // Remove the readme
            let path = Path::new("README.md");

            let opts = RmOpts::from_path(path);
            rm::rm(&repo, &opts).await?;

            let status = command::status(&repo)?;
            status.print_stdout();

            assert_eq!(status.staged_files.len(), 1);
            assert_eq!(
                status.staged_files.get(path).unwrap().status,
                StagedEntryStatus::Removed
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_without_recursive_flag_should_be_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Remove the train dir
            let path = Path::new("train");

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: false,
                recursive: false, // This should be an error
                remote: false,
            };

            let result = rm::rm(&repo, &opts).await;
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_that_is_not_committed_should_throw_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // The train dir is not committed, so should get an error trying to remove
            let train_dir = Path::new("train");

            let opts = RmOpts {
                path: train_dir.to_path_buf(),
                staged: false,
                recursive: true, // Need to specify recursive
                remote: false,
            };

            let result = rm::rm(&repo, &opts).await;
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_with_modifications_should_throw_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("train", |repo| async move {
            // Remove the train dir
            let train_dir = Path::new("train");

            let opts = RmOpts {
                path: train_dir.to_path_buf(),
                staged: false,
                recursive: true, // Need to specify recursive
                remote: false,
            };

            // copy a cat into the dog image
            util::fs::copy(
                Path::new("data/test/images/cat_1.jpg"),
                repo.path.join(train_dir.join("dog_1.jpg")),
            )?;

            // There should be one modified file
            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.modified_files.len(), 1);

            let result = rm::rm(&repo, &opts).await;
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("train", |repo| async move {
            // Remove the train dir
            let path = Path::new("train");

            let og_num_files = util::fs::rcount_files_in_dir(&repo.path.join(path));

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: false,
                recursive: true, // Must pass in recursive = true
                remote: false,
            };
            rm::rm(&repo, &opts).await?;

            let status = command::status(&repo)?;
            status.print_stdout();

            assert_eq!(status.staged_files.len(), og_num_files);
            for (_, staged_entry) in status.staged_files.iter() {
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            // commit the removal
            let commit = command::commit(&repo, "removed train dir")?;

            // make sure the train dir is deleted from the commits db
            let commit_reader = CommitEntryReader::new(&repo, &commit)?;
            assert!(!commit_reader.has_dir(path));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_with_slash() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("train", |repo| async move {
            // Remove the train dir
            let path = Path::new("train/");

            let og_num_files = util::fs::rcount_files_in_dir(&repo.path.join(path));

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: false,
                recursive: true, // Must pass in recursive = true
                remote: false,
            };
            rm::rm(&repo, &opts).await?;

            let status = command::status(&repo)?;
            status.print_stdout();

            assert_eq!(status.staged_files.len(), og_num_files);
            for (_, staged_entry) in status.staged_files.iter() {
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_subdir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("annotations", |repo| async move {
            // Remove the annotations/train subdir
            let path = Path::new("annotations").join("train");
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path.join(&path));

            let opts = RmOpts {
                path,
                staged: false,
                recursive: true, // Must pass in recursive = true
                remote: false,
            };
            rm::rm(&repo, &opts).await?;

            let status = command::status(&repo)?;
            status.print_stdout();

            assert_eq!(status.staged_files.len(), og_num_files);
            for (_, staged_entry) in status.staged_files.iter() {
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            Ok(())
        })
        .await
    }
}
