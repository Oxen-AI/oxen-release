use crate::command;
use crate::error::OxenError;
/// Based on: https://git-scm.com/docs/git-rm
///
/// Remove files matching pathspec from the index, or from the working tree and the index.
/// `oxen rm` will not remove a file from just your working directory.
/// (There is no option to remove a file only from the working tree and yet keep it in the index; use /bin/rm if you want to do that.)
/// The files being removed have to be identical to the tip of the branch,
/// and no updates to their contents can be staged in the index,
/// though that default behavior can be overridden with the -f option.
/// When --cached is given, the staged content has to match either the tip of the branch or the file on disk,
/// allowing the file to be removed from just the index.
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::util;

use std::path::Path;

use super::CommitDirReader;
use super::Stager;

pub fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    // RmOpts supports --cached, --force, and --recursive

    // Check if it is a directory and -r was provided
    let full_path = repo.path.join(&opts.path);
    if full_path.is_dir() && opts.recursive {
        return rm_dir(repo, opts);
    }

    // Error if is a directory and -r was not provided
    if full_path.is_dir() && !opts.recursive {
        let error = format!("Not removing {full_path:?} recursively without -r");
        return Err(OxenError::basic_str(error));
    }

    rm_file(repo, opts)
}

fn rm_dir(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();
    if opts.staged {
        return remove_staged_dir(repo, path);
    }

    Ok(())
}

fn rm_file(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();
    if opts.staged {
        return remove_staged_file(repo, path);
    }

    if !file_contents_matches_head(repo, path)? {
        let error = format!("File {path:?} does not match HEAD commit");
        return Err(OxenError::basic_str(error));
    }

    let full_path = repo.path.join(path);
    log::debug!("REMOVING FILE: {full_path:?}");
    std::fs::remove_file(full_path)?;

    Ok(())
}

fn remove_staged_file(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    stager.remove_staged_file(path)
}

fn remove_staged_dir(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    stager.remove_staged_dir(path)
}

fn file_contents_matches_head(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    let commit = command::head_commit(repo)?;
    let commit_reader = CommitDirReader::new(repo, &commit)?;
    match commit_reader.get_entry(path) {
        Ok(Some(entry)) => {
            let full_path = repo.path.join(path);
            let hash = util::hasher::hash_file_contents(&full_path)?;
            log::debug!(
                "file_contents_matches_head File {path:?} {hash:?} == {:?}",
                entry.hash
            );
            Ok(entry.hash == hash)
        }
        Ok(None) => {
            log::warn!("File {path:?} does not exist in HEAD commit");
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

// unit tests
#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::command;
    use crate::error::OxenError;
    use crate::index::rm;
    use crate::opts::RmOpts;
    use crate::test;

    #[test]
    fn test_rm_staged_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Stage the README.md file
            let path = Path::new("README.md");
            command::add(&repo, repo.path.join(path))?;

            let status = command::status(&repo)?;
            assert_eq!(status.added_files.len(), 1);
            assert!(status.added_files.contains_key(path));

            let opts = RmOpts::from_staged_path(path);
            rm::rm(&repo, &opts)?;

            let status = command::status(&repo)?;
            assert_eq!(status.added_files.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_rm_staged_dir_without_recursive_flag_should_be_error() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Stage the README.md file
            let path = Path::new("train");
            command::add(&repo, repo.path.join(path))?;

            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.added_dirs.len(), 1);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                force: false,
                recursive: false, // This should be an error
            };
            let result = rm::rm(&repo, &opts);
            assert!(result.is_err());

            Ok(())
        })
    }

    #[test]
    fn test_rm_staged_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Stage the README.md file
            let path = Path::new("train");
            command::add(&repo, repo.path.join(path))?;

            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.added_dirs.len(), 1);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                force: false,
                recursive: true, // make sure to pass in recursive
            };
            rm::rm(&repo, &opts)?;

            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.added_dirs.len(), 0);
            assert_eq!(status.added_files.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_rm_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Remove the readme
            let path = Path::new("README.md");

            let opts = RmOpts::from_path(path);
            rm::rm(&repo, &opts)?;

            let status = command::status(&repo)?;
            status.print_stdout();

            assert_eq!(status.removed_files.len(), 1);
            assert_eq!(status.removed_files.first().unwrap(), path);

            Ok(())
        })
    }

    #[test]
    fn test_rm_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Remove the readme
            let path = Path::new("train");

            let opts = RmOpts::from_path(path);
            rm::rm(&repo, &opts)?;

            let status = command::status(&repo)?;
            status.print_stdout();

            assert_eq!(status.removed_files.len(), 1);
            assert_eq!(status.removed_files.first().unwrap(), path);

            Ok(())
        })
    }

    #[test]
    fn test_rm_subdir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Remove the readme
            let path = Path::new("annotations").join("train");

            let opts = RmOpts::from_path(path);
            rm::rm(&repo, &opts)?;

            // TODO

            Ok(())
        })
    }
}
