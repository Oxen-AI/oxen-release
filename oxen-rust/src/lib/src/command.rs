//! # Oxen Commands
//!
//! Top level commands you are likely to run on an Oxen repository
//!

use crate::error::OxenError;
use crate::index::{Committer, Stager};
use crate::model::{Commit, LocalRepository, RepoStatus};
use crate::util;

use std::path::Path;

pub const NO_REPO_MSG: &str = "fatal: no oxen repository exists, looking for directory: .oxen ";

/// # Initialize an Empty Oxen Repository
///
/// ```
/// # use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
///
/// let base_dir = Path::new("/tmp/repo_dir");
/// command::init(base_dir)?;
/// assert!(base_dir.join(".oxen").exists());
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn init(path: &Path) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);
    std::fs::create_dir_all(hidden_dir)?;
    let config_path = util::fs::config_filepath(path);
    let repo = LocalRepository::new(path)?;
    repo.save(&config_path)?;
    Ok(repo)
}

/// # Get status of files in repository
///
/// What files are tracked, added, untracked, etc
///
/// Empty Repository:
///
/// ```
/// use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
///
/// let base_dir = Path::new("/tmp/repo_dir");
/// // Initialize empty repo
/// let repo = command::init(&base_dir)?;
/// // Get status on repo
/// let status = command::status(&repo)?;
/// assert!(status.is_clean());
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
///
/// Repository with files
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
///
/// let base_dir = Path::new("/tmp/repo_dir");
/// // Initialize empty repo
/// let repo = command::init(&base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Get status on repo
/// let status = command::status(&repo)?;
/// assert_eq!(status.untracked_files.len(), 1);
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn status(repository: &LocalRepository) -> Result<RepoStatus, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(&repository.path);
    if !hidden_dir.exists() {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let committer = Committer::new(repository)?;
    let stager = Stager::new(repository)?;

    let added_dirs = stager.list_added_directories()?;
    let added_files = stager.list_added_files()?;
    let untracked_dirs = stager.list_untracked_directories(&committer)?;
    let untracked_files = stager.list_untracked_files(&committer)?;
    let status = RepoStatus {
        added_dirs,
        added_files,
        untracked_dirs,
        untracked_files,
    };
    Ok(status)
}

/// # Get status of files in repository
pub fn add(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    let committer = Committer::new(repo)?;
    stager.add(path, &committer)?;
    Ok(())
}

/// # Commit the staged files in the repo
pub fn commit(repo: &LocalRepository, message: &str) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    let mut committer = Committer::new(repo)?;
    let added_files = stager.list_added_files()?;
    let added_dirs = stager.list_added_directories()?;

    committer.commit(&added_files, &added_dirs, message)?;
    stager.unstage()?;
    Ok(())
}

/// # Get a log of all the commits
pub fn log(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    let committer = Committer::new(repo)?;
    let commits = committer.list_commits()?;
    Ok(commits)
}

/// # Create a new branch
/// This creates a new pointer to the current commit with a name
/// It does not switch you to this branch, you still must call `checkout_branch`
pub fn create_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    let committer = Committer::new(repo)?;
    match committer.get_head_commit() {
        Ok(Some(head_commit)) => {
            committer.referencer.create_branch(name, &head_commit.id)?;
            Ok(())
        }
        _ => Err(OxenError::basic_str(
            "Err: No Commits. Cannot create a branch until you make your initial commit.",
        )),
    }
}

/// # Checkout a branch
/// This switches HEAD to point to the branch name
/// It also updates all the local files to be from the commit that this branch references
pub fn checkout_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    let committer = Committer::new(repo)?;
    if committer.referencer.has_branch(name) {
        committer.referencer.set_head(name)?;
        Ok(())
    } else {
        let err = format!("Branch not found {}", name);
        Err(OxenError::basic_str(&err))
    }
}

/// # Create a branch and check it out in one go
/// This creates a branch with name
/// Then switches HEAD to point to the branch
pub fn create_checkout_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    let committer = Committer::new(repo)?;
    match committer.get_head_commit() {
        Ok(Some(head_commit)) => {
            committer.referencer.create_branch(name, &head_commit.id)?;
            committer.referencer.set_head(name)?;
            Ok(())
        }
        _ => Err(OxenError::basic_str(
            "Err: No Commits. Cannot create a branch until you make your initial commit.",
        )),
    }
}

#[cfg(test)]
mod tests {

    use crate::command;
    use crate::error::OxenError;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_init() -> Result<(), OxenError> {
        test::run_empty_repo_dir_test(|repo_dir| {
            // Init repo
            let repository = command::init(repo_dir)?;

            // Init should create the .oxen directory
            let hidden_dir = util::fs::oxen_hidden_dir(repo_dir);
            let config_file = util::fs::config_filepath(repo_dir);
            assert!(hidden_dir.exists());
            assert!(config_file.exists());
            // Name and id will be random but should be populated
            assert!(!repository.id.is_empty());
            assert!(!repository.name.is_empty());

            Ok(())
        })
    }

    #[test]
    fn test_command_status_empty() -> Result<(), OxenError> {
        test::run_empty_repo_test(|repo| {
            let repo_status = command::status(&repo)?;

            assert_eq!(repo_status.added_dirs.len(), 0);
            assert_eq!(repo_status.added_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_command_status_has_txt_file() -> Result<(), OxenError> {
        test::run_empty_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World");

            // Get status
            let repo_status = command::status(&repo)?;
            assert_eq!(repo_status.added_dirs.len(), 0);
            assert_eq!(repo_status.added_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 1);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_command_add_file() -> Result<(), OxenError> {
        test::run_empty_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World");

            // Track the file
            command::add(&repo, &hello_file)?;
            // Get status and make sure it is removed from the untracked, and added to the tracked
            let repo_status = command::status(&repo)?;
            assert_eq!(repo_status.added_dirs.len(), 0);
            assert_eq!(repo_status.added_files.len(), 1);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_command_commit_file() -> Result<(), OxenError> {
        test::run_empty_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World");

            // Track the file
            command::add(&repo, &hello_file)?;
            // Commit the file
            command::commit(&repo, "My message")?;

            // Get status and make sure it is removed from the untracked and added
            let repo_status = command::status(&repo)?;
            assert_eq!(repo_status.added_dirs.len(), 0);
            assert_eq!(repo_status.added_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            let commits = command::log(&repo)?;
            assert_eq!(commits.len(), 1);

            Ok(())
        })
    }
}
