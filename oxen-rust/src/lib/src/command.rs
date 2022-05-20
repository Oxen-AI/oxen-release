//! # Oxen Commands
//!
//! Top level commands you are likely to run on an Oxen repository
//!

use crate::constants::NO_REPO_MSG;
use crate::error::OxenError;
use crate::index::{Committer, Indexer, Referencer, Stager};
use crate::model::{Branch, Commit, LocalRepository, RemoteRepository, StagedData};
use crate::util;

use rocksdb::{IteratorMode, LogLevel, Options, DB};
use std::path::Path;
use std::str;

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

    if let Ok(commit) = commit_with_no_files(&repo, "Initialized Repo 🐂") {
        println!("Initial commit {}", commit.id);
    }

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
pub fn status(repository: &LocalRepository) -> Result<StagedData, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(&repository.path);
    if !hidden_dir.exists() {
        return Err(OxenError::basic_str(NO_REPO_MSG));
    }

    let committer = Committer::new(repository)?;
    let stager = Stager::new(repository)?;
    let status = stager.status(&committer)?;
    Ok(status)
}

/// # Get status of files in repository
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
///
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir");
/// let repo = command::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// command::add(&repo, &hello_file)?;
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn add(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    let committer = Committer::new(repo)?;
    stager.add(path, &committer)?;
    Ok(())
}

/// # Commit the staged files in the repo
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
///
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir");
/// let repo = command::init(base_dir)?;
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
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn commit(repo: &LocalRepository, message: &str) -> Result<Option<Commit>, OxenError> {
    let status = status(repo)?;
    if status.is_clean() {
        return Ok(None);
    }
    let commit = p_commit(repo, &status, message)?;
    Ok(Some(commit))
}

fn commit_with_no_files(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    let status = status(repo)?;
    let commit = p_commit(repo, &status, message)?;
    Ok(commit)
}

fn p_commit(
    repo: &LocalRepository,
    status: &StagedData,
    message: &str,
) -> Result<Commit, OxenError> {
    let stager = Stager::new(repo)?;
    let mut committer = Committer::new(repo)?;
    let commit = committer.commit(status, message)?;
    stager.unstage()?;
    Ok(commit)
}

/// # Get a log of all the commits
///
/// ```
/// use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
///
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir");
/// let repo = command::init(base_dir)?;
///
/// // Print     commit history
/// let history = command::log(&repo)?;
/// for commit in history.iter() {
///   println!("{} {}", commit.id, commit.message);
/// }
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn log(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    let committer = Committer::new(repo)?;
    let commits = committer.list_commits()?;
    Ok(commits)
}

/// # Create a new branch
/// This creates a new pointer to the current commit with a name,
/// it does not switch you to this branch, you still must call `checkout_branch`
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

/// # Checkout a branch or commit id
/// This switches HEAD to point to the branch name or commit id,
/// it also updates all the local files to be from the commit that this branch references
pub fn checkout(repo: &LocalRepository, value: &str) -> Result<(), OxenError> {
    let committer = Committer::new(repo)?;
    if committer.referencer.has_branch(value) {
        if let Some(current_branch) = committer.referencer.get_current_branch()? {
            // If we are already on the branch, do nothing
            if current_branch.name == value {
                eprintln!("Already on branch {}", value);
                return Ok(());
            }
        }

        println!("checkout branch: {}", value);
        committer.set_working_repo_to_branch(value)?;
    } else {
        let current_commit_id = committer.referencer.head_commit_id()?;
        // If we are already on the commit, do nothing
        if current_commit_id == value {
            eprintln!("Commit already checked out {}", value);
            return Ok(());
        }

        committer.set_working_repo_to_commit_id(value)?;
    }
    committer.referencer.set_head(value);
    Ok(())
}

/// # Create a branch and check it out in one go
/// This creates a branch with name,
/// then switches HEAD to point to the branch
pub fn create_checkout_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    println!("create and checkout branch: {}", name);
    let committer = Committer::new(repo)?;
    match committer.get_head_commit() {
        Ok(Some(head_commit)) => {
            committer.referencer.create_branch(name, &head_commit.id)?;
            committer.referencer.set_head(name);
            Ok(())
        }
        _ => Err(OxenError::basic_str(
            "Err: No Commits. Cannot create a branch until you make your initial commit.",
        )),
    }
}

/// # List branches
pub fn list_branches(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    let referencer = Referencer::new(repo)?;
    let branches = referencer.list_branches()?;
    Ok(branches)
}

/// # Get the current branch
pub fn current_branch(repo: &LocalRepository) -> Result<Option<Branch>, OxenError> {
    let referencer = Referencer::new(repo)?;
    let branch = referencer.get_current_branch()?;
    Ok(branch)
}

/// # Get the current commit
pub fn head_commit(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    let committer = Committer::new(repo)?;
    let commit = committer.get_head_commit()?;
    Ok(commit)
}

/// # Set the remote for a repository
/// Tells the CLI where to push the changes to
pub fn set_remote(repo: &mut LocalRepository, name: &str, url: &str) -> Result<(), OxenError> {
    repo.set_remote(name, url);
    repo.save_default()?;
    Ok(())
}

/// # Get a log of all the commits
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
///
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir");
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
/// command::set_remote(&mut repo, "origin", "http://hub.oxen.ai/repositories/hello");
///
/// // Push the file
/// command::push(&repo);
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn push(repo: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let indexer = Indexer::new(repo)?;
    let committer = Committer::new(repo)?;

    indexer.push(&committer)
}

/// Clone a repo from a url to a directory
pub fn clone(url: &str, dst: &Path) -> Result<LocalRepository, OxenError> {
    LocalRepository::clone_remote(url, dst)
}

/// Pull a repository's data
pub fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = Indexer::new(repo)?;
    indexer.pull()?;
    Ok(())
}

/// Inspect a key value database for debugging
pub fn inspect(path: &Path) -> Result<(), OxenError> {
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);
    let db = DB::open_for_read_only(&opts, path, false)?;
    let iter = db.iterator(IteratorMode::Start);
    for (key, value) in iter {
        if let (Ok(key), Ok(value)) = (str::from_utf8(&key), str::from_utf8(&value)) {
            println!("{}\t{}", key, value)
        }
    }
    Ok(())
}
