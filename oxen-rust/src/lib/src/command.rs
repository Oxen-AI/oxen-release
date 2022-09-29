//! # Oxen Commands
//!
//! Top level commands you are likely to run on an Oxen repository
//!

use crate::api;
use crate::constants;
use crate::error::OxenError;
use crate::index::{
    CommitDirReader, CommitReader, CommitWriter, Indexer, Merger, RefReader, RefWriter, Stager,
};
use crate::model::{
    Branch, Commit, LocalRepository, RemoteBranch, RemoteRepository, RepositoryNew, StagedData,
};
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
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// let base_dir = Path::new("/tmp/repo_dir_init");
/// command::init(base_dir)?;
/// assert!(base_dir.join(".oxen").exists());
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn init(path: &Path) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);
    if hidden_dir.exists() {
        let err = format!("Oxen repository already exists: {:?}", path);
        return Err(OxenError::basic_str(err));
    }

    // Cleanup the .oxen dir if init fails
    match p_init(path) {
        Ok(result) => Ok(result),
        Err(error) => {
            std::fs::remove_dir_all(hidden_dir)?;
            Err(error)
        }
    }
}

fn p_init(path: &Path) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    std::fs::create_dir_all(hidden_dir)?;
    let config_path = util::fs::config_filepath(path);
    let repo = LocalRepository::new(path)?;
    repo.save(&config_path)?;

    let commit = commit_with_no_files(&repo, constants::INITIAL_COMMIT_MSG)?;
    println!("Initial commit {}", commit.id);

    // TODO: cleanup .oxen on failure

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
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// let base_dir = Path::new("/tmp/repo_dir_status_1");
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
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// let base_dir = Path::new("/tmp/repo_dir_status_2");
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
    log::debug!("status before new_from_head");
    let reader = CommitDirReader::new_from_head(repository)?;
    log::debug!("status before Stager::new");
    let stager = Stager::new(repository)?;
    log::debug!("status before stager.status");
    let status = stager.status(&reader)?;
    Ok(status)
}

/// # Get status of files in repository
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir_add");
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
pub fn add<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let stager = Stager::new_with_merge(repo)?;
    let commit = head_commit(repo)?;
    let reader = CommitDirReader::new(repo, &commit)?;
    stager.add(path.as_ref(), &reader)?;
    Ok(())
}

/// # Add tabular file to track row level changes
pub fn add_tabular<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let stager = Stager::new_with_merge(repo)?;
    let commit = head_commit(repo)?;
    let reader = CommitDirReader::new(repo, &commit)?;
    stager.add_tabular_file(path.as_ref(), &reader)?;
    Ok(())
}

/// # Commit the staged files in the repo
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::test;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir_commit");
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
    if !status.has_added_entries() {
        println!(
            "No files are staged, not committing. Stage a file or directory with `oxen add <file>`"
        );
        return Ok(None);
    }
    let commit = p_commit(repo, &status, message)?;
    Ok(Some(commit))
}

fn commit_with_no_files(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    let status = StagedData::empty();
    let commit = p_commit(repo, &status, message)?;
    Ok(commit)
}

fn p_commit(
    repo: &LocalRepository,
    status: &StagedData,
    message: &str,
) -> Result<Commit, OxenError> {
    let stager = Stager::new(repo)?;
    let commit_writer = CommitWriter::new(repo)?;
    let commit = commit_writer.commit(status, message)?;
    stager.unstage()?;
    Ok(commit)
}

/// # Get a log of all the commits
///
/// ```
/// use liboxen::command;
/// # use liboxen::test;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir_log");
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
    let committer = CommitReader::new(repo)?;
    let commits = committer.history_from_head()?;
    Ok(commits)
}

/// # Get the history for a specific branch or commit
pub fn log_commit_or_branch_history(
    repo: &LocalRepository,
    commit_or_branch: &str,
) -> Result<Vec<Commit>, OxenError> {
    let committer = CommitReader::new(repo)?;
    let commit_id = match get_branch_commit_id(repo, commit_or_branch)? {
        Some(branch_commit_id) => branch_commit_id,
        None => String::from(commit_or_branch),
    };

    log::debug!("log_commit_or_branch_history: commit_id: {}", commit_id);
    match committer.history_from_commit_id(&commit_id) {
        Ok(commits) => Ok(commits),
        Err(_) => Err(OxenError::local_commit_or_branch_not_found(
            commit_or_branch,
        )),
    }
}

/// # Create a new branch from the head commit
/// This creates a new pointer to the current commit with a name,
/// it does not switch you to this branch, you still must call `checkout_branch`
pub fn create_branch_from_head(repo: &LocalRepository, name: &str) -> Result<Branch, OxenError> {
    let ref_writer = RefWriter::new(repo)?;
    let commit_reader = CommitReader::new(repo)?;
    let head_commit = commit_reader.head_commit()?;
    ref_writer.create_branch(name, &head_commit.id)
}

/// # Create a local branch from a specific commit id
pub fn create_branch(
    repo: &LocalRepository,
    name: &str,
    commit_id: &str,
) -> Result<Branch, OxenError> {
    let ref_writer = RefWriter::new(repo)?;
    let commit_reader = CommitReader::new(repo)?;
    if commit_reader.commit_id_exists(commit_id) {
        ref_writer.create_branch(name, commit_id)
    } else {
        Err(OxenError::commit_id_does_not_exist(commit_id))
    }
}

/// # Delete a local branch
/// Checks to make sure the branch has been merged before deleting.
pub fn delete_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    api::local::branches::delete(repo, name)
}

/// # Delete a remote branch
pub fn delete_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch_name: &str,
) -> Result<(), OxenError> {
    if let Some(remote) = repo.get_remote(remote) {
        if let Some(remote_repo) = api::remote::repositories::get_by_remote_url(&remote.url)? {
            if let Some(branch) = api::remote::branches::get_by_name(&remote_repo, branch_name)? {
                api::remote::branches::delete(&remote_repo, &branch.name)?;
                Ok(())
            } else {
                Err(OxenError::remote_branch_not_found(branch_name))
            }
        } else {
            Err(OxenError::remote_repo_not_found(&remote.url))
        }
    } else {
        Err(OxenError::remote_not_set())
    }
}

/// # Force delete a local branch
/// Caution! Will delete a local branch without checking if it has been merged or pushed.
pub fn force_delete_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    api::local::branches::force_delete(repo, name)
}

/// # Checkout a branch or commit id
/// This switches HEAD to point to the branch name or commit id,
/// it also updates all the local files to be from the commit that this branch references
pub fn checkout<S: AsRef<str>>(repo: &LocalRepository, value: S) -> Result<(), OxenError> {
    let value = value.as_ref();
    log::debug!("--- CHECKOUT START {} ----", value);
    if branch_exists(repo, value) {
        if already_on_branch(repo, value) {
            println!("Already on branch {}", value);
            return Ok(());
        }

        println!("Checkout branch: {}", value);
        set_working_branch(repo, value)?;
        set_head(repo, value)?;
    } else {
        // If we are already on the commit, do nothing
        if already_on_commit(repo, value) {
            eprintln!("Commit already checked out {}", value);
            return Ok(());
        }

        println!("Checkout commit: {}", value);
        set_working_commit_id(repo, value)?;
        set_head(repo, value)?;
    }
    log::debug!("--- CHECKOUT END {} ----", value);
    Ok(())
}

fn set_working_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    let commit_writer = CommitWriter::new(repo)?;
    commit_writer.set_working_repo_to_branch(name)
}

fn set_working_commit_id(repo: &LocalRepository, commit_id: &str) -> Result<(), OxenError> {
    let commit_writer = CommitWriter::new(repo)?;
    commit_writer.set_working_repo_to_commit_id(commit_id)
}

fn set_head(repo: &LocalRepository, value: &str) -> Result<(), OxenError> {
    let ref_writer = RefWriter::new(repo)?;
    ref_writer.set_head(value);
    Ok(())
}

fn get_branch_commit_id(repo: &LocalRepository, name: &str) -> Result<Option<String>, OxenError> {
    match RefReader::new(repo) {
        Ok(ref_reader) => ref_reader.get_commit_id_for_branch(name),
        _ => Err(OxenError::basic_str("Could not read reference for repo.")),
    }
}

fn branch_exists(repo: &LocalRepository, name: &str) -> bool {
    match RefReader::new(repo) {
        Ok(ref_reader) => ref_reader.has_branch(name),
        _ => false,
    }
}

fn already_on_branch(repo: &LocalRepository, name: &str) -> bool {
    match RefReader::new(repo) {
        Ok(ref_reader) => {
            if let Ok(Some(current_branch)) = ref_reader.get_current_branch() {
                // If we are already on the branch, do nothing
                if current_branch.name == name {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

fn already_on_commit(repo: &LocalRepository, commit_id: &str) -> bool {
    match RefReader::new(repo) {
        Ok(ref_reader) => {
            if let Ok(Some(head_commit_id)) = ref_reader.head_commit_id() {
                // If we are already on the branch, do nothing
                if head_commit_id == commit_id {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// # Create a branch and check it out in one go
/// This creates a branch with name,
/// then switches HEAD to point to the branch
pub fn create_checkout_branch(repo: &LocalRepository, name: &str) -> Result<Branch, OxenError> {
    println!("Create and checkout branch: {}", name);
    let head_commit = head_commit(repo)?;
    let ref_writer = RefWriter::new(repo)?;

    let branch = ref_writer.create_branch(name, &head_commit.id)?;
    ref_writer.set_head(name);
    Ok(branch)
}

/// # Merge a branch into the current branch
/// Checks for simple fast forward merge, or if current branch has diverged from the merge branch
/// it will perform a 3 way merge
/// If there are conflicts, it will abort and show the conflicts to be resolved in the `status` command
pub fn merge<S: AsRef<str>>(
    repo: &LocalRepository,
    branch_name: S,
) -> Result<Option<Commit>, OxenError> {
    let branch_name = branch_name.as_ref();
    if branch_exists(repo, branch_name) {
        if let Some(branch) = current_branch(repo)? {
            let merger = Merger::new(repo)?;
            if let Some(commit) = merger.merge(branch_name)? {
                println!(
                    "Successfully merged `{}` into `{}`",
                    branch_name, branch.name
                );
                println!("HEAD -> {}", commit.id);
                Ok(Some(commit))
            } else {
                eprintln!("Automatic merge failed; fix conflicts and then commit the result.");
                Ok(None)
            }
        } else {
            Err(OxenError::basic_str(
                "Must be on a branch to perform a merge.",
            ))
        }
    } else {
        Err(OxenError::local_branch_not_found(branch_name))
    }
}

/// # List local branches
pub fn list_branches(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branches = ref_reader.list_branches()?;
    Ok(branches)
}

/// # List remote branches
pub fn list_remote_branches(
    repo: &LocalRepository,
    name: &str,
) -> Result<Vec<RemoteBranch>, OxenError> {
    let mut branches: Vec<RemoteBranch> = vec![];
    if let Some(remote) = repo.get_remote(name) {
        if let Some(remote_repo) = api::remote::repositories::get_by_remote_url(&remote.url)? {
            for branch in api::remote::branches::list(&remote_repo)? {
                branches.push(RemoteBranch {
                    remote: remote.name.clone(),
                    branch: branch.name.clone(),
                });
            }
        }
    }
    Ok(branches)
}

/// # Get the current branch
pub fn current_branch(repo: &LocalRepository) -> Result<Option<Branch>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branch = ref_reader.get_current_branch()?;
    Ok(branch)
}

/// # Get the current commit
pub fn root_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let committer = CommitReader::new(repo)?;
    let commit = committer.root_commit()?;
    Ok(commit)
}

/// # Get the current commit
pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let committer = CommitReader::new(repo)?;
    let commit = committer.head_commit()?;
    Ok(commit)
}

/// # Create a remote repository
/// Takes the current directory name, and creates a repository on the server we can sync to. Returns the remote URL.
pub fn create_remote(
    repo: &LocalRepository,
    namespace: &str,
    name: &str,
    host: &str,
) -> Result<RemoteRepository, OxenError> {
    api::remote::repositories::create(repo, namespace, name, host)
}

/// # Set the remote for a repository
/// Tells the CLI where to push the changes to
pub fn set_remote(
    repo: &mut LocalRepository,
    name: &str,
    url: &str,
) -> Result<RemoteRepository, OxenError> {
    repo.set_remote(name, url);
    repo.save_default()?;
    let repo = RepositoryNew::from_url(url)?;
    Ok(RemoteRepository::from_new(&repo, url))
}

/// # Remove the remote for a repository
/// If you added a remote you no longer want, can remove it by supplying the name
pub fn remove_remote(repo: &mut LocalRepository, name: &str) -> Result<(), OxenError> {
    repo.remove_remote(name);
    repo.save_default()?;
    Ok(())
}

/// # Get a log of all the commits
///
/// ```
/// # use liboxen::api;
/// # use liboxen::test;
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir_push");
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
/// command::set_remote(&mut repo, "origin", "http://0.0.0.0:3000/repositories/hello");
///
/// let remote_repo = command::create_remote(&repo, "repositories", "hello", "0.0.0.0:3000")?;
///
/// // Push the file
/// command::push(&repo);
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # api::remote::repositories::delete(&remote_repo)?;
/// # Ok(())
/// # }
/// ```
pub fn push(repo: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let indexer = Indexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer.push(&rb)
}

/// Push to a specific remote
pub fn push_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
) -> Result<RemoteRepository, OxenError> {
    let indexer = Indexer::new(repo)?;
    let rb = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch),
    };
    indexer.push(&rb)
}

/// Clone a repo from a url to a directory
pub fn clone(url: &str, dst: &Path) -> Result<LocalRepository, OxenError> {
    match LocalRepository::clone_remote(url, dst) {
        Ok(Some(repo)) => Ok(repo),
        Ok(None) => Err(OxenError::remote_repo_not_found(url)),
        Err(err) => Err(err),
    }
}

/// Pull a repository's data from origin/main
pub fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = Indexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer.pull(&rb)?;
    Ok(())
}

/// Pull a specific origin and branch
pub fn pull_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
) -> Result<(), OxenError> {
    let indexer = Indexer::new(repo)?;
    let rb = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch),
    };
    indexer.pull(&rb)?;
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
