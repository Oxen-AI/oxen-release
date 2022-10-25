//! # Oxen Commands
//!
//! Top level commands you are likely to run on an Oxen repository
//!

use crate::api;
use crate::constants;
use crate::error::OxenError;
use crate::index::differ;
use crate::index::schema_writer::SchemaWriter;
use crate::index::CommitSchemaRowIndex;
use crate::index::SchemaReader;
use crate::index::{
    CommitDirReader, CommitReader, CommitWriter, EntryIndexer, MergeConflictReader, Merger,
    RefReader, RefWriter, Stager,
};
use crate::media::{df_opts::DFOpts, tabular};
use crate::model::Schema;
use crate::model::{
    Branch, Commit, EntryType, LocalRepository, RemoteBranch, RemoteRepository, StagedData,
};

use crate::util;
use crate::util::resource;

use bytevec::ByteDecodable;
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

/// Similar to status but takes the starting directory to look from
pub fn status_from_dir(repository: &LocalRepository, dir: &Path) -> Result<StagedData, OxenError> {
    log::debug!("status before new_from_head");
    let reader = CommitDirReader::new_from_head(repository)?;
    log::debug!("status before Stager::new");
    let stager = Stager::new(repository)?;
    log::debug!("status before stager.status");
    let status = stager.status_from_dir(&reader, dir)?;
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
    let path = path.as_ref();
    if !path.is_file() {
        log::warn!("Could not find file {:?}", path);
        return Err(OxenError::basic_str(
            "Err: oxen add -d <path> must be valid file",
        ));
    }

    let stager = Stager::new_with_merge(repo)?;
    let commit = head_commit(repo)?;
    let reader = CommitDirReader::new(repo, &commit)?;
    stager.add_file_with_type(path.as_ref(), &reader, EntryType::Tabular)?;
    Ok(())
}

/// Interact with dataframes from CLI
pub fn df<P: AsRef<Path>>(input: P, opts: DFOpts) -> Result<(), OxenError> {
    let mut df = tabular::show_path(input, opts.clone())?;

    if let Some(output) = opts.output {
        println!("Writing {:?}", output);
        tabular::write_df(&mut df, output)?;
    }

    Ok(())
}

/// Read the saved off schemas for a commit id
pub fn schema_list(
    repo: &LocalRepository,
    commit_id: Option<&str>,
) -> Result<Vec<Schema>, OxenError> {
    if let Some(commit_id) = commit_id {
        if let Some(commit) = commit_from_branch_or_commit_id(repo, commit_id)? {
            let schema_reader = SchemaReader::new(repo, &commit.id)?;
            schema_reader.list_schemas()
        } else {
            Err(OxenError::commit_id_does_not_exist(commit_id))
        }
    } else {
        let head_commit = head_commit(repo)?;
        let schema_reader = SchemaReader::new(repo, &head_commit.id)?;
        schema_reader.list_schemas()
    }
}

pub fn schema_show(
    repo: &LocalRepository,
    commit_id: Option<&str>,
    name_or_hash: &str,
) -> Result<Option<Schema>, OxenError> {
    // The list of schemas should not be too long, so just filter right now
    let list: Vec<Schema> = schema_list(repo, commit_id)?
        .into_iter()
        .filter(|s| s.name == Some(String::from(name_or_hash)) || s.hash == *name_or_hash)
        .collect();
    if !list.is_empty() {
        Ok(Some(list.first().unwrap().clone()))
    } else {
        Ok(None)
    }
}

pub fn schema_name(
    repo: &LocalRepository,
    hash: &str,
    val: &str,
) -> Result<Option<Schema>, OxenError> {
    let head_commit = head_commit(repo)?;
    if let Some(mut schema) = schema_show(repo, Some(&head_commit.id), hash)? {
        let schema_writer = SchemaWriter::new(repo, &head_commit.id)?;
        schema.name = Some(String::from(val));
        let schema = schema_writer.update_schema(&schema)?;
        Ok(Some(schema))
    } else {
        Ok(None)
    }
}

fn commit_from_branch_or_commit_id<S: AsRef<str>>(
    repo: &LocalRepository,
    val: S,
) -> Result<Option<Commit>, OxenError> {
    let val = val.as_ref();
    let commit_reader = CommitReader::new(repo)?;
    if let Some(commit) = commit_reader.get_commit_by_id(val)? {
        return Ok(Some(commit));
    }

    let ref_reader = RefReader::new(repo)?;
    if let Some(branch) = ref_reader.get_branch_by_name(val)? {
        if let Some(commit) = commit_reader.get_commit_by_id(branch.commit_id)? {
            return Ok(Some(commit));
        }
    }

    Ok(None)
}

/// # Restore a removed file that was committed
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
/// let hello_name = "hello.txt";
/// let hello_path = base_dir.join(hello_name);
/// util::fs::write_to_path(&hello_path, "Hello World");
///
/// // Stage the file
/// command::add(&repo, &hello_path)?;
///
/// // Commit staged
/// let commit = command::commit(&repo, "My commit message")?.unwrap();
///
/// // Remove the file from disk
/// std::fs::remove_file(hello_path)?;
///
/// // Restore the file
/// command::restore(&repo, Some(&commit.id), hello_name)?;
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn restore<P: AsRef<Path>>(
    repo: &LocalRepository,
    commit_or_branch: Option<&str>,
    path: P,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    let commit = resource::get_commit_or_head(repo, commit_or_branch)?;
    let reader = CommitDirReader::new(repo, &commit)?;

    if let Some(entry) = reader.get_entry(path)? {
        if util::fs::is_tabular(&entry.path) {
            let schema_reader = SchemaReader::new(repo, &commit.id)?;
            if let Some(schema) = schema_reader.get_schema_for_file(&entry.path)? {
                let row_index_reader =
                    CommitSchemaRowIndex::new(repo, &commit.id, &schema, &entry.path)?;
                let mut df = row_index_reader.entry_df()?;
                log::debug!("Got subset! {}", df);
                let working_path = repo.path.join(path);
                log::debug!("Write to {:?}", working_path);
                tabular::write_df(&mut df, working_path)?;
            } else {
                log::error!(
                    "Could not restore tabular file, no schema found for file {:?}",
                    entry.path
                );
            }
        } else {
            // just copy data back over if !tabular
            let version_path = util::fs::version_path(repo, &entry);
            let working_path = repo.path.join(path);
            std::fs::copy(version_path, working_path)?;
        }

        println!("Restored file {:?}", path);
        Ok(())
    } else {
        let error = format!("Could not restore file: {:?}", path);
        Err(OxenError::basic_str(error))
    }
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
pub async fn delete_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch_name: &str,
) -> Result<(), OxenError> {
    if let Some(remote) = repo.get_remote(remote) {
        if let Some(remote_repo) = api::remote::repositories::get_by_remote(&remote).await? {
            if let Some(branch) =
                api::remote::branches::get_by_name(&remote_repo, branch_name).await?
            {
                api::remote::branches::delete(&remote_repo, &branch.name).await?;
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

/// # Checkout a file and take their changes
/// This overwrites the current file with the changes in the branch we are merging in
pub fn checkout_theirs<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let merger = MergeConflictReader::new(repo)?;
    let conflicts = merger.list_conflicts()?;

    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        // Lookup the file for the merge commit entry and copy it over
        restore(repo, Some(&conflict.merge_entry.commit_id), path)
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}

/// # Combine Conflicting Tabular Data Files
/// This overwrites the current file with the changes in their file
pub fn checkout_combine<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let merger = MergeConflictReader::new(repo)?;
    let conflicts = merger.list_conflicts()?;

    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        if util::fs::is_tabular(&conflict.head_entry.path) {
            let df_head_path = util::fs::version_path(repo, &conflict.head_entry);
            let df_merge_path = util::fs::version_path(repo, &conflict.merge_entry);
            let df_head = tabular::read_df(&df_head_path, DFOpts::empty())?;
            let df_merge = tabular::read_df(&df_merge_path, DFOpts::empty())?;

            log::debug!("GOT DF HEAD {}", df_head);
            log::debug!("GOT DF MERGE {}", df_merge);

            match df_head.vstack(&df_merge) {
                Ok(result) => {
                    log::debug!("GOT DF COMBINED {}", result);
                    match result.unique(None, polars::frame::UniqueKeepStrategy::First) {
                        Ok(mut uniq) => {
                            log::debug!("GOT DF COMBINED UNIQUE {}", uniq);
                            let output_path = repo.path.join(&conflict.head_entry.path);
                            tabular::write_df(&mut uniq, &output_path)
                        }
                        _ => Err(OxenError::basic_str("Could not uniq data")),
                    }
                }
                _ => Err(OxenError::basic_str(
                    "Could not combine data, make sure schema's match",
                )),
            }
        } else {
            Err(OxenError::basic_str(
                "Cannot use --combine on non-tabular data file.",
            ))
        }
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
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
pub async fn list_remote_branches(
    repo: &LocalRepository,
    name: &str,
) -> Result<Vec<RemoteBranch>, OxenError> {
    let mut branches: Vec<RemoteBranch> = vec![];
    if let Some(remote) = repo.get_remote(name) {
        if let Some(remote_repo) = api::remote::repositories::get_by_remote(&remote).await? {
            for branch in api::remote::branches::list(&remote_repo).await? {
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
    resource::get_head_commit(repo)
}

/// # Create a remote repository
/// Takes the current directory name, and creates a repository on the server we can sync to. Returns the remote URL.
pub async fn create_remote<S: AsRef<str>>(
    repo: &LocalRepository,
    namespace: &str,
    name: &str,
    host: S,
) -> Result<RemoteRepository, OxenError> {
    api::remote::repositories::create(repo, namespace, name, host.as_ref()).await
}

/// # Set the remote for a repository
/// Tells the CLI where to push the changes to
pub fn add_remote(repo: &mut LocalRepository, name: &str, url: &str) -> Result<(), OxenError> {
    repo.add_remote(name, url);
    repo.save_default()?;
    Ok(())
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
/// # #[tokio::main]
/// # async fn main() -> Result<(), OxenError> {
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
/// command::add_remote(&mut repo, "origin", "http://0.0.0.0:3000/repositories/hello");
///
/// let remote_repo = command::create_remote(&repo, "repositories", "hello", "0.0.0.0:3000").await?;
///
/// // Push the file
/// command::push(&repo).await;
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # api::remote::repositories::delete(&remote_repo).await?;
/// # Ok(())
/// # }
/// ```
pub async fn push(repo: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer.push(&rb).await
}

/// Push to a specific remote
pub async fn push_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
) -> Result<RemoteRepository, OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch),
    };
    indexer.push(&rb).await
}

/// Clone a repo from a url to a directory
pub async fn clone(url: &str, dst: &Path) -> Result<LocalRepository, OxenError> {
    match LocalRepository::clone_remote(url, dst).await {
        Ok(Some(repo)) => Ok(repo),
        Ok(None) => Err(OxenError::remote_repo_not_found(url)),
        Err(err) => Err(err),
    }
}

/// Pull a repository's data from origin/main
pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer.pull(&rb).await?;
    Ok(())
}

/// Diff a file from commit history
pub fn diff(
    repo: &LocalRepository,
    commit_id_or_branch: Option<&str>,
    path: &str,
) -> Result<String, OxenError> {
    let commit = resource::get_commit_or_head(repo, commit_id_or_branch)?;
    differ::diff(repo, Some(&commit.id), path)
}

/// Pull a specific origin and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch),
    };
    indexer.pull(&rb).await?;
    Ok(())
}

/// Inspect a key value database for debugging
pub fn inspect(path: &Path) -> Result<(), OxenError> {
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);
    let db = DB::open_for_read_only(&opts, path, false)?;
    let iter = db.iterator(IteratorMode::Start);
    for (key, value) in iter {
        // try to decode u32 first (hacky but only two types we inspect right now)
        if let (Ok(key), Ok(value)) = (str::from_utf8(&key), u32::decode::<u8>(&value)) {
            println!("{}\t{}", key, value)
        } else if let (Ok(key), Ok(value)) = (str::from_utf8(&key), str::from_utf8(&value)) {
            println!("{}\t{}", key, value)
        }
    }
    Ok(())
}
