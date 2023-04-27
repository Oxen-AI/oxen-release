//! # Oxen Commands - entry point for all Oxen commands
//!
//! Top level commands you are likely to run on an Oxen repository
//!

use crate::api;
use crate::cache;
use crate::config::UserConfig;
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::constants::DEFAULT_PAGE_NUM;
use crate::constants::DEFAULT_PAGE_SIZE;
use crate::constants::DEFAULT_REMOTE_NAME;
use crate::df::{df_opts::DFOpts, tabular};
use crate::error::OxenError;
use crate::index::oxenignore;
use crate::index::remote_stager;
use crate::index::SchemaIndexReader;
use crate::index::{self, differ};
use crate::index::{
    CommitReader, CommitWriter, EntryIndexer, MergeConflictReader, Merger, RefReader, RefWriter,
    Stager,
};
use crate::model::entry::mod_entry::ModType;
use crate::model::schema;
use crate::model::staged_data::StagedDataOpts;
use crate::model::CommitBody;
use crate::model::Schema;
use crate::model::User;
use crate::model::{Branch, Commit, LocalRepository, RemoteBranch, RemoteRepository, StagedData};

use crate::opts::AddOpts;
use crate::opts::PaginateOpts;
use crate::opts::{CloneOpts, LogOpts, RestoreOpts, RmOpts};
use crate::util;
use crate::view::PaginatedDirEntries;

use bytevec::ByteDecodable;
use polars::prelude::DataFrame;
use rocksdb::{IteratorMode, LogLevel, Options, DB};
use std::path::Path;
use std::path::PathBuf;
use std::str;

pub mod add;
pub mod commit;
pub mod init;
pub mod push;
pub mod status;

pub use crate::command::add::add;
pub use crate::command::commit::commit;
pub use crate::command::init::init;
pub use crate::command::push::{push, push_remote_branch, push_remote_repo_branch_name};
pub use crate::command::status::{status, status_from_dir};

pub async fn remote_status(
    remote_repo: &RemoteRepository,
    branch: &Branch,
    directory: &Path,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    let user_id = UserConfig::identifier()?;
    remote_stager::status(remote_repo, branch, &user_id, directory, opts).await
}

pub async fn remote_status_from_local(
    repository: &LocalRepository,
    directory: &Path,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    remote_stager::status_from_local(repository, directory, opts).await
}

pub async fn remote_ls(
    remote_repo: &RemoteRepository,
    branch: &Branch,
    directory: &Path,
    opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    api::remote::dir::list_dir(
        remote_repo,
        &branch.name,
        directory,
        opts.page_num,
        opts.page_size,
    )
    .await
}

pub async fn remote_add<P: AsRef<Path>>(
    repo: &LocalRepository,
    path: P,
    opts: &AddOpts,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    // * make sure we are on a branch
    let branch = current_branch(repo)?;
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
        .ok_or_else(OxenError::remote_not_set)?;

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
            Err(OxenError::file_does_not_exist(path))
        }
    }
}

fn add_row_local(path: &Path, data: &str) -> Result<(), OxenError> {
    if util::fs::is_tabular(path) {
        let mut opts = DFOpts::empty();
        opts.add_row = Some(data.to_string());
        opts.output = Some(path.to_path_buf());
        df(path, opts)?;
    } else {
        util::fs::append_to_file(path, data)?;
    }

    Ok(())
}

async fn add_row_remote(
    repo: &LocalRepository,
    path: &Path,
    data: &str,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    if let Some(branch) = current_branch(repo)? {
        let user_id = UserConfig::identifier()?;
        let modification = api::remote::staging::stage_modification(
            &remote_repo,
            &branch.name,
            &user_id,
            path,
            data.to_string(),
            opts.content_type.to_owned(),
            ModType::Append,
        )
        .await?;
        println!("{:?}", modification.to_df()?);
        modification.to_df()
    } else {
        Err(OxenError::basic_str(
            "Must be on a branch to stage remote changes.",
        ))
    }
}

pub async fn add_row(
    repo: &LocalRepository,
    path: &Path,
    data: &str,
    opts: &DFOpts,
) -> Result<(), OxenError> {
    if opts.is_remote {
        add_row_remote(repo, path, data, opts).await?;
    } else {
        add_row_local(path, data)?;
    }

    Ok(())
}

pub async fn delete_staged_row(
    repository: &LocalRepository,
    path: impl AsRef<Path>,
    uuid: &str,
) -> Result<DataFrame, OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repository).await?;
    if let Some(branch) = current_branch(repository)? {
        let user_id = UserConfig::identifier()?;
        let modification = api::remote::staging::delete_staged_modification(
            &remote_repo,
            &branch.name,
            &user_id,
            path,
            uuid,
        )
        .await?;
        modification.to_df()
    } else {
        Err(OxenError::basic_str(
            "Must be on a branch to stage remote changes.",
        ))
    }
}

/// Removes the path from the index
pub async fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    index::rm(repo, opts).await
}

/// Interact with DataFrames from CLI
pub fn df<P: AsRef<Path>>(input: P, opts: DFOpts) -> Result<(), OxenError> {
    let mut df = tabular::show_path(input, opts.clone())?;

    if let Some(output) = opts.output {
        println!("Writing {output:?}");
        tabular::write_df(&mut df, output)?;
    }

    Ok(())
}

/// Interact with Remote DataFrames from CLI
pub async fn remote_df<P: AsRef<Path>>(
    repo: &LocalRepository,
    input: P,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    // Special case where we are writing data
    if let Some(row) = &opts.add_row {
        add_row_remote(repo, input.as_ref(), row, &opts).await
    } else if let Some(uuid) = &opts.delete_row {
        delete_staged_row(repo, input, uuid).await
    } else {
        let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
        let branch = current_branch(repo)?.unwrap();
        let output = opts.output.clone();
        let (mut df, size) = api::remote::df::show(&remote_repo, &branch.name, input, opts).await?;
        if let Some(output) = output {
            println!("Writing {output:?}");
            tabular::write_df(&mut df, output)?;
        }

        println!("Full shape: ({}, {})\n", size.height, size.width);
        println!("Slice {df:?}");
        Ok(df)
    }
}

pub fn df_schema<P: AsRef<Path>>(
    input: P,
    flatten: bool,
    opts: DFOpts,
) -> Result<String, OxenError> {
    tabular::schema_to_string(input, flatten, &opts)
}

/// List staged schema
pub fn schema_get_staged(
    repo: &LocalRepository,
    schema_ref: &str,
) -> Result<Option<Schema>, OxenError> {
    let stager = Stager::new(repo)?;
    stager.get_staged_schema(schema_ref)
}

/// List the saved off schemas for a commit id
pub fn schema_list(
    repo: &LocalRepository,
    commit_id: Option<&str>,
) -> Result<Vec<Schema>, OxenError> {
    api::local::schemas::list(repo, commit_id)
}

pub fn schema_list_staged(repo: &LocalRepository) -> Result<Vec<Schema>, OxenError> {
    let stager = Stager::new(repo)?;
    stager.list_staged_schemas()
}

pub fn schema_get_from_head(
    repo: &LocalRepository,
    schema_ref: &str,
) -> Result<Option<Schema>, OxenError> {
    schema_get(repo, None, schema_ref)
}

pub fn schema_get(
    repo: &LocalRepository,
    commit_id: Option<&str>,
    schema_ref: &str,
) -> Result<Option<Schema>, OxenError> {
    // The list of schemas should not be too long, so just filter right now
    let list: Vec<Schema> = schema_list(repo, commit_id)?
        .into_iter()
        .filter(|s| s.name == Some(String::from(schema_ref)) || s.hash == *schema_ref)
        .collect();
    if !list.is_empty() {
        Ok(Some(list.first().unwrap().clone()))
    } else {
        Ok(None)
    }
}

pub fn schema_name(repo: &LocalRepository, hash: &str, val: &str) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    stager.update_schema_names_for_hash(hash, val)
}

pub fn schema_list_indices(
    repo: &LocalRepository,
    schema_ref: &str,
) -> Result<Vec<schema::Field>, OxenError> {
    let head_commit = api::local::commits::head_commit(repo)?;
    if let Some(schema) = schema_get(repo, Some(&head_commit.id), schema_ref)? {
        let index_reader = SchemaIndexReader::new(repo, &head_commit, &schema)?;
        index_reader.list_field_indices()
    } else {
        Err(OxenError::schema_does_not_exist(schema_ref))
    }
}

/// # Restore a removed file that was committed
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::test;
/// # use liboxen::error::OxenError;
/// # use liboxen::opts::RestoreOpts;
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
/// command::restore(&repo, RestoreOpts::from_path_ref(hello_name, commit.id))?;
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    index::restore(repo, opts)
}

/// Remove all staged changes from file on remote
pub async fn remote_restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    let branch = current_branch(repo)?;
    if branch.is_none() {
        return Err(OxenError::must_be_on_valid_branch());
    }
    let branch = branch.unwrap();
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let user_id = UserConfig::identifier()?;
    api::remote::staging::restore_df(&remote_repo, &branch.name, &user_id, opts.path.to_owned())
        .await
}

/// # Commit changes that are staged on the remote repository
pub async fn remote_commit(
    repo: &LocalRepository,
    message: &str,
) -> Result<Option<Commit>, OxenError> {
    let branch = current_branch(repo)?;
    if branch.is_none() {
        return Err(OxenError::must_be_on_valid_branch());
    }
    let branch = branch.unwrap();

    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let cfg = UserConfig::get()?;
    let body = CommitBody {
        message: message.to_string(),
        user: User {
            name: cfg.name,
            email: cfg.email,
        },
    };
    let user_id = UserConfig::identifier()?;
    let commit =
        api::remote::staging::commit_staged(&remote_repo, &branch.name, &user_id, &body).await?;
    Ok(Some(commit))
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
/// // Print commit history
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

/// Log given options
pub async fn log_with_opts(
    repo: &LocalRepository,
    opts: &LogOpts,
) -> Result<Vec<Commit>, OxenError> {
    if opts.remote {
        let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
        let committish = if let Some(committish) = &opts.committish {
            committish.to_owned()
        } else {
            current_branch(repo)?.unwrap().name
        };
        let commits = api::remote::commits::list_commit_history(&remote_repo, &committish).await?;
        Ok(commits)
    } else {
        let committer = CommitReader::new(repo)?;

        let commits = if let Some(committish) = &opts.committish {
            let commit = api::local::commits::get_by_id_or_branch(repo, committish)?.ok_or(
                OxenError::committish_not_found(committish.to_string().into()),
            )?;
            committer.history_from_commit_id(&commit.id)?
        } else {
            committer.history_from_head()?
        };
        Ok(commits)
    }
}

/// # Get the history for a specific branch or commit
pub fn log_commit_or_branch_history(
    repo: &LocalRepository,
    commit_or_branch: &str,
) -> Result<Vec<Commit>, OxenError> {
    log::debug!("log_commit_or_branch_history: {}", commit_or_branch);
    let committer = CommitReader::new(repo)?;
    if commit_or_branch.contains("..") {
        // This is BASE..HEAD format, and we only want to history from BASE to HEAD
        let split: Vec<&str> = commit_or_branch.split("..").collect();
        let base = split[0];
        let head = split[1];
        let base_commit_id = match get_branch_commit_id(repo, base)? {
            Some(branch_commit_id) => branch_commit_id,
            None => String::from(base),
        };
        let head_commit_id = match get_branch_commit_id(repo, head)? {
            Some(branch_commit_id) => branch_commit_id,
            None => String::from(head),
        };
        log::debug!(
            "log_commit_or_branch_history: base_commit_id: {} head_commit_id: {}",
            base_commit_id,
            head_commit_id
        );
        return match committer.history_from_base_to_head(&base_commit_id, &head_commit_id) {
            Ok(commits) => Ok(commits),
            Err(_) => Err(OxenError::local_commit_or_branch_not_found(
                commit_or_branch,
            )),
        };
    }

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

/// # Rename current branch
pub fn rename_current_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    api::local::branches::rename_current_branch(repo, name)
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
pub async fn checkout<S: AsRef<str>>(
    repo: &LocalRepository,
    value: S,
) -> Result<Option<Branch>, OxenError> {
    let value = value.as_ref();
    log::debug!("--- CHECKOUT START {} ----", value);
    if branch_exists(repo, value) {
        if already_on_branch(repo, value) {
            println!("Already on branch {value}");
            return api::local::branches::get_by_name(repo, value);
        }

        println!("Checkout branch: {value}");
        set_working_branch(repo, value).await?;
        set_head(repo, value)?;
        api::local::branches::get_by_name(repo, value)
    } else {
        // If we are already on the commit, do nothing
        if already_on_commit(repo, value) {
            eprintln!("Commit already checked out {value}");
            return Ok(None);
        }

        println!("Checkout commit: {value}");
        set_working_commit_id(repo, value).await?;
        set_head(repo, value)?;
        Ok(None)
    }
}

/// # Checkout a file and take their changes
/// This overwrites the current file with the changes in the branch we are merging in
pub fn checkout_theirs<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let merger = MergeConflictReader::new(repo)?;
    let conflicts = merger.list_conflicts()?;
    log::debug!(
        "checkout_theirs {:?} conflicts.len() {}",
        path.as_ref(),
        conflicts.len()
    );

    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        // Lookup the file for the merge commit entry and copy it over
        restore(
            repo,
            RestoreOpts::from_path_ref(path, conflict.merge_entry.commit_id.clone()),
        )
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}

/// # Combine Conflicting Tabular Data Files
/// This overwrites the current file with the changes in their file
pub fn checkout_combine<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let merger = MergeConflictReader::new(repo)?;
    let conflicts = merger.list_conflicts()?;
    log::debug!(
        "checkout_combine checking path {:?} -> [{}] conflicts",
        path.as_ref(),
        conflicts.len()
    );
    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        if util::fs::is_tabular(&conflict.base_entry.path) {
            let df_base_path = util::fs::version_path(repo, &conflict.base_entry);
            let df_base = tabular::read_df(df_base_path, DFOpts::empty())?;
            let df_merge_path = util::fs::version_path(repo, &conflict.merge_entry);
            let df_merge = tabular::read_df(df_merge_path, DFOpts::empty())?;

            log::debug!("GOT DF HEAD {}", df_base);
            log::debug!("GOT DF MERGE {}", df_merge);

            match df_base.vstack(&df_merge) {
                Ok(result) => {
                    log::debug!("GOT DF COMBINED {}", result);
                    match result.unique(None, polars::frame::UniqueKeepStrategy::First, None) {
                        Ok(mut uniq) => {
                            log::debug!("GOT DF COMBINED UNIQUE {}", uniq);
                            let output_path = repo.path.join(&conflict.base_entry.path);
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

async fn set_working_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    let commit_writer = CommitWriter::new(repo)?;
    commit_writer.set_working_repo_to_branch(name).await
}

async fn set_working_commit_id(repo: &LocalRepository, commit_id: &str) -> Result<(), OxenError> {
    let commit_writer = CommitWriter::new(repo)?;
    commit_writer.set_working_repo_to_commit_id(commit_id).await
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
    println!("Create and checkout branch: {name}");
    let head_commit = api::local::commits::head_commit(repo)?;
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
    merge_branch_name: S,
) -> Result<Option<Commit>, OxenError> {
    let merge_branch_name = merge_branch_name.as_ref();
    if !branch_exists(repo, merge_branch_name) {
        return Err(OxenError::local_branch_not_found(merge_branch_name));
    }

    let base_branch = current_branch(repo)?.ok_or(OxenError::must_be_on_valid_branch())?;
    let merge_branch = api::local::branches::get_by_name(repo, merge_branch_name)?
        .ok_or(OxenError::local_branch_not_found(merge_branch_name))?;

    let merger = Merger::new(repo)?;
    if let Some(commit) = merger.merge_into_base(&merge_branch, &base_branch)? {
        println!(
            "Successfully merged `{}` into `{}`",
            merge_branch_name, base_branch.name
        );
        println!("HEAD -> {}", commit.id);
        Ok(Some(commit))
    } else {
        eprintln!("Automatic merge failed; fix conflicts and then commit the result.");
        Ok(None)
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
    if url::Url::parse(url).is_err() {
        return Err(OxenError::invalid_set_remote_url(url));
    }

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

/// Clone a repo from a url to a directory
pub async fn clone(opts: &CloneOpts) -> Result<LocalRepository, OxenError> {
    match LocalRepository::clone_remote(opts).await {
        Ok(Some(repo)) => Ok(repo),
        Ok(None) => Err(OxenError::remote_repo_not_found(&opts.url)),
        Err(err) => Err(err),
    }
}

// To make CloneOpts refactor easier...
pub async fn clone_remote(
    url: &str,
    dst: &Path,
    shallow: bool,
) -> Result<LocalRepository, OxenError> {
    let opts = CloneOpts {
        url: url.to_string(),
        dst: dst.to_path_buf(),
        branch: DEFAULT_BRANCH_NAME.to_string(),
        shallow,
    };
    match LocalRepository::clone_remote(&opts).await {
        Ok(Some(repo)) => Ok(repo),
        Ok(None) => Err(OxenError::remote_repo_not_found(&opts.url)),
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

/// Diff a file from a commit or compared to another file
/// `resource` can be a None, commit id, branch name, or another path.
///    None: compare `path` to the last commit versioned of the file. If a merge conflict with compare to the merge conflict
///    commit id: compare `path` to the version of `path` from that commit
///    branch name: compare `path` to the version of `path` from that branch
///    another path: compare `path` to the other `path` provided
/// `path` is the path you want to compare the resource to
pub fn diff(
    repo: &LocalRepository,
    resource: Option<&str>,
    path: impl AsRef<Path>,
) -> Result<String, OxenError> {
    if let Some(resource) = resource {
        // `resource` is Some(resource)
        if let Some(compare_commit) = api::local::commits::get_by_id(repo, resource)? {
            // `resource` is a commit id
            let original_commit = api::local::commits::head_commit(repo)?;
            differ::diff(repo, &original_commit, &compare_commit, path)
        } else if let Some(branch) = api::local::branches::get_by_name(repo, resource)? {
            // `resource` is a branch name
            let compare_commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
            let original_commit = api::local::commits::head_commit(repo)?;

            differ::diff(repo, &original_commit, &compare_commit, path)
        } else if Path::new(resource).exists() {
            // `resource` is another path
            differ::diff_files(resource, path)
        } else {
            Err(OxenError::basic_str(format!(
                "Could not find resource: {resource:?}"
            )))
        }
    } else {
        // `resource` is None
        // First check if there are merge conflicts
        let merger = MergeConflictReader::new(repo)?;
        if merger.has_conflicts()? {
            match merger.get_conflict_commit() {
                Ok(Some(commit)) => {
                    let current_path = path.as_ref();
                    let version_path =
                        differ::get_version_file_from_commit(repo, &commit, current_path)?;
                    differ::diff_files(current_path, version_path)
                }
                err => {
                    log::error!("{err:?}");
                    Err(OxenError::basic_str(format!(
                        "Could not find merge resource: {resource:?}"
                    )))
                }
            }
        } else {
            // No merge conflicts, compare to last version committed of the file
            let current_path = path.as_ref();
            let commit = api::local::commits::head_commit(repo)?;
            let version_path = differ::get_version_file_from_commit(repo, &commit, current_path)?;
            differ::diff_files(version_path, current_path)
        }
    }
}

pub async fn remote_diff(
    repo: &LocalRepository,
    branch_name: Option<&str>,
    path: &Path,
) -> Result<String, OxenError> {
    let branch = get_branch_by_name_or_current(repo, branch_name)?;
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let user_id = UserConfig::identifier()?;
    let diff = api::remote::staging::diff_staged_file(
        &remote_repo,
        &branch.name,
        &user_id,
        path,
        DEFAULT_PAGE_NUM,
        DEFAULT_PAGE_SIZE,
    )
    .await?;
    Ok(diff.to_string())
}

/// Get branch by name
fn get_branch_by_name_or_current(
    repo: &LocalRepository,
    branch_name: Option<&str>,
) -> Result<Branch, OxenError> {
    if let Some(branch_name) = branch_name {
        match api::local::branches::get_by_name(repo, branch_name)? {
            Some(branch) => Ok(branch),
            None => Err(OxenError::local_branch_not_found(branch_name)),
        }
    } else {
        match current_branch(repo)? {
            Some(branch) => Ok(branch),
            None => Err(OxenError::must_be_on_valid_branch()),
        }
    }
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

/// Run the computation cache on all repositories within a directory
pub async fn compute_cache_on_all_repos(path: &Path) -> Result<(), OxenError> {
    let namespaces = api::local::repositories::list_namespaces(path)?;
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = api::local::repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            println!("Compute cache for repo {:?}", repo.path);
            match compute_cache(&repo, None).await {
                Ok(_) => {
                    println!("Done.");
                }
                Err(err) => {
                    log::error!(
                        "Could not compute cache for repo {:?}\nErr: {}",
                        repo.path,
                        err
                    )
                }
            }
        }
    }

    Ok(())
}

/// Run the computation cache on all repositories within a directory
pub async fn compute_cache(
    repo: &LocalRepository,
    committish: Option<String>,
) -> Result<(), OxenError> {
    println!(
        "Compute cache for commit given [{committish:?}] on repo {:?}",
        repo.path
    );
    let commits = if let Some(committish) = committish {
        let opts = LogOpts {
            committish: Some(committish),
            remote: false,
        };
        log_with_opts(repo, &opts).await?
    } else {
        log(repo)?
    };
    for commit in commits {
        println!("Compute cache for commit {:?}", commit);
        cache::commit_cacher::run_all(repo, &commit)?;
    }
    Ok(())
}

/// Inspect a key value database for debugging
pub fn inspect(path: &Path) -> Result<(), OxenError> {
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);
    let db = DB::open_for_read_only(&opts, dunce::simplified(path), false)?;
    let iter = db.iterator(IteratorMode::Start);
    for item in iter {
        match item {
            Ok((key, value)) => {
                // try to decode u32 first (hacky but only two types we inspect right now)
                if let (Ok(key), Ok(value)) = (str::from_utf8(&key), u32::decode::<u8>(&value)) {
                    println!("{key}\t{value}")
                } else if let (Ok(key), Ok(value)) = (str::from_utf8(&key), str::from_utf8(&value))
                {
                    println!("{key}\t{value}")
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(())
}
