//! # Oxen Commands
//!
//! Top level commands you are likely to run on an Oxen repository
//!

use crate::api;
use crate::compute;
use crate::config::UserConfig;
use crate::constants;
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
    CommitDirReader, CommitReader, CommitWriter, EntryIndexer, MergeConflictReader, Merger,
    RefReader, RefWriter, Stager,
};
use crate::model::entry::mod_entry::ModType;
use crate::model::schema;
use crate::model::staged_data::StagedDataOpts;
use crate::model::CommitBody;
use crate::model::Schema;
use crate::model::User;
use crate::model::{Branch, Commit, LocalRepository, RemoteBranch, RemoteRepository, StagedData};

use crate::opts::{CloneOpts, LogOpts, RestoreOpts, RmOpts};
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
        let err = format!("Oxen repository already exists: {path:?}");
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

    api::local::commits::commit_with_no_files(&repo, constants::INITIAL_COMMIT_MSG)?;

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

/// # Stage files into repository
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
    let ignore = oxenignore::create(repo);
    stager.add(path.as_ref(), &reader, &ignore)?;
    Ok(())
}

pub async fn remote_add<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let path = path.as_ref();
    // * make sure we are on a branch
    let branch = current_branch(repo)?;
    if branch.is_none() {
        return Err(OxenError::basic_str(
            "Must be on branch to stage remote changes.",
        ));
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

    // Post into directory that is also local
    let directory = path
        .parent()
        .ok_or_else(|| OxenError::basic_str("Could not get parent directory"))?;

    let relative = if directory.is_relative() {
        directory.to_path_buf()
    } else {
        util::fs::path_relative_to_dir(directory, &repo.path)?
    };
    let directory_name = relative
        .to_str()
        .ok_or_else(|| OxenError::basic_str("Could not convert path to string"))?
        .to_string();

    let user_id = UserConfig::identifier()?;
    let result = api::remote::staging::stage_file(
        &remote_repo,
        &branch.name,
        &user_id,
        &directory_name,
        path.to_path_buf(),
    )
    .await?;

    println!("{}", result.to_string_lossy());

    Ok(())
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
) -> Result<(), OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    if let Some(branch) = current_branch(repo)? {
        let user_id = UserConfig::identifier()?;
        api::remote::staging::stage_modification(
            &remote_repo,
            &branch.name,
            &user_id,
            path,
            data.to_string(),
            opts.content_type.to_owned(),
            ModType::Append,
        )
        .await?;
        Ok(())
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
) -> Result<(), OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repository).await?;
    if let Some(branch) = current_branch(repository)? {
        let user_id = UserConfig::identifier()?;
        api::remote::staging::delete_staged_modification(
            &remote_repo,
            &branch.name,
            &user_id,
            path,
            uuid,
        )
        .await?;
        Ok(())
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
) -> Result<(), OxenError> {
    // Special case where we are writing data
    if let Some(row) = &opts.add_row {
        add_row(repo, input.as_ref(), &row, &opts).await?;
    } else {
        let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
        let branch = current_branch(repo)?.unwrap();
        let output = opts.output.clone();
        let mut df = api::remote::df::show(&remote_repo, &branch.name, input, opts).await?;
        if let Some(output) = output {
            println!("Writing {output:?}");
            tabular::write_df(&mut df, output)?;
        }

        println!("{df:?}");
    }

    Ok(())
}

pub fn df_schema<P: AsRef<Path>>(input: P, flatten: bool) -> Result<String, OxenError> {
    tabular::schema_to_string(input, flatten)
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
    let head_commit = head_commit(repo)?;
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
    let mut status = status(repo)?;
    if !status.has_added_entries() {
        println!(
            "No files are staged, not committing. Stage a file or directory with `oxen add <file>`"
        );
        return Ok(None);
    }
    let commit = api::local::commits::commit(repo, &mut status, message)?;
    Ok(Some(commit))
}

/// # Commit changes that are staged on the remote repository
pub async fn remote_commit(
    repo: &LocalRepository,
    message: &str,
) -> Result<Option<Commit>, OxenError> {
    let branch = current_branch(repo)?;
    if branch.is_none() {
        return Err(OxenError::basic_str("Must be on branch."));
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
        let commits = committer.history_from_head()?;
        Ok(commits)
    }
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
pub async fn checkout<S: AsRef<str>>(repo: &LocalRepository, value: S) -> Result<(), OxenError> {
    let value = value.as_ref();
    log::debug!("--- CHECKOUT START {} ----", value);
    if branch_exists(repo, value) {
        if already_on_branch(repo, value) {
            println!("Already on branch {value}");
            return Ok(());
        }

        println!("Checkout branch: {value}");
        set_working_branch(repo, value).await?;
        set_head(repo, value)?;
    } else {
        // If we are already on the commit, do nothing
        if already_on_commit(repo, value) {
            eprintln!("Commit already checked out {value}");
            return Ok(());
        }

        println!("Checkout commit: {value}");
        set_working_commit_id(repo, value).await?;
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
        if util::fs::is_tabular(&conflict.head_entry.path) {
            let df_head_path = util::fs::version_path(repo, &conflict.head_entry);
            let df_head = tabular::read_df(df_head_path, DFOpts::empty())?;
            let df_merge_path = util::fs::version_path(repo, &conflict.merge_entry);
            let df_merge = tabular::read_df(df_merge_path, DFOpts::empty())?;

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
            Err(OxenError::must_be_on_valid_branch())
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
/// command::add_remote(&mut repo, "origin", "http://localhost:3000/repositories/hello");
///
/// let remote_repo = command::create_remote(&repo, "repositories", "hello", "localhost:3000").await?;
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

/// Diff a file from commit history
pub fn diff(
    repo: &LocalRepository,
    commit_id_or_branch: Option<&str>,
    path: &Path,
) -> Result<String, OxenError> {
    let commit = resource::get_commit_or_head(repo, commit_id_or_branch)?;
    differ::diff(repo, Some(&commit.id), path)
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
pub fn migrate_all_repos(path: &Path) -> Result<(), OxenError> {
    let namespaces = api::local::repositories::list_namespaces(path)?;
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = api::local::repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            println!("Migrate repo {:?}", repo.path);
            match migrate_repo(&repo) {
                Ok(_) => {
                    println!("Done.");
                }
                Err(err) => {
                    log::error!("Could not migrate repo {:?}\nErr: {}", repo.path, err)
                }
            }
        }
    }

    Ok(())
}

/// Run the computation cache on all repositories within a directory
pub fn migrate_repo(repo: &LocalRepository) -> Result<(), OxenError> {
    let commits = log(repo)?;
    for commit in commits {
        compute::commit_cacher::run_all(repo, &commit)?;
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
