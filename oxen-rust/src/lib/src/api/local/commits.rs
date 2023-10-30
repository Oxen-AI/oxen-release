//! # Local Commits
//!
//! Interact with local commits.
//!

use crate::constants::{HISTORY_DIR, TREE_DIR};
use crate::core::cache::cachers::content_validator;
use crate::core::db::path_db;
use crate::core::db::tree_db::{TreeDB, TreeNode};
use crate::core::index::tree_db_reader::TreeDBMerger;
use crate::core::index::{
    self, CommitEntryReader, CommitEntryWriter, CommitReader, CommitWriter, RefReader, RefWriter,
    Stager, TreeDBReader,
};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, StagedData};
use crate::opts::LogOpts;
use crate::util::fs::commit_content_is_valid_path;
use crate::view::{PaginatedCommits, StatusMessage};
use crate::{api, util};
use rayon::prelude::*;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Iterate over commits and get the one with the latest timestamp
pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let reader = CommitReader::new(repo)?;
    reader.latest_commit()
}

/// The current HEAD commit of the branch you are on
pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let reader = CommitReader::new(repo)?;
    reader.head_commit()
}

/// Get the root commit of a repository
pub fn root_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let committer = CommitReader::new(repo)?;
    let commit = committer.root_commit()?;
    Ok(commit)
}

/// Get a commit by it's hash
pub fn get_by_id(repo: &LocalRepository, commit_id: &str) -> Result<Option<Commit>, OxenError> {
    let reader = CommitReader::new(repo)?;
    reader.get_commit_by_id(commit_id)
}

/// Get a list commits by the commit message
pub fn get_by_message(
    repo: &LocalRepository,
    msg: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    let commits = list_all(repo)?;
    let filtered: Vec<Commit> = commits
        .into_iter()
        .filter(|commit| commit.message == msg.as_ref())
        .collect();
    Ok(filtered)
}

/// Get the most recent commit by the commit message, starting at the HEAD commit
pub fn first_by_message(
    repo: &LocalRepository,
    msg: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let committer = CommitReader::new(repo)?;
    let commits = committer.history_from_head()?;
    Ok(commits
        .into_iter()
        .find(|commit| commit.message == msg.as_ref()))
}

pub fn get_parents(repo: &LocalRepository, commit: &Commit) -> Result<Vec<Commit>, OxenError> {
    let committer = CommitReader::new(repo)?;
    let mut commits: Vec<Commit> = vec![];
    for commit_id in commit.parent_ids.iter() {
        if let Some(commit) = committer.get_commit_by_id(commit_id)? {
            commits.push(commit)
        } else {
            return Err(OxenError::commit_db_corrupted(commit_id));
        }
    }
    Ok(commits)
}

pub fn commit_content_size(repo: &LocalRepository, commit: &Commit) -> Result<u64, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    let entries = reader.list_entries()?;
    Ok(compute_entries_size(&entries))
}

pub fn compute_entries_size(entries: &[CommitEntry]) -> u64 {
    // Sum up entry size in parallel using rayon
    entries.par_iter().map(|entry| entry.num_bytes).sum::<u64>()
}

pub fn commit_from_branch_or_commit_id<S: AsRef<str>>(
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

pub fn list_with_missing_dbs(
    repo: &LocalRepository,
    commit_id: &str,
) -> Result<Vec<Commit>, OxenError> {
    let mut missing_db_commits: Vec<Commit> = vec![];

    // Get full commit history for this repo to report any missing commits
    let commits = api::local::commits::list_from(repo, commit_id)?;
    for commit in commits {
        if !commit_history_db_exists(repo, &commit)? {
            missing_db_commits.push(commit);
        }
    }
    // BASE-->HEAD order
    missing_db_commits.reverse();

    Ok(missing_db_commits)
}

pub fn list_with_missing_entries(
    repo: &LocalRepository,
    commit_id: &str,
) -> Result<Vec<Commit>, OxenError> {
    log::debug!("list_with_missing_entries[{}]", commit_id);
    let mut missing_entry_commits: Vec<Commit> = vec![];

    // Get full commit history for this repo to report any missing commits
    let commits = api::local::commits::list_from(repo, commit_id)?;

    log::debug!("considering {} commits", commits.len());

    for commit in commits {
        log::debug!("considering commit {}", commit);
        let path = commit_content_is_valid_path(repo, &commit);
        let path_is_valid = path.exists();
        let content_is_valid = content_validator::is_valid(repo, &commit)?;
        log::debug!(
            "commit {} path_is_valid: {} content_is_valid: {} path: {:?}",
            commit,
            path_is_valid,
            content_is_valid,
            path,
        );

        if path_is_valid && content_is_valid {
            continue;
        }
        log::debug!("UNSYNCED COMMIT {}", commit);
        missing_entry_commits.push(commit);
    }

    log::debug!("found {} unsynced commits", missing_entry_commits.len());

    // BASE-->HEAD order - essential for ensuring sync order
    missing_entry_commits.reverse();

    Ok(missing_entry_commits)
}

pub fn commit_history_db_exists(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<bool, OxenError> {
    let commit_history_dir = util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id);
    Ok(commit_history_dir.exists())
}

pub fn commit_with_no_files(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    let status = StagedData::empty();
    let commit = commit(repo, &status, message)?;
    println!("Initial commit {}", commit.id);
    Ok(commit)
}

pub fn commit(
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

pub fn create_commit_object_with_committers(
    _repo_dir: &Path,
    branch_name: impl AsRef<str>,
    commit: &Commit,
    commit_reader: &CommitReader,
    commit_writer: &CommitWriter,
    ref_writer: &RefWriter,
) -> Result<(), OxenError> {
    log::debug!("Create commit obj: {} -> '{}'", commit.id, commit.message);

    // If we have a root, and we are trying to push a new one, don't allow it
    if let Ok(root) = commit_reader.root_commit() {
        if commit.parent_ids.is_empty() && root.id != commit.id {
            log::error!("Root commit does not match {} != {}", root.id, commit.id);
            return Err(OxenError::root_commit_does_not_match(commit.to_owned()));
        }
    }

    // Todo - add back error creating commit writer on other side
    match commit_writer.add_commit_to_db(commit) {
        Ok(_) => {
            log::debug!("Successfully added commit [{}] to db", commit.id);
            ref_writer.set_branch_commit_id(branch_name.as_ref(), &commit.id)?;
        }
        Err(err) => {
            log::error!("Error adding commit to db: {:?}", err);
        }
    }
    Ok(())
}

pub fn create_commit_object(
    repo_dir: &Path,
    branch_name: impl AsRef<str>,
    commit: &Commit,
) -> Result<(), OxenError> {
    log::debug!("Create commit obj: {} -> '{}'", commit.id, commit.message);

    // Instantiate repo from dir
    let repo = LocalRepository::from_dir(repo_dir)?;

    // Create readers and writers
    let commit_reader = CommitReader::new(&repo)?;
    let commit_writer = CommitWriter::new(&repo)?;
    let ref_writer = RefWriter::new(&repo)?;

    create_commit_object_with_committers(
        repo_dir,
        branch_name,
        commit,
        &commit_reader,
        &commit_writer,
        &ref_writer,
    )
}

/// List commits on the current branch from HEAD
pub fn list(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    let committer = CommitReader::new(repo)?;
    let commits = committer.history_from_head()?;
    Ok(commits)
}

/// List commits for the repository in no particular order
pub fn list_all(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    let committer = CommitReader::new(repo)?;
    let commits = committer.list_all()?;
    Ok(commits)
}

/// Get commit history given options
pub async fn list_with_opts(
    repo: &LocalRepository,
    opts: &LogOpts,
) -> Result<Vec<Commit>, OxenError> {
    if opts.remote {
        let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
        let revision = if let Some(revision) = &opts.revision {
            revision.to_owned()
        } else {
            api::local::branches::current_branch(repo)?.unwrap().name
        };
        let commits = api::remote::commits::list_commit_history(&remote_repo, &revision).await?;
        Ok(commits)
    } else {
        let committer = CommitReader::new(repo)?;

        let commits = if let Some(revision) = &opts.revision {
            let commit = api::local::revisions::get(repo, revision)?
                .ok_or(OxenError::revision_not_found(revision.to_string().into()))?;
            committer.history_from_commit_id(&commit.id)?
        } else {
            committer.history_from_head()?
        };
        Ok(commits)
    }
}

/// List the history for a specific branch or commit (revision)
pub fn list_from(repo: &LocalRepository, revision: &str) -> Result<Vec<Commit>, OxenError> {
    log::debug!("list_from: {}", revision);
    let committer = CommitReader::new(repo)?;
    if revision.contains("..") {
        // This is BASE..HEAD format, and we only want to history from BASE to HEAD
        let split: Vec<&str> = revision.split("..").collect();
        let base = split[0];
        let head = split[1];
        let base_commit_id = match api::local::branches::get_commit_id(repo, base)? {
            Some(branch_commit_id) => branch_commit_id,
            None => String::from(base),
        };
        let head_commit_id = match api::local::branches::get_commit_id(repo, head)? {
            Some(branch_commit_id) => branch_commit_id,
            None => String::from(head),
        };
        log::debug!(
            "list_from: base_commit_id: {} head_commit_id: {}",
            base_commit_id,
            head_commit_id
        );
        return match committer.history_from_base_to_head(&base_commit_id, &head_commit_id) {
            Ok(commits) => Ok(commits),
            Err(_) => Err(OxenError::local_revision_not_found(revision)),
        };
    }

    let commit_id = match api::local::branches::get_commit_id(repo, revision)? {
        Some(branch_commit_id) => branch_commit_id,
        None => String::from(revision),
    };

    log::debug!("list_from: commit_id: {}", commit_id);
    match committer.history_from_commit_id(&commit_id) {
        Ok(commits) => Ok(commits),
        Err(_) => Err(OxenError::local_revision_not_found(revision)),
    }
}

/// Retrieve entries with filepaths matching a provided glob pattern
pub fn glob_entry_paths(
    repo: &LocalRepository,
    commit: &Commit,
    pattern: &str,
) -> Result<HashSet<PathBuf>, OxenError> {
    let committer = CommitEntryReader::new(repo, commit)?;
    let entries = committer.glob_entry_paths(pattern)?;
    Ok(entries)
}

/// List paginated commits starting from the given revision
pub fn list_from_paginated(
    repo: &LocalRepository,
    revision: &str,
    page_number: usize,
    page_size: usize,
) -> Result<PaginatedCommits, OxenError> {
    let commits = list_from(repo, revision)?;
    let (commits, pagination) = util::paginate(commits, page_number, page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}

pub fn commit_history_is_complete(repo: &LocalRepository, commit: &Commit) -> bool {
    // Get full commit history from this head backwards
    let history = api::local::commits::list_from(repo, &commit.id).unwrap();

    // Ensure traces back to base commit
    let maybe_initial_commit = history.last().unwrap();
    if !maybe_initial_commit.parent_ids.is_empty() {
        // If it has parents, it isn't an initial commit
        return false;
    }

    // Ensure all commits and their parents are synced
    // Initialize commit reader
    for c in &history {
        if !index::commit_sync_status::commit_is_synced(repo, c) {
            return false;
        }
    }
    true
}

// For merkle-tree driven conflict detection between local
// and remote heads. NOT a general-purpose merge conflict utility (yet)
pub fn head_commits_have_conflicts(
    repo: &LocalRepository,
    client_head_id: &str,
    server_head_id: &str,
    lca_id: &str,
) -> Result<bool, OxenError> {
    // Connect to the 3 commit merkle trees
    // TODONOW: generalize the tree_db access
    let lca_db_path = CommitEntryWriter::commit_tree_db(&repo.path, lca_id);
    let server_db_path = CommitEntryWriter::commit_tree_db(&repo.path, server_head_id);
    let client_db_path = util::fs::oxen_hidden_dir(&repo.path)
        .join("tmp")
        .join(client_head_id)
        .join(TREE_DIR); // TODONOW this path...

    // TODONOW: multithreaded
    // TODONOW: not loving this param ordering tbh
    let tree_merger = TreeDBMerger::new(repo, client_db_path, server_db_path, lca_db_path)?;

    // Start at the top level of the client db
    // TODONOW: need to probably fold these up into a tree_db_reader
    // TODONOW this is horrifying, fix the double .db

    let client_root = &tree_merger.client_reader.get_entry("")?.unwrap();
    let server_root = &tree_merger.server_reader.get_entry("")?.unwrap();
    let lca_root = &tree_merger.lca_reader.get_entry("")?.unwrap();

    // TODONOW: state management for these db connections...probably needs to be on a struct.
    let has_conflict = tree_merger.r_tree_has_conflict(&client_root, &server_root, &lca_root);
    log::debug!("This tree has conflict: {:?}", has_conflict);
    has_conflict
}


#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::test;

    #[tokio::test]
    async fn test_commit_history_is_complete() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let deep_clone =
                    command::deep_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                // Get head commit of deep_clone repo
                let head_commit = api::local::commits::head_commit(&deep_clone)?;
                assert!(api::local::commits::commit_history_is_complete(
                    &deep_clone,
                    &head_commit
                ));
                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_history_is_not_complete_standard_repo() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let clone =
                    command::clone_url(&remote_repo.remote.url, &new_repo_dir.join("new_repo"))
                        .await?;
                // Get head commit of deep_clone repo
                let head_commit = api::local::commits::head_commit(&clone)?;
                assert!(!api::local::commits::commit_history_is_complete(
                    &clone,
                    &head_commit
                ));
                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_history_is_not_complete_shallow_repo() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let shallow_clone =
                    command::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                // Get head commit of deep_clone repo
                let head_commit = api::local::commits::head_commit(&shallow_clone)?;
                assert!(!api::local::commits::commit_history_is_complete(
                    &shallow_clone,
                    &head_commit
                ));
                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }
}
