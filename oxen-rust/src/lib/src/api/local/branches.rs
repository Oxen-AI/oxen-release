//! # Local Branches
//!
//! Interact with branches on your local machine.
//!

use crate::api;
use crate::core::index::{CommitReader, CommitWriter, EntryIndexer, RefReader, RefWriter};
use crate::error::OxenError;
use crate::model::{Branch, Commit, LocalRepository, RemoteBranch};

/// List all the local branches within a repo
pub fn list(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branches = ref_reader.list_branches()?;
    Ok(branches)
}

/// Get a branch by name
pub fn get_by_name(repo: &LocalRepository, name: &str) -> Result<Option<Branch>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    ref_reader.get_branch_by_name(name)
}

/// Get branch by name or fall back the current
pub fn get_by_name_or_current(
    repo: &LocalRepository,
    branch_name: Option<&str>,
) -> Result<Branch, OxenError> {
    if let Some(branch_name) = branch_name {
        match api::local::branches::get_by_name(repo, branch_name)? {
            Some(branch) => Ok(branch),
            None => Err(OxenError::local_branch_not_found(branch_name)),
        }
    } else {
        match api::local::branches::current_branch(repo)? {
            Some(branch) => Ok(branch),
            None => Err(OxenError::must_be_on_valid_branch()),
        }
    }
}

/// Get commit id from a branch by name
pub fn get_commit_id(repo: &LocalRepository, name: &str) -> Result<Option<String>, OxenError> {
    match RefReader::new(repo) {
        Ok(ref_reader) => ref_reader.get_commit_id_for_branch(name),
        _ => Err(OxenError::basic_str("Could not read reference for repo.")),
    }
}

/// Check if a branch exists
pub fn exists(repo: &LocalRepository, name: &str) -> Result<bool, OxenError> {
    match get_by_name(repo, name)? {
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

/// Get the current branch
pub fn current_branch(repo: &LocalRepository) -> Result<Option<Branch>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branch = ref_reader.get_current_branch()?;
    Ok(branch)
}

/// # Create a new branch from the head commit
/// This creates a new pointer to the current commit with a name,
/// it does not switch you to this branch, you still must call `checkout_branch`
pub fn create_from_head(repo: &LocalRepository, name: &str) -> Result<Branch, OxenError> {
    let ref_writer = RefWriter::new(repo)?;
    let commit_reader = CommitReader::new(repo)?;
    let head_commit = commit_reader.head_commit()?;
    ref_writer.create_branch(name, &head_commit.id)
}

/// # Create a local branch from a specific commit id
pub fn create(repo: &LocalRepository, name: &str, commit_id: &str) -> Result<Branch, OxenError> {
    let ref_writer = RefWriter::new(repo)?;
    let commit_reader = CommitReader::new(repo)?;
    if commit_reader.commit_id_exists(commit_id) {
        ref_writer.create_branch(name, commit_id)
    } else {
        Err(OxenError::commit_id_does_not_exist(commit_id))
    }
}

/// # Create a branch and check it out in one go
/// This creates a branch with name,
/// then switches HEAD to point to the branch
pub fn create_checkout(repo: &LocalRepository, name: &str) -> Result<Branch, OxenError> {
    println!("Create and checkout branch: {name}");
    let head_commit = api::local::commits::head_commit(repo)?;
    let ref_writer = RefWriter::new(repo)?;

    let branch = ref_writer.create_branch(name, &head_commit.id)?;
    ref_writer.set_head(name);
    Ok(branch)
}

/// Update the branch name to point to a commit id
pub fn update(repo: &LocalRepository, name: &str, commit_id: &str) -> Result<Branch, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    match ref_reader.get_branch_by_name(name)? {
        Some(branch) => {
            // Set the branch to point to the commit
            let ref_writer = RefWriter::new(repo)?;
            match ref_writer.set_branch_commit_id(name, commit_id) {
                Ok(()) => Ok(branch),
                Err(err) => Err(err),
            }
        }
        None => create(repo, name, commit_id),
    }
}

pub fn delete(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    if let Ok(Some(branch)) = current_branch(repo) {
        if branch.name == name {
            let err = format!("Err: Cannot delete current checked out branch '{name}'");
            return Err(OxenError::basic_str(err));
        }
    }

    if branch_has_been_merged(repo, name)? {
        let ref_writer = RefWriter::new(repo)?;
        ref_writer.delete_branch(name)
    } else {
        let err = format!("Err: The branch '{name}' is not fully merged.\nIf you are sure you want to delete it, run 'oxen branch -D {name}'.");
        Err(OxenError::basic_str(err))
    }
}

/// # Force delete a local branch
/// Caution! Will delete a local branch without checking if it has been merged or pushed.
pub fn force_delete(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    if let Ok(Some(branch)) = current_branch(repo) {
        if branch.name == name {
            let err = format!("Err: Cannot delete current checked out branch '{name}'");
            return Err(OxenError::basic_str(err));
        }
    }

    let ref_writer = RefWriter::new(repo)?;
    ref_writer.delete_branch(name)
}

pub fn is_checked_out(repo: &LocalRepository, name: &str) -> bool {
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

pub async fn set_working_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    let branch = api::local::branches::get_by_name(repo, name)?
        .ok_or(OxenError::local_branch_not_found(name))?;
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(&branch.commit_id))?;

    // Sync changes if needed
    maybe_pull_missing_entries(repo, &commit).await?;

    let commit_writer = CommitWriter::new(repo)?;
    commit_writer.set_working_repo_to_commit(&commit).await
}

async fn maybe_pull_missing_entries(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    // If we don't have a remote, there are not missing entries, so return
    let rb = RemoteBranch::default();
    let remote = repo.get_remote(&rb.remote);
    if remote.is_none() {
        log::debug!("No remote, no missing entries to fetch");
        return Ok(());
    }

    // Safe to unwrap now.
    let remote = remote.unwrap();

    match api::remote::repositories::get_by_remote(&remote).await {
        Ok(Some(remote_repo)) => {
            let indexer = EntryIndexer::new(repo)?;
            indexer
                .pull_all_entries_for_commit(&remote_repo, commit)
                .await?;
        }
        Ok(None) => {
            log::debug!("No remote repo found, no entries to fetch");
        }
        Err(err) => {
            log::error!("Error getting remote repo: {}", err);
        }
    };

    Ok(())
}

pub async fn set_working_commit_id(
    repo: &LocalRepository,
    commit_id: &str,
) -> Result<(), OxenError> {
    let commit = api::local::commits::get_by_id(repo, commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(commit_id))?;

    let commit_writer = CommitWriter::new(repo)?;
    commit_writer.set_working_repo_to_commit(&commit).await
}

pub fn set_head(repo: &LocalRepository, value: &str) -> Result<(), OxenError> {
    let ref_writer = RefWriter::new(repo)?;
    ref_writer.set_head(value);
    Ok(())
}

fn branch_has_been_merged(repo: &LocalRepository, name: &str) -> Result<bool, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let commit_reader = CommitReader::new(repo)?;

    if let Some(branch_commit_id) = ref_reader.get_commit_id_for_branch(name)? {
        if let Some(commit_id) = ref_reader.head_commit_id()? {
            let history = commit_reader.history_from_commit_id(&commit_id)?;
            for commit in history.iter() {
                if commit.id == branch_commit_id {
                    return Ok(true);
                }
            }
            // We didn't find commit
            Ok(false)
        } else {
            // Cannot check if it has been merged if we are in a detached HEAD state
            Ok(false)
        }
    } else {
        let err = format!("Err: The branch '{name}' does not exist.");
        Err(OxenError::basic_str(err))
    }
}

pub fn rename_current_branch(repo: &LocalRepository, new_name: &str) -> Result<(), OxenError> {
    if let Ok(Some(branch)) = current_branch(repo) {
        let ref_writer = RefWriter::new(repo)?;
        ref_writer.rename_branch(&branch.name, new_name)?;
        ref_writer.set_head(new_name);
        Ok(())
    } else {
        Err(OxenError::must_be_on_valid_branch())
    }
}
