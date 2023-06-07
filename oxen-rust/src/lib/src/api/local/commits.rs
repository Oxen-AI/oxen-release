//! # Local Commits
//!
//! Interact with local commits.
//!

use crate::api;
use crate::core::index::{CommitEntryReader, CommitReader, CommitWriter, RefReader, Stager};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, StagedData};
use crate::opts::LogOpts;

use rayon::prelude::*;
use std::path::Path;

// Iterate over commits and get the one with the latest timestamp
pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let reader = CommitReader::new(repo)?;
    reader.latest_commit()
}

pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let reader = CommitReader::new(repo)?;
    reader.head_commit()
}

pub fn get_by_id(repo: &LocalRepository, commit_id: &str) -> Result<Option<Commit>, OxenError> {
    let reader = CommitReader::new(repo)?;
    reader.get_commit_by_id(commit_id)
}

pub fn get_by_id_or_branch(
    repo: &LocalRepository,
    branch_or_commit: &str,
) -> Result<Option<Commit>, OxenError> {
    log::debug!(
        "get_by_id_or_branch checking commit id {} in {:?}",
        branch_or_commit,
        repo.path
    );
    let ref_reader = RefReader::new(repo)?;
    let commit_id = match ref_reader.get_commit_id_for_branch(branch_or_commit)? {
        Some(branch_commit_id) => branch_commit_id,
        None => String::from(branch_or_commit),
    };
    log::debug!(
        "get_by_id_or_branch resolved commit id {} -> {}",
        branch_or_commit,
        commit_id
    );
    let reader = CommitReader::new(repo)?;
    reader.get_commit_by_id(commit_id)
}

/// Current head commit
pub fn get_head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let committer = CommitReader::new(repo)?;
    committer.head_commit()
}

/// Get the root commit
pub fn root_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let committer = CommitReader::new(repo)?;
    let commit = committer.root_commit()?;
    Ok(commit)
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

pub fn commit_with_no_files(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    let mut status = StagedData::empty();
    let commit = commit(repo, &mut status, message)?;
    println!("Initial commit {}", commit.id);
    Ok(commit)
}

pub fn commit(
    repo: &LocalRepository,
    status: &mut StagedData,
    message: &str,
) -> Result<Commit, OxenError> {
    let stager = Stager::new(repo)?;
    let commit_writer = CommitWriter::new(repo)?;
    let commit = commit_writer.commit(status, message)?;
    stager.unstage()?;
    Ok(commit)
}

pub fn create_commit_object(
    repo_dir: &Path,
    branch_name: impl AsRef<str>,
    commit: &Commit,
) -> Result<(), OxenError> {
    log::debug!("Create commit obj: {} -> '{}'", commit.id, commit.message);

    // Instantiate repo from dir
    let repo = LocalRepository::from_dir(repo_dir)?;

    // If we have a root, and we are trying to push a new one, don't allow it
    if let Ok(root) = root_commit(&repo) {
        if commit.parent_ids.is_empty() && root.id != commit.id {
            log::error!("Root commit does not match {} != {}", root.id, commit.id);
            return Err(OxenError::root_commit_does_not_match(commit.to_owned()));
        }
    }

    let result = CommitWriter::new(&repo);
    match result {
        Ok(commit_writer) => match commit_writer.add_commit_to_db(commit) {
            Ok(_) => {
                log::debug!("Successfully added commit [{}] to db", commit.id);
                api::local::branches::update(&repo, branch_name.as_ref(), &commit.id)?;
            }
            Err(err) => {
                log::error!("Error adding commit to db: {:?}", err);
            }
        },
        Err(err) => {
            log::error!("Error creating commit writer: {:?}", err);
        }
    };
    Ok(())
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
        let committish = if let Some(committish) = &opts.committish {
            committish.to_owned()
        } else {
            api::local::branches::current_branch(repo)?.unwrap().name
        };
        let commits = api::remote::commits::list_commit_history(&remote_repo, &committish).await?;
        Ok(commits)
    } else {
        let committer = CommitReader::new(repo)?;

        let commits = if let Some(committish) = &opts.committish {
            let commit = get_by_id_or_branch(repo, committish)?.ok_or(
                OxenError::committish_not_found(committish.to_string().into()),
            )?;
            committer.history_from_commit_id(&commit.id)?
        } else {
            committer.history_from_head()?
        };
        Ok(commits)
    }
}

/// # List the history for a specific branch or commit
pub fn list_from(repo: &LocalRepository, commit_or_branch: &str) -> Result<Vec<Commit>, OxenError> {
    log::debug!("log_commit_or_branch_history: {}", commit_or_branch);
    let committer = CommitReader::new(repo)?;
    if commit_or_branch.contains("..") {
        // This is BASE..HEAD format, and we only want to history from BASE to HEAD
        let split: Vec<&str> = commit_or_branch.split("..").collect();
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

    let commit_id = match api::local::branches::get_commit_id(repo, commit_or_branch)? {
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
