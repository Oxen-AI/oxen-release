//! # Local Commits
//!
//! Interact with local commits.
//!

use crate::constants::{HISTORY_DIR, OXEN_HIDDEN_DIR};
use crate::core::cache::cacher_status::CacherStatusType;
use crate::core::cache::cachers::content_validator;
use crate::core::cache::commit_cacher;
use crate::core::index::{CommitEntryReader, CommitReader, CommitWriter, RefReader, Stager};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, StagedData};
use crate::opts::LogOpts;
use crate::util::fs::commit_content_is_valid_path;
use crate::view::{PaginatedCommits, StatusMessage};
use crate::{api, util};

use rayon::prelude::*;
use std::path::Path;

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

pub fn list_with_missing_dbs(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    let mut missing_db_commits: Vec<Commit> = vec![];

    // Get full commit history for this repo to report any missing commits
    let commits = api::local::commits::list_all(repo)?;
    for commit in commits {
        if !commit_history_db_exists(repo, &commit)? {
            missing_db_commits.push(commit);
        }
    }

    Ok(missing_db_commits)
}

pub fn list_with_missing_entries(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    log::debug!("In here working on finding some commit entries");
    let mut missing_entry_commits: Vec<Commit> = vec![];

    // Get full commit history for this repo to report any missing commits
    let commits = api::local::commits::list_all(repo)?;

    for commit in commits {
        if commit_content_is_valid_path(&repo, &commit).exists()
            && content_validator::is_valid(&repo, &commit)?
        {
            continue;
        }
        missing_entry_commits.push(commit);
    }
    Ok(missing_entry_commits)
}

pub fn commit_history_db_exists(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<bool, OxenError> {
    // Check if OXEN_HIDDEN_DIR/HISTORY_DIR/commit_id exists
    let commit_history_dir = util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id);
    Ok(commit_history_dir.exists())
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
