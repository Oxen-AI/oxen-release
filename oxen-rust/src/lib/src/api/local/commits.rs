use crate::command;
use crate::error::OxenError;
use crate::index::{CommitDirReader, CommitReader, CommitWriter, RefReader, Stager};
use crate::model::{Commit, CommitEntry, LocalRepository, StagedData};

use std::path::Path;

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
        "get_by_id_or_branch resolved commit id {}",
        branch_or_commit
    );
    let reader = CommitReader::new(repo)?;
    reader.get_commit_by_id(commit_id)
}

pub fn get_head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let committer = CommitReader::new(repo)?;
    committer.head_commit()
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
    let reader = CommitDirReader::new(repo, commit)?;
    let entries = reader.list_entries()?;
    compute_entries_size(&entries)
}

pub fn compute_entries_size(entries: &[CommitEntry]) -> Result<u64, OxenError> {
    let mut total_size: u64 = 0;

    for entry in entries.iter() {
        total_size += entry.num_bytes;
    }
    Ok(total_size)
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

pub fn create_commit_object(repo_dir: &Path, commit: &Commit) -> Result<(), OxenError> {
    // Instantiate repo from dir
    let repo = LocalRepository::from_dir(repo_dir)?;

    // If we have a root, and we are trying to push a new one, don't allow it
    if let Ok(root) = command::root_commit(&repo) {
        if commit.parent_ids.is_empty() && root.id != commit.id {
            log::error!("Root commit does not match {} != {}", root.id, commit.id);
            return Err(OxenError::RootCommitDoesNotMatch(commit.id.to_owned()));
        }
    }

    // Check parent ids to make sure they are synced, otherwise we should not add to tree
    for id in commit.parent_ids.iter() {
        if get_by_id_or_branch(&repo, id)?.is_none() {
            return Err(OxenError::basic_str("Parent commit not found"));
        }
    }

    let result = CommitWriter::new(&repo);
    match result {
        Ok(commit_writer) => match commit_writer.add_commit_to_db(commit) {
            Ok(_) => {
                log::debug!("Successfully added commit [{}] to db", commit.id);
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
