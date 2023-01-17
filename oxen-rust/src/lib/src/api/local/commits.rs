use crate::error::OxenError;
use crate::index::{CommitDirReader, CommitReader, RefReader};
use crate::model::{Commit, CommitEntry, LocalRepository};

pub fn get_by_id(repo: &LocalRepository, commit_id: &str) -> Result<Option<Commit>, OxenError> {
    let reader = CommitReader::new(repo)?;
    reader.get_commit_by_id(commit_id)
}

pub fn get_by_id_or_branch(
    repo: &LocalRepository,
    branch_or_commit: &str,
) -> Result<Option<Commit>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let commit_id = match ref_reader.get_commit_id_for_branch(branch_or_commit)? {
        Some(branch_commit_id) => branch_commit_id,
        None => String::from(branch_or_commit),
    };
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
