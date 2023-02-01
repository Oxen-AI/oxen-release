use time::OffsetDateTime;

use crate::error::OxenError;
use crate::index::{CommitDirReader, CommitWriter, RefWriter};
use crate::model::{
    Branch, Commit, CommitEntry, DirEntry, LocalRepository, NewCommit, StagedData, StagedEntry,
    StagedEntryStatus, User,
};
use crate::util;

use std::path::{Path, PathBuf};

pub fn get_entry_for_commit(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<Option<CommitEntry>, OxenError> {
    let reader = CommitDirReader::new(repo, commit)?;
    reader.get_entry(path)
}

pub fn list_all(repo: &LocalRepository, commit: &Commit) -> Result<Vec<CommitEntry>, OxenError> {
    let reader = CommitDirReader::new(repo, commit)?;
    reader.list_entries()
}

pub fn count_for_commit(repo: &LocalRepository, commit: &Commit) -> Result<usize, OxenError> {
    let reader = CommitDirReader::new(repo, commit)?;
    reader.num_entries()
}

pub fn list_page(
    repo: &LocalRepository,
    commit: &Commit,
    page: &usize,
    page_size: &usize,
) -> Result<Vec<CommitEntry>, OxenError> {
    let reader = CommitDirReader::new(repo, commit)?;
    reader.list_entry_page(*page, *page_size)
}

pub fn list_directory(
    repo: &LocalRepository,
    commit: &Commit,
    branch_or_commit_id: &str,
    directory: &Path,
    page: &usize,
    page_size: &usize,
) -> Result<(Vec<DirEntry>, usize), OxenError> {
    let reader = CommitDirReader::new(repo, commit)?;
    reader.list_directory(directory, branch_or_commit_id, *page, *page_size)
}

// TODO: rip this logic out... instead we are going to use remote_stager to do the same process we do locally

/*
pub fn append_to_and_commit_entry_on_branch(
    repo: &LocalRepository,
    base_branch: &Branch,
    entry: &CommitEntry,
    user: &User,
    message: &str,
    data: &str,
) -> Result<Branch, OxenError> {
    // We generate a branch postfix so that each name is unique
    // Since this call is meant to be run on the server, many people could append at once
    // So given a branch name "collect-data"
    // We generate a sub-branch name "collect-data/UUID" and return this to the user
    // This way we can later filter by the prefix, and no one's data conflicts

    // We will have a queue that cleans up these branches and merges them into the base branch
    // Add the timestamp at the front so that we can sequentially apply the changes
    let timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let branch_postfix = format!("{}_{}", timestamp, uuid::Uuid::new_v4());
    log::debug!("Appending to entry {} on branch: {}", entry.path.display(), branch_postfix);
    match append_data(repo, entry, data, &branch_postfix) {
        Ok(tmp_file) => {
            let tmp_branch = format!("{}/{}", base_branch.name, branch_postfix);
            commit_tmp_file(repo, base_branch, entry, &tmp_file, user, message, &tmp_branch)
        },
        Err(err) => {
            Err(err)
        }
    }
}

fn append_data(
    repo: &LocalRepository,
    entry: &CommitEntry,
    data: &str,
    uuid: &str,
) -> Result<PathBuf, OxenError> {
    let version_path = util::fs::version_path(repo, entry);
    // Generate random uuid for tmp file
    let tmp_path = repo.path.join(PathBuf::from(uuid));
    if util::fs::is_tabular(&version_path) {
        append_to_tabular(repo, entry, &tmp_path, data)
    } else if util::fs::is_utf8(&version_path) {
        append_to_utf8(&version_path, &tmp_path, data)
    } else {
        Err(OxenError::basic_str("Cannot append to file unless is of type 'tabular' or 'text'"))
    }
}

fn append_to_tabular(
    repo: &LocalRepository,
    entry: &CommitEntry,
    tmp_path: &Path,
    data: &str
) -> Result<PathBuf, OxenError> {
    Ok(tmp_path.to_path_buf())
}

fn append_to_utf8(
    version_path: &Path,
    tmp_path: &Path,
    data: &str
) -> Result<PathBuf, OxenError> {
    use std::fs::OpenOptions;
    use std::io::prelude::*;

    // Copy to tmp path
    std::fs::copy(version_path, tmp_path)?;
    // Append
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(tmp_path)?;
    write!(file, "{}", data)?;

    // Return appended path
    Ok(tmp_path.to_path_buf())
}

fn commit_tmp_file(
    repo: &LocalRepository,
    base_branch: &Branch,
    entry: &CommitEntry,
    tmp_path: &Path,
    user: &User,
    message: &str,
    branch_name: &str,
) -> Result<Branch, OxenError> {
    // Create the new branch to add data to
    create_new_branch(repo, base_branch, branch_name)?;

    // Create a new commit based off of the base branch commit_id
    let commit_writer = CommitWriter::new(repo)?;
    let timestamp = OffsetDateTime::now_utc();
    let new_commit = NewCommit {
        parent_ids: vec![base_branch.commit_id.to_owned()],
        message: message.to_string(),
        author: user.name.to_owned(),
        email: user.email.to_owned(),
        timestamp: timestamp
    };

    // Create "staged data" which is really just the file we want to commit
    let mut staged_data = StagedData::empty();
    let hash = util::hasher::hash_file_contents(tmp_path)?;
    let staged_entry = StagedEntry {
        hash: hash.clone(),
        status: StagedEntryStatus::Modified,
        tmp_file: Some(tmp_path.to_path_buf())
    };
    staged_data.added_files.insert(entry.path.to_owned(), staged_entry);

    // Create commit
    let commit = commit_writer.commit_from_new(&new_commit, &staged_data)?;

    // Update branch to new commit id
    update_new_branch_commit_id(repo, branch_name, &commit.id)?;

    // Clean up tmp file
    log::debug!("Removing tmp file: {}", tmp_path.display());
    std::fs::remove_file(tmp_path)?;

    Ok(Branch {
        name: branch_name.to_string(),
        commit_id: commit.id.to_owned(),
        is_head: false
    })
}

fn create_new_branch(
    repo: &LocalRepository,
    base_branch: &Branch,
    branch_name: &str,
) -> Result<Branch, OxenError> {
    // TODO: Might have to lock this DB and wait for it to unlock...if many requests come at once
    // Create new branch off of
    let ref_writer = RefWriter::new(repo)?;
    ref_writer.create_branch(branch_name, &base_branch.commit_id)
}

fn update_new_branch_commit_id(
    repo: &LocalRepository,
    branch_name: &str,
    commit_id: &str,
) -> Result<(), OxenError> {
    // TODO: Might have to lock this DB and wait for it to unlock...if many requests come at once
    // Create new branch off of
    let ref_writer = RefWriter::new(repo)?;
    ref_writer.set_branch_commit_id(branch_name, commit_id)
}
*/

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::model::User;
    use crate::test;
    use crate::util;

    #[test]
    fn test_api_local_entries_list_all() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (file already created in helper)
            let file_to_add = repo.path.join("labels.txt");

            // Commit the file
            command::add(&repo, file_to_add)?;
            let commit = command::commit(&repo, "Adding labels file")?.unwrap();

            let entries = api::local::entries::list_all(&repo, &commit)?;
            assert_eq!(entries.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_api_local_entries_count_one_for_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (file already created in helper)
            let file_to_add = repo.path.join("labels.txt");

            // Commit the file
            command::add(&repo, file_to_add)?;
            let commit = command::commit(&repo, "Adding labels file")?.unwrap();

            let count = api::local::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, 1);

            Ok(())
        })
    }

    #[test]
    fn test_api_local_entries_count_many_for_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (files already created in helper)
            let dir_to_add = repo.path.join("train");
            let num_files = util::fs::rcount_files_in_dir(&dir_to_add);

            // Commit the dir
            command::add(&repo, &dir_to_add)?;
            let commit = command::commit(&repo, "Adding training data")?.unwrap();
            let count = api::local::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, num_files);

            Ok(())
        })
    }

    #[test]
    fn test_api_local_entries_count_many_dirs() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (files already created in helper)
            let num_files = util::fs::rcount_files_in_dir(&repo.path);

            // Commit the dir
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all data")?.unwrap();

            let count = api::local::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, num_files);

            Ok(())
        })
    }
}
