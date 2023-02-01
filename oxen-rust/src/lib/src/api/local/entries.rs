
use crate::error::OxenError;

use crate::index::CommitDirReader;
use crate::model::{
    Commit, CommitEntry, DirEntry, LocalRepository
};
use std::path::{Path};

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

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::error::OxenError;
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
