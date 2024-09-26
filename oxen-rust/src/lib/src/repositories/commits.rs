//! # Commits
//!
//! Create, read, and list commits
//!

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, MerkleHash};
use crate::opts::PaginateOpts;
use crate::util;
use crate::view::{PaginatedCommits, StatusMessage};
use crate::{core, resource};

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

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
/// let base_dir = Path::new("repo_dir_commit");
/// let repo = repositories::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// repositories::add(&repo, &hello_file)?;
///
/// // Commit staged
/// repositories::commit(&repo, "My commit message")?;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn commit(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::commit(repo, message),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::commit(repo, message),
    }
}

/// Iterate over all commits and get the one with the latest timestamp
pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::latest_commit(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::latest_commit(repo),
    }
}

/// The current HEAD commit of the branch you currently have checked out
pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::head_commit(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::head_commit(repo),
    }
}

/// Maybe get the head commit if it exists
/// Returns None if the head commit does not exist (empty repo)
pub fn head_commit_maybe(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::head_commit_maybe(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::head_commit_maybe(repo),
    }
}

/// Get the root commit of a repository
pub fn root_commit_maybe(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let root_commit = core::v0_10_0::commits::root_commit(repo)?;
            Ok(Some(root_commit))
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::root_commit_maybe(repo),
    }
}

/// Get a commit by it's MerkleHash
pub fn get_by_hash(repo: &LocalRepository, hash: &MerkleHash) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::get_by_id(repo, &hash.to_string()),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::get_by_hash(repo, hash),
    }
}

/// Get a commit by it's string hash
pub fn get_by_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let commit_id = commit_id.as_ref();
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::get_by_id(repo, commit_id),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::get_by_id(repo, commit_id),
    }
}

/// Commit id exists
pub fn commit_id_exists(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<bool, OxenError> {
    get_by_id(repo, commit_id.as_ref()).map(|commit| commit.is_some())
}

/// List commits on the current branch from HEAD
pub fn list(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::list(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list(repo),
    }
}

/// List commits for the repository in no particular order
pub fn list_all(repo: &LocalRepository) -> Result<HashSet<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::list_all(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_all(repo),
    }
}

// Source
pub fn get_commit_or_head<S: AsRef<str> + Clone>(
    repo: &LocalRepository,
    commit_id_or_branch_name: Option<S>,
) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => resource::get_commit_or_head(repo, commit_id_or_branch_name),
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::commits::get_commit_or_head(repo, commit_id_or_branch_name)
        }
    }
}

pub fn list_all_paginated(
    repo: &LocalRepository,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    log::info!("list_all_paginated: {:?} {:?}", repo.path, pagination);
    let commits = list_all(repo)?;
    let commits: Vec<Commit> = commits.into_iter().collect();
    let (commits, pagination) = util::paginate(commits, pagination.page_num, pagination.page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}

/// List the history for a specific branch or commit (revision)
pub fn list_from(repo: &LocalRepository, revision: &str) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::list_from(repo, revision),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_from(repo, revision),
    }
}
pub fn list_from_with_depth(
    repo: &LocalRepository,
    revision: &str,
) -> Result<HashMap<Commit, usize>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => Err(OxenError::basic_str(
            "list_from_with_depth not supported in v0.10.0",
        )),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_from_with_depth(repo, revision),
    }
}

/// List the history between two commits
pub fn list_between(
    repo: &LocalRepository,
    start: &Commit,
    end: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::list_between(repo, start, end),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_between(repo, start, end),
    }
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
    let commits = list(repo)?;
    Ok(commits
        .into_iter()
        .find(|commit| commit.message == msg.as_ref()))
}

/// List all the commits that have missing entries
/// Useful for knowing which commits to resend
pub fn list_with_missing_entries(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::commits::list_with_missing_entries(repo, commit_id)
        }
        MinOxenVersion::V0_19_0 => {
            panic!("list_with_missing_entries not needed in v0.19.0");
        }
    }
}

/// Retrieve entries with filepaths matching a provided glob pattern
pub fn search_entries(
    repo: &LocalRepository,
    commit: &Commit,
    pattern: &str,
) -> Result<HashSet<PathBuf>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::search_entries(repo, commit, pattern),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::search_entries(repo, commit, pattern),
    }
}

/// List paginated commits starting from the given revision
pub fn list_from_paginated(
    repo: &LocalRepository,
    revision: &str,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    let commits = list_from(repo, revision)?;
    let (commits, pagination) = util::paginate(commits, pagination.page_num, pagination.page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}

/// List paginated commits by resource
pub fn list_by_path_from_paginated(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    log::info!("list_by_path_from_paginated: {:?} {:?}", commit, path);
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::commits::list_by_path_from_paginated(repo, commit, path, pagination)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::commits::list_by_path_from_paginated(repo, commit, path, pagination)
        }
    }
}

// TODO: Temporary function until after v0.19.0, we shouldn't need this check
// once everything is working off the Merkle tree
pub fn get_commit_status_tmp(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Option<core::v0_10_0::cache::cacher_status::CacherStatusType>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::cache::commit_cacher::get_status(&repo, &commit),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::get_commit_status_tmp(repo, commit),
    }
}

// TODO: Temporary function until after v0.19.0, we shouldn't need this check
// once everything is working off the Merkle tree
pub fn is_commit_valid_tmp(repo: &LocalRepository, commit: &Commit) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::cache::cachers::content_validator::is_valid(&repo, &commit)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::is_commit_valid_tmp(repo, commit),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::command;
    use crate::core::v0_10_0::index::CommitEntryReader;
    use crate::error::OxenError;
    use crate::model::StagedEntryStatus;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_commit_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            repositories::add(&repo, &hello_file)?;
            // Commit the file
            let commit = repositories::commit(&repo, "My message")?;
            assert_eq!(commit.message, "My message");

            // Get status and make sure it is removed from the untracked and added
            let repo_status = repositories::status(&repo)?;
            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_commit_removed_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            repositories::add(&repo, &hello_file)?;

            // Remove the file
            util::fs::remove_file(&hello_file)?;

            // Commit the file
            repositories::commit(&repo, "My message")?;

            // Get status and make sure the file was not committed
            let head = repositories::commits::head_commit(&repo)?;
            let commit_reader = CommitEntryReader::new(&repo, &head)?;
            let commit_list = commit_reader.list_files()?;
            assert_eq!(commit_list.len(), 0);

            // Test subsequent commit
            let goodbye_file = repo.path.join("goodbye.txt");
            util::fs::write_to_path(&goodbye_file, "Goodbye World")?;

            repositories::add(&repo, &goodbye_file)?;

            util::fs::remove_file(&goodbye_file)?;

            repositories::commit(&repo, "Second Message")?;

            Ok(())
        })
    }

    #[test]
    fn test_command_commit_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Track the file
            let train_dir = repo.path.join("train");
            repositories::add(&repo, train_dir)?;
            // Commit the file
            repositories::commit(&repo, "Adding training data")?;

            let repo_status = repositories::status(&repo)?;
            repo_status.print();
            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 2);
            assert_eq!(repo_status.untracked_dirs.len(), 4);

            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_command_commit_dir_recursive() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Track the annotations dir, which has sub dirs
            let annotations_dir = repo.path.join("annotations");
            repositories::add(&repo, annotations_dir)?;
            repositories::commit(&repo, "Adding annotations data dir, which has two levels")?;

            let repo_status = repositories::status(&repo)?;
            repo_status.print();

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 2);
            assert_eq!(repo_status.untracked_dirs.len(), 4);

            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 2);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_command_commit_top_level_dir_then_revert() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a branch to make the changes
            let branch_name = "feature/adding-train";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Track & commit (train dir already created in helper)
            let train_path = repo.path.join("train");
            let og_num_files = util::fs::rcount_files_in_dir(&train_path);

            // Add directory
            repositories::add(&repo, &train_path)?;
            // Make sure we can get the status
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 1);

            // Commit changes
            repositories::commit(&repo, "Adding train dir")?;
            // Make sure we can get the status and they are no longer added
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 0);

            // checkout OG and make sure it removes the train dir
            repositories::checkout(&repo, orig_branch.name).await?;
            assert!(!train_path.exists());

            // checkout branch again and make sure it reverts
            repositories::checkout(&repo, branch_name).await?;
            assert!(train_path.exists());
            assert_eq!(util::fs::rcount_files_in_dir(&train_path), og_num_files);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_commit_second_level_dir_then_revert() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a branch to make the changes
            let branch_name = "feature/adding-annotations";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Track & commit (dir already created in helper)
            let new_dir_path = repo.path.join("annotations").join("train");
            let og_num_files = util::fs::rcount_files_in_dir(&new_dir_path);

            repositories::add(&repo, &new_dir_path)?;
            repositories::commit(&repo, "Adding train dir")?;

            // checkout OG and make sure it removes the train dir
            repositories::checkout(&repo, orig_branch.name).await?;
            assert!(!new_dir_path.exists());

            // checkout branch again and make sure it reverts
            repositories::checkout(&repo, branch_name).await?;
            assert!(new_dir_path.exists());
            assert_eq!(util::fs::rcount_files_in_dir(&new_dir_path), og_num_files);

            Ok(())
        })
        .await
    }

    #[test]
    fn test_command_commit_removed_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (dir already created in helper)
            let dir_to_remove = repo.path.join("train");
            let og_file_count = util::fs::rcount_files_in_dir(&dir_to_remove);

            repositories::add(&repo, &dir_to_remove)?;
            repositories::commit(&repo, "Adding train directory")?;

            // Delete the directory
            util::fs::remove_dir_all(&dir_to_remove)?;

            // Add the deleted dir, so that we can commit the deletion
            repositories::add(&repo, &dir_to_remove)?;

            // Make sure we have the correct amount of files tagged as removed
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), og_file_count);
            assert_eq!(
                status.staged_files.iter().next().unwrap().1.status,
                StagedEntryStatus::Removed
            );

            // Make sure they don't show up in the status
            assert_eq!(status.removed_files.len(), 0);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_commit_after_merge_conflict() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            let labels_path = repo.path.join("labels.txt");
            repositories::add(&repo, &labels_path)?;
            repositories::commit(&repo, "adding initial labels file")?;

            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Add a "none" category on a branch
            let branch_name = "change-labels";
            repositories::branches::create_checkout(&repo, branch_name)?;

            test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
            repositories::add(&repo, &labels_path)?;
            repositories::commit(&repo, "adding none category")?;

            // Add a "person" category on a the main branch
            repositories::checkout(&repo, og_branch.name).await?;

            test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
            repositories::add(&repo, &labels_path)?;
            repositories::commit(&repo, "adding person category")?;

            // Try to merge in the changes
            command::merge(&repo, branch_name)?;

            // We should have a conflict
            let status = repositories::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            // Assume that we fixed the conflict and added the file
            let path = status.merge_conflicts[0].base_entry.path.clone();
            let fullpath = repo.path.join(path);
            repositories::add(&repo, fullpath)?;

            // Should commit, and then see full commit history
            repositories::commit(&repo, "merging into main")?;

            // Should have commits:
            //  1) initial
            //  2) add labels
            //  3) change-labels branch modification
            //  4) main branch modification
            //  5) merge commit
            let history = repositories::commits::list(&repo)?;
            assert_eq!(history.len(), 5);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_hash_on_modified_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Add a text file
            let text_path = repo.path.join("text.txt");
            util::fs::write_to_path(&text_path, "Hello World")?;

            // Get the hash of the file at this timestamp
            let hash_when_add = util::hasher::hash_file_contents(&text_path)?;
            repositories::add(&repo, &text_path)?;

            // Modify the text file
            util::fs::write_to_path(&text_path, "Goodbye, world!")?;

            // Get the new hash
            let hash_after_modification = util::hasher::hash_file_contents(&text_path)?;

            // Commit the file
            repositories::commit(&repo, "My message")?;

            // Get the most recent commit - the new head commit
            let head = repositories::commits::head_commit(&repo)?;

            // Initialize a commit entry reader here
            let commit_reader = CommitEntryReader::new(&repo, &head)?;

            // Get the commit entry for the text file
            let text_entry = commit_reader.get_entry(Path::new("text.txt"))?.unwrap();

            // Hashes should be different
            assert_ne!(hash_when_add, hash_after_modification);

            // Hash should match new hash
            assert_eq!(text_entry.hash, hash_after_modification);

            Ok(())
        })
    }
}
