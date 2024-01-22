//! # Local Branches
//!
//! Interact with branches on your local machine.
//!

use std::collections::HashSet;
use std::path::Path;

use crate::constants::{BRANCH_LOCKS_DIR, OXEN_HIDDEN_DIR};
use crate::core::index::{
    CommitEntryReader, CommitReader, CommitWriter, EntryIndexer, RefReader, RefWriter,
};
use crate::error::OxenError;
use crate::model::{Branch, Commit, CommitEntry, LocalRepository, RemoteBranch};
use crate::{api, util};

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

pub fn lock(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    // Errors if lock exists - to avoid double-request ("is_locked" -> if false "lock")
    let oxen_dir = repo.path.join(OXEN_HIDDEN_DIR);
    let locks_dir = oxen_dir.join(BRANCH_LOCKS_DIR);

    let clean_name = branch_name_no_slashes(name);
    let branch_lock_file = locks_dir.join(clean_name);
    log::debug!(
        "Locking branch: {} to path {}",
        name,
        branch_lock_file.display()
    );

    if branch_lock_file.exists() || api::local::repositories::is_locked(repo) {
        return Err(OxenError::remote_branch_locked());
    }

    // If the branch exists, get the current head commit and lock it as the current "latest commit"
    // during the lifetime of the push operation.
    let maybe_branch = api::local::branches::get_by_name(repo, name)?;

    let maybe_latest_commit;
    if let Some(branch) = maybe_branch {
        maybe_latest_commit = branch.commit_id;
    } else {
        maybe_latest_commit = "branch being created".to_string();
    }

    // Create locks dir if needed
    if !locks_dir.exists() {
        util::fs::create_dir_all(&locks_dir)?;
    }

    util::fs::write_to_path(&branch_lock_file, maybe_latest_commit)?;
    Ok(())
}

pub fn is_locked(repo: &LocalRepository, name: &str) -> Result<bool, OxenError> {
    // Get the oxen hidden dir
    let oxen_dir = repo.path.join(OXEN_HIDDEN_DIR);
    let locks_dir = oxen_dir.join(BRANCH_LOCKS_DIR);

    // Create locks dir if not exists
    if !locks_dir.exists() {
        util::fs::create_dir_all(&locks_dir)?;
    }

    // Add a file with the branch name to the locks dir
    let clean_name = branch_name_no_slashes(name);
    let branch_lock_file = locks_dir.join(clean_name);
    log::debug!(
        "Checking if branch is locked: {} at path {}",
        name,
        branch_lock_file.display()
    );
    // Branch is locked if file eixsts
    Ok(branch_lock_file.exists())
}

pub fn read_lock_file(repo: &LocalRepository, name: &str) -> Result<String, OxenError> {
    // Get the oxen hidden dir
    let oxen_dir = repo.path.join(OXEN_HIDDEN_DIR);
    let locks_dir = oxen_dir.join(BRANCH_LOCKS_DIR);

    // Add a file with the branch name to the locks dir
    let clean_name = branch_name_no_slashes(name);
    let branch_lock_file = locks_dir.join(clean_name);
    log::debug!(
        "Reading lock file for branch: {} at path {}",
        name,
        branch_lock_file.display()
    );

    // Check if lock exists
    if !branch_lock_file.exists() {
        let err = format!("Err: Branch '{name}' is not locked.");
        return Err(OxenError::basic_str(err));
    }

    let contents = std::fs::read_to_string(branch_lock_file)?;
    Ok(contents)
}

pub fn latest_synced_commit(repo: &LocalRepository, name: &str) -> Result<Commit, OxenError> {
    // If branch is locked, we want to get the commit from the lockfile
    if is_locked(repo, name)? {
        let commit_id = read_lock_file(repo, name)?;
        let commit = api::local::commits::get_by_id(repo, &commit_id)?
            .ok_or(OxenError::commit_id_does_not_exist(&commit_id))?;
        return Ok(commit);
    }
    // If branch is not locked, we want to get the latest commit from the branch
    let branch = api::local::branches::get_by_name(repo, name)?
        .ok_or(OxenError::local_branch_not_found(name))?;
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(&branch.commit_id))?;
    Ok(commit)
}

pub fn unlock(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    // Get the oxen hidden dir
    let oxen_dir = repo.path.join(OXEN_HIDDEN_DIR);
    let locks_dir = oxen_dir.join(BRANCH_LOCKS_DIR);

    // Add a file with the branch name to the locks dir
    let clean_name = branch_name_no_slashes(name);
    let branch_lock_file = locks_dir.join(clean_name);
    log::debug!(
        "Unlocking branch: {} at path {}",
        name,
        branch_lock_file.display()
    );

    // Check if lock exists
    if !branch_lock_file.exists() {
        log::debug!("Branch is not locked, nothing to do");
        return Ok(());
    }

    util::fs::remove_file(&branch_lock_file)?;

    Ok(())
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
    println!("Checkout commit: {commit}");

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

// Traces through a branches history to list all unique versions of a file
pub fn list_entry_versions_on_branch(
    local_repo: &LocalRepository,
    branch_name: &str,
    path: &Path,
) -> Result<Vec<(Commit, CommitEntry)>, OxenError> {
    let branch = api::local::branches::get_by_name(local_repo, branch_name)?
        .ok_or(OxenError::local_branch_not_found(branch_name))?;
    let commit_reader = CommitReader::new(local_repo)?;

    let root_commit = commit_reader.root_commit()?;
    let mut branch_commits =
        commit_reader.history_from_base_to_head(&root_commit.id, &branch.commit_id)?;

    // Sort on timestamp oldest to newest
    branch_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    log::debug!(
        "got branch commits {:#?} for branch {:?}",
        branch_commits,
        branch.name
    );

    let mut result: Vec<(Commit, CommitEntry)> = Vec::new();
    let mut seen_hashes: HashSet<String> = HashSet::new();

    for commit in branch_commits {
        let entry_reader = CommitEntryReader::new(local_repo, &commit)?;
        let entry = entry_reader.get_entry(path)?;

        if let Some(entry) = entry {
            log::debug!("found entry {} for commit: {}", path.display(), commit.id);
            if !seen_hashes.contains(&entry.hash) {
                seen_hashes.insert(entry.hash.clone());
                result.push((commit, entry));
            }
        } else {
            log::debug!(
                "did not find entry {} for commit: {}",
                path.display(),
                commit.id
            );
        }
    }

    result.reverse();

    Ok(result)
}

fn branch_name_no_slashes(name: &str) -> String {
    // Replace all slashes with dashes

    name.replace('/', "-")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core;
    use crate::error::OxenError;
    use crate::test;
    use crate::util;

    #[test]
    fn test_list_branch_versions_main() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Make a dir
            let dir_path = Path::new("test_dir");
            let dir_repo_path = repo.path.join(dir_path);
            util::fs::create_dir_all(&dir_repo_path)?;

            // File in the dir
            let file_path = dir_path.join(Path::new("test_file.txt"));
            let file_repo_path = repo.path.join(&file_path);
            util::fs::write_to_path(&file_repo_path, "test")?;

            // Add the dir
            command::add(&repo, &repo.path)?;
            let commit_1 = command::commit(&repo, "adding test dir")?;

            // New file in root
            let file_path_2 = Path::new("test_file_2.txt");
            let file_repo_path_2 = repo.path.join(&file_path_2);
            util::fs::write_to_path(&file_repo_path_2, "test")?;

            // Add the file
            command::add(&repo, &file_repo_path_2)?;
            let commit_2 = command::commit(&repo, "adding test file")?;

            // Now modify both files, add a third
            let file_path_3 = Path::new("test_file_3.txt");
            let file_repo_path_3 = repo.path.join(&file_path_3);

            util::fs::write_to_path(&file_repo_path_3, "test 3")?;
            util::fs::write_to_path(&file_repo_path_2, "something different now")?;
            util::fs::write_to_path(&file_repo_path, "something different now")?;

            // Add-commit all
            command::add(&repo, &repo.path)?;

            let commit_3 = command::commit(&repo, "adding test file 2")?;

            let branch = api::local::branches::get_by_name(&repo, DEFAULT_BRANCH_NAME)?.unwrap();

            let file_versions =
                api::local::branches::list_entry_versions_on_branch(&repo, "main", &file_path)?;

            let file_2_versions = api::local::branches::list_entry_versions_on_branch(
                &repo,
                "main",
                &file_path_2.to_path_buf(),
            )?;

            let file_3_versions = api::local::branches::list_entry_versions_on_branch(
                &repo,
                "main",
                &file_path_3.to_path_buf(),
            )?;

            assert_eq!(file_versions.len(), 2);
            assert_eq!(file_versions[0].0.id, commit_3.id);
            assert_eq!(file_versions[1].0.id, commit_1.id);

            assert_eq!(file_2_versions.len(), 2);
            assert_eq!(file_2_versions[0].0.id, commit_3.id);
            assert_eq!(file_2_versions[1].0.id, commit_2.id);

            assert_eq!(file_3_versions.len(), 1);
            assert_eq!(file_3_versions[0].0.id, commit_3.id);

            Ok(())
        })
    }

    #[test]
    fn test_list_branch_versions_branch_off_main() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let dir_path = Path::new("test_dir");
            std::fs::create_dir_all(repo.path.join(dir_path))?;

            let file_path = dir_path.join(Path::new("test_file.txt"));
            let file_repo_path = repo.path.join(&file_path);

            // STARTING ON MAIN

            // Write initial file
            util::fs::write_to_path(&file_repo_path, "test")?;
            command::add(&repo, &repo.path)?;
            let commit_1 = command::commit(&repo, "adding test file")?;

            // Change it
            util::fs::write_to_path(&file_repo_path, "something different now")?;
            command::add(&repo, &repo.path)?;
            let commit_2 = command::commit(&repo, "adding test file 2")?;

            // Add an irrelevant file - aka this isn't changing for commit 3
            let file_path_2 = Path::new("test_file_2.txt");
            let file_repo_path_2 = repo.path.join(&file_path_2);
            util::fs::write_to_path(&file_repo_path_2, "test")?;
            command::add(&repo, &repo.path)?;
            let _commit_3 = command::commit(&repo, "adding test file 3")?;

            // Branch off of main
            api::local::branches::create_checkout(&repo, "test_branch")?;

            // Change the file again
            util::fs::write_to_path(&file_repo_path, "something different now again")?;
            command::add(&repo, &repo.path)?;
            let commit_4 = command::commit(&repo, "adding test file 4")?;

            // One more time on branch
            util::fs::write_to_path(&file_repo_path, "something different now again again")?;
            command::add(&repo, &repo.path)?;
            let commit_5 = command::commit(&repo, "adding test file 5")?;

            // Back to main - hacky to avoid async checkout
            {
                let ref_writer = core::index::RefWriter::new(&repo)?;
                ref_writer.set_head(DEFAULT_BRANCH_NAME);
            }

            // Another commit
            util::fs::write_to_path(&file_repo_path, "something different now again again again")?;
            command::add(&repo, &repo.path)?;
            let commit_6 = command::commit(&repo, "adding test file 6")?;

            let main = api::local::branches::get_by_name(&repo, DEFAULT_BRANCH_NAME)?.unwrap();
            let branch = api::local::branches::get_by_name(&repo, "test_branch")?.unwrap();
            let main_versions =
                api::local::branches::list_entry_versions_on_branch(&repo, "main", &file_path)?;

            let branch_versions = api::local::branches::list_entry_versions_on_branch(
                &repo,
                "test_branch",
                &file_path.to_path_buf(),
            )?;

            // Main should have commits 6, 2, and 1.
            assert_eq!(main_versions.len(), 3);
            assert_eq!(main_versions[0].0.id, commit_6.id);
            assert_eq!(main_versions[1].0.id, commit_2.id);
            assert_eq!(main_versions[2].0.id, commit_1.id);

            // Branch should have commits 5, 4, 2, and 1.
            assert_eq!(branch_versions.len(), 4);
            assert_eq!(branch_versions[0].0.id, commit_5.id);
            assert_eq!(branch_versions[1].0.id, commit_4.id);
            assert_eq!(branch_versions[2].0.id, commit_2.id);
            assert_eq!(branch_versions[3].0.id, commit_1.id);

            Ok(())
        })
    }
}
