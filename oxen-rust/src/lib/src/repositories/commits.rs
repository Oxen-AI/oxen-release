//! # Local Commits
//!
//! Interact with local commits.
//!

use crate::constants::{
    HISTORY_DIR, OBJECT_DIRS_DIR, OBJECT_FILES_DIR, OBJECT_SCHEMAS_DIR, OBJECT_VNODES_DIR, TREE_DIR,
};
use crate::core::db;
use crate::core::db::key_val::path_db;
use crate::core::db::key_val::tree_db::{self, TreeObject};
use crate::core::v0_10_0::cache::cachers::content_validator;
use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_10_0::index::tree_db_reader::TreeDBMerger;
use crate::core::v0_10_0::index::{
    self, CommitDirEntryReader, CommitEntryReader, CommitEntryWriter, CommitReader, CommitWriter,
    RefReader, RefWriter, Stager, TreeObjectReader,
};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, StagedData};
use crate::opts::LogOpts;
use crate::util::fs::commit_content_is_valid_path;
use crate::view::{PaginatedCommits, StatusMessage};
use crate::{api, repositories, util};
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};

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
    let commits = repositories::commits::list_from(repo, commit_id)?;
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
    let commits = repositories::commits::list_from(repo, commit_id)?;

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

pub fn list_all_paginated(
    repo: &LocalRepository,
    page_number: usize,
    page_size: usize,
) -> Result<PaginatedCommits, OxenError> {
    let commits = list_all(repo)?;
    let (commits, pagination) = util::paginate(commits, page_number, page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}

/// Get commit history given options
pub async fn list_with_opts(
    repo: &LocalRepository,
    opts: &LogOpts,
) -> Result<Vec<Commit>, OxenError> {
    if opts.remote {
        let remote_repo = api::client::repositories::get_default_remote(repo).await?;
        let revision = if let Some(revision) = &opts.revision {
            revision.to_owned()
        } else {
            repositories::branches::current_branch(repo)?.unwrap().name
        };
        let commits = api::client::commits::list_commit_history(&remote_repo, &revision).await?;
        Ok(commits)
    } else {
        let committer = CommitReader::new(repo)?;

        let commits = if let Some(revision) = &opts.revision {
            let commit = repositories::revisions::get(repo, revision)?
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
        let base_commit_id = match repositories::branches::get_commit_id(repo, base)? {
            Some(branch_commit_id) => branch_commit_id,
            None => String::from(base),
        };
        let head_commit_id = match repositories::branches::get_commit_id(repo, head)? {
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

    let commit_id = match repositories::branches::get_commit_id(repo, revision)? {
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

// load all commit entry readers once
fn get_commit_entry_readers(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<Vec<(Commit, CommitDirEntryReader)>, OxenError> {
    let commit_reader = CommitReader::new(repo)?;
    let commits = commit_reader.history_from_commit_id(&commit.id)?;
    let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
    for c in commits {
        let object_reader = get_object_reader(repo, &c.id)?;
        let reader = CommitDirEntryReader::new(repo, &c.id, path, object_reader.clone())?;
        commit_entry_readers.push((c.clone(), reader));
    }
    Ok(commit_entry_readers)
}

pub fn list_by_resource_from_paginated(
    repo: &LocalRepository,
    path: &Path,
    commit: &Commit,
    page_number: usize,
    page_size: usize,
) -> Result<PaginatedCommits, OxenError> {
    let object_reader = get_object_reader(repo, &commit.id)?;
    let entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &commit.id, object_reader.clone())?;

    let commits = if entry_reader.has_dir(path) {
        // log::debug!("list_by_resource_from_paginated: has dir {:?}", path);
        list_by_directory(repo, path, commit)?
    } else {
        // log::debug!("list_by_resource_from_paginated: checking file {:?}", path);
        // load all commit entry readers once
        let commit_entry_readers =
            get_commit_entry_readers(repo, commit, path.parent().unwrap_or(Path::new("")))?;
        // log::debug!(
        //     "list_by_resource_from_paginated got {} entry readers",
        //     commit_entry_readers.len()
        // );
        list_by_file(path, &commit_entry_readers)?
    };

    paginate_and_format_results(commits, page_number, page_size)
}

fn latest_commit_in_files(
    latest_commit: &mut Option<Commit>,
    entry_reader: &CommitDirEntryReader,
    readers: &[(Commit, CommitDirEntryReader)],
) -> Result<(), OxenError> {
    // We're trying to find the latest commit
    for file in entry_reader.list_files()? {
        let file_name = file.file_name().unwrap().to_str().unwrap();
        // log::debug!("repositories::commits::list_by_directory: file {:?} file_name {:?}", file, file_name);

        let mut latest_file_commit: Option<Commit> = None;
        let mut latest_file_hash: Option<String> = None;

        for (commit, cer) in readers {
            if let Some(lc) = latest_commit.as_ref() {
                // If the commit is older than the latest commit, we can skip it
                // log::debug!("repositories::commits::list_by_directory: comparing commit: {} with latest commit: {} and timestamp: {} with latest timestamp: {}", commit, lc, commit.timestamp, lc.timestamp);
                if commit.timestamp <= lc.timestamp {
                    continue;
                }
            } else {
                log::debug!(
                    "repositories::commits::list_by_directory: setting initial commit: {}",
                    commit
                );
                *latest_commit = Some(commit.clone());
            }
            log::debug!(
                "repositories::commits::list_by_directory: considering file: {:?} in commit: {}",
                file_name,
                commit
            );

            if let Some(entry) = cer.get_entry(file_name)? {
                log::debug!(
                    "repositories::commits::list_by_directory: found file in commit: {}",
                    commit
                );
                if latest_file_hash.is_none() {
                    log::debug!("repositories::commits::list_by_directory: setting initial file {} latest commit: {}", file_name, commit);

                    latest_file_hash = Some(entry.hash.clone());
                    latest_file_commit = Some(commit.clone());
                    *latest_commit = latest_file_commit.clone();
                }

                let lc = latest_file_commit.as_mut().unwrap();
                // If the commit is newer than the latest commit, we update the latest commit
                log::debug!("repositories::commits::list_by_directory: comparing commit: {} with latest commit: {} and hash: {} with latest hash: {}", commit, lc, entry.hash, latest_file_hash.as_ref().unwrap());
                if commit.timestamp >= lc.timestamp
                    && &entry.hash != latest_file_hash.as_ref().unwrap()
                {
                    log::debug!(
                        "repositories::commits::list_by_directory: updating latest commit: {}",
                        commit
                    );
                    latest_file_commit = Some(commit.clone());
                    latest_file_hash = Some(entry.hash);
                    *latest_commit = latest_file_commit.clone();
                }
            }
        }
    }
    log::debug!(
        "repositories::commits::list_by_directory: returning latest commit: {:?}",
        latest_commit
    );
    Ok(())
}

pub fn latest_commit_by_directory(
    repo: &LocalRepository,
    path: &Path,
    commit: &Commit,
) -> Result<(Option<Commit>, HashSet<String>), OxenError> {
    log::debug!(
        "repositories::commits::list_by_directory: path {:?} for commit {}",
        path,
        commit
    );
    // List all the commits
    let object_reader = get_object_reader(repo, &commit.id)?;

    let dir_entry_reader =
        CommitDirEntryReader::new(repo, &commit.id, path, object_reader.clone())?;
    let mut readers = get_commit_entry_readers(repo, commit, path)?;
    readers.reverse();

    // Loop over all commits and check if this dir is present
    let mut valid_commit_ids: HashSet<String> = HashSet::new();
    for (c, _) in &readers {
        let entry_reader =
            CommitEntryReader::new_from_commit_id(repo, &c.id, object_reader.clone())?;
        if entry_reader.has_dir(path) {
            valid_commit_ids.insert(c.id.clone());
        }
    }

    // Filter out commits that don't contain this dir
    readers.retain(|(commit, _)| valid_commit_ids.contains(&commit.id));

    // We're trying to find the latest commit
    let mut latest_commit: Option<Commit> = None;
    // Set latest commit to be the earliest valid commit to start with
    let mut earliest_commit: Option<Commit> = None;
    let commit_reader = CommitReader::new(repo)?;
    for valid_commit_id in &valid_commit_ids {
        if let Some(commit) = commit_reader.get_commit_by_id(valid_commit_id)? {
            if earliest_commit.is_none()
                || commit.timestamp < earliest_commit.as_ref().unwrap().timestamp
            {
                earliest_commit = Some(commit);
            }
        }
    }

    if earliest_commit.is_some() {
        latest_commit = earliest_commit;
    }

    latest_commit_in_files(&mut latest_commit, &dir_entry_reader, &readers)?;

    let dirs = dir_entry_reader.list_dirs()?;
    for dir in dirs {
        if path.starts_with(&dir) && dir.components().count() > path.components().count() {
            log::debug!("repositories::commits::list_by_directory: dir {:?}", dir);
            let dir_entry_reader =
                CommitDirEntryReader::new(repo, &commit.id, &dir, object_reader.clone())?;
            latest_commit_in_files(&mut latest_commit, &dir_entry_reader, &readers)?;
        }
    }

    Ok((latest_commit, valid_commit_ids))
}

fn list_by_directory(
    repo: &LocalRepository,
    path: &Path,
    commit: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    let (latest_commit, valid_commit_ids) = latest_commit_by_directory(repo, path, commit)?;

    // Just return the history from the latest commit
    if let Some(latest_commit) = latest_commit {
        let commit_reader = CommitReader::new(repo)?;
        let commits = commit_reader.history_from_commit_id(&latest_commit.id)?;
        let commits = commits
            .into_iter()
            .filter(|c| valid_commit_ids.contains(&c.id))
            .collect();
        return Ok(commits);
    }

    Ok(vec![])
}

fn list_by_file(
    path: &Path,
    commit_entry_readers: &[(Commit, CommitDirEntryReader)],
) -> Result<Vec<Commit>, OxenError> {
    repositories::entries::get_commit_history_path(commit_entry_readers, path)
}

fn paginate_and_format_results(
    commits: Vec<Commit>,
    page_number: usize,
    page_size: usize,
) -> Result<PaginatedCommits, OxenError> {
    let (commits, pagination) = util::paginate(commits, page_number, page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}

pub fn commit_history_is_complete(repo: &LocalRepository, commit: &Commit) -> bool {
    // Get full commit history from this head backwards
    let history = repositories::commits::list_from(repo, &commit.id).unwrap();

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

pub fn head_commits_have_conflicts(
    repo: &LocalRepository,
    client_head_id: &str,
    server_head_id: &str,
    lca_id: &str,
) -> Result<bool, OxenError> {
    // Connect to the 3 commit merkle trees
    log::debug!("checking new head commits have conflicts");
    // Get server head and lca commits
    let server_head = repositories::commits::get_by_id(repo, server_head_id)?.unwrap();
    let lca = repositories::commits::get_by_id(repo, lca_id)?.unwrap();

    // Initialize commit entry readers for the server head and LCA - we have full db structures for them, where the client db is going to be kinda weird...
    let server_reader = TreeObjectReader::new(repo, &server_head)?;
    let lca_reader = TreeObjectReader::new(repo, &lca)?;
    let client_db_path = util::fs::oxen_hidden_dir(&repo.path)
        .join("tmp")
        .join(client_head_id)
        .join(TREE_DIR);

    let tree_merger = TreeDBMerger::new(client_db_path.clone(), server_reader, lca_reader);
    // Start at the top level of the client db
    let maybe_client_root = &tree_merger.client_reader.get_root_entry()?;
    let maybe_server_root = &tree_merger.server_reader.get_root_entry()?;
    let maybe_lca_root = &tree_merger.lca_reader.get_root_entry()?;

    tree_merger.r_tree_has_conflict(maybe_client_root, maybe_server_root, maybe_lca_root)
}

pub fn has_merkle_tree(repo: &LocalRepository, commit: &Commit) -> Result<bool, OxenError> {
    let path = CommitEntryWriter::commit_tree_db(&repo.path, &commit.id);
    Ok(path.exists())
}

pub fn merge_objects_dbs(repo_objects_dir: &Path, tmp_objects_dir: &Path) -> Result<(), OxenError> {
    let repo_dirs_dir = repo_objects_dir.join(OBJECT_DIRS_DIR);
    let repo_files_dir = repo_objects_dir.join(OBJECT_FILES_DIR);
    let repo_schemas_dir = repo_objects_dir.join(OBJECT_SCHEMAS_DIR);
    let repo_vnodes_dir = repo_objects_dir.join(OBJECT_VNODES_DIR);

    let new_dirs_dir = tmp_objects_dir.join(OBJECT_DIRS_DIR);
    let new_files_dir = tmp_objects_dir.join(OBJECT_FILES_DIR);
    let new_schemas_dir = tmp_objects_dir.join(OBJECT_SCHEMAS_DIR);
    let new_vnodes_dir = tmp_objects_dir.join(OBJECT_VNODES_DIR);

    log::debug!("opening tmp dirs");
    let opts = db::key_val::opts::default();
    let new_dirs_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, new_dirs_dir, false)?;
    let new_files_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, new_files_dir, false)?;
    let new_schemas_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, new_schemas_dir, false)?;
    let new_vnodes_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, new_vnodes_dir, false)?;

    // Create if missing for the local repo dirs - useful in case of remote download to cache dir without full repo

    log::debug!("opening repo dirs");
    let repo_dirs_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, repo_dirs_dir)?;
    let repo_files_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, repo_files_dir)?;
    let repo_schemas_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, repo_schemas_dir)?;
    let repo_vnodes_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, repo_vnodes_dir)?;

    //

    let new_dirs: Vec<TreeObject> = path_db::list_entries(&new_dirs_db)?;
    for dir in new_dirs {
        tree_db::put_tree_object(&repo_dirs_db, dir.hash(), &dir)?;
    }

    let new_files: Vec<TreeObject> = path_db::list_entries(&new_files_db)?;
    for file in new_files {
        tree_db::put_tree_object(&repo_files_db, file.hash(), &file)?;
    }

    let new_schemas: Vec<TreeObject> = path_db::list_entries(&new_schemas_db)?;
    for schema in new_schemas {
        tree_db::put_tree_object(&repo_schemas_db, schema.hash(), &schema)?;
    }

    let new_vnodes: Vec<TreeObject> = path_db::list_entries(&new_vnodes_db)?;
    for vnode in new_vnodes {
        tree_db::put_tree_object(&repo_vnodes_db, vnode.hash(), &vnode)?;
    }

    Ok(())
}
#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::repositories;
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
                let head_commit = repositories::commits::head_commit(&deep_clone)?;
                assert!(repositories::commits::commit_history_is_complete(
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
                let head_commit = repositories::commits::head_commit(&clone)?;
                assert!(!repositories::commits::commit_history_is_complete(
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
                let head_commit = repositories::commits::head_commit(&shallow_clone)?;
                assert!(!repositories::commits::commit_history_is_complete(
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
