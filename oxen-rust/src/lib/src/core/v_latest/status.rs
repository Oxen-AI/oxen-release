use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::core::oxenignore;
use crate::core::staged::staged_db_manager::with_staged_db_manager;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::merkle_tree::node::StagedMerkleTreeNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::staged_data::StagedDataOpts;
use crate::model::{
    Commit, LocalRepository, MerkleHash, StagedData, StagedDirStats, StagedEntry,
    StagedEntryStatus, StagedSchema, SummarizedStagedDirStats,
};
use crate::{repositories, util};

use ignore::gitignore::Gitignore;
use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, IteratorMode, SingleThreaded};
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::time::Duration;

use crate::core::v_latest::index::CommitMerkleTree;
use crate::core::v_latest::watcher_client::{WatcherClient, WatcherStatus};
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;

pub fn status(repo: &LocalRepository) -> Result<StagedData, OxenError> {
    status_from_dir(repo, &repo.path)
}

pub fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    let opts = StagedDataOpts {
        paths: vec![dir.as_ref().to_path_buf()],
        ..StagedDataOpts::default()
    };
    status_from_opts(repo, &opts)
}

/// Status with optional watcher cache support
pub async fn status_with_cache(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    use_cache: bool,
) -> Result<StagedData, OxenError> {
    // If cache is enabled, try to use the watcher
    if use_cache {
        log::debug!("Attempting to use watcher cache for status");

        // Try to connect to watcher
        if let Some(client) = WatcherClient::connect(repo).await {
            log::info!("Connected to watcher, getting status");

            // Try to get status from watcher
            match client.get_status().await {
                Ok(watcher_status) => {
                    log::debug!("Got status from watcher, merging with staged data");
                    log::info!("Got status from watcher, merging with staged data");
                    return merge_watcher_with_staged(repo, opts, watcher_status);
                }
                Err(e) => {
                    log::warn!("Failed to get status from watcher: {}", e);
                    // Fall through to regular status
                }
            }
        } else {
            log::warn!("Could not connect to watcher");
        }
    } else {
        log::debug!("Cache disabled, using direct scan");
    }

    // Fallback to regular status
    status_from_opts(repo, opts)
}

/// Merge watcher data with staged database and other sources
fn merge_watcher_with_staged(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    watcher: WatcherStatus,
) -> Result<StagedData, OxenError> {
    log::debug!("Merging watcher data with staged database");

    let mut staged_data = StagedData::empty();

    // Apply oxenignore filtering
    let oxenignore = oxenignore::create(repo);

    // Use watcher data for filesystem state
    // Apply path filtering if paths were specified
    if !opts.paths.is_empty() && opts.paths[0] != repo.path {
        // Filter watcher results to only include specified paths
        let requested_paths: HashSet<PathBuf> = opts
            .paths
            .iter()
            .map(|p| util::fs::path_relative_to_dir(p, &repo.path))
            .filter_map(Result::ok)
            .collect();

        staged_data.untracked_files = watcher
            .untracked
            .into_iter()
            .filter(|p| requested_paths.iter().any(|req| p.starts_with(req)))
            .filter(|p| !oxenignore::is_ignored(p, &oxenignore, false))
            .collect();

        staged_data.modified_files = watcher
            .modified
            .into_iter()
            .filter(|p| requested_paths.iter().any(|req| p.starts_with(req)))
            .filter(|p| !oxenignore::is_ignored(p, &oxenignore, false))
            .collect();

        staged_data.removed_files = watcher
            .removed
            .into_iter()
            .filter(|p| requested_paths.iter().any(|req| p.starts_with(req)))
            .filter(|p| !oxenignore::is_ignored(p, &oxenignore, false))
            .collect();
    } else {
        // Use all watcher data with oxenignore filtering
        staged_data.untracked_files = watcher
            .untracked
            .into_iter()
            .filter(|p| !oxenignore::is_ignored(p, &oxenignore, false))
            .collect();
        staged_data.modified_files = watcher
            .modified
            .into_iter()
            .filter(|p| !oxenignore::is_ignored(p, &oxenignore, false))
            .collect();
        staged_data.removed_files = watcher
            .removed
            .into_iter()
            .filter(|p| !oxenignore::is_ignored(p, &oxenignore, false))
            .collect();
    }

    // Extract untracked directories from untracked files
    let mut untracked_dirs: HashMap<PathBuf, usize> = HashMap::new();
    for file in &staged_data.untracked_files {
        if let Some(parent) = file.parent() {
            if !parent.as_os_str().is_empty() {
                *untracked_dirs.entry(parent.to_path_buf()).or_insert(0) += 1;
            }
        }
    }
    staged_data.untracked_dirs = untracked_dirs.into_iter().collect();

    // Now read staged data from the database
    let staged_db_maybe = open_staged_db(repo)?;

    if let Some(staged_db) = staged_db_maybe {
        log::debug!("Reading staged entries from database");

        let read_progress = ProgressBar::new_spinner();
        read_progress.set_style(ProgressStyle::default_spinner());
        read_progress.enable_steady_tick(Duration::from_millis(100));

        // Read staged entries based on paths
        let mut dir_entries = HashMap::new();
        if !opts.paths.is_empty() {
            for path in &opts.paths {
                let (sub_dir_entries, _) =
                    read_staged_entries_below_path(repo, &staged_db, path, &read_progress)?;
                dir_entries.extend(sub_dir_entries);
            }
        } else {
            let (entries, _) = read_staged_entries(repo, &staged_db, &read_progress)?;
            dir_entries = entries;
        }

        read_progress.finish_and_clear();

        // Process staged entries and build staged data
        status_from_dir_entries(&mut staged_data, dir_entries)?;
    }

    // Find merge conflicts
    let conflicts = repositories::merge::list_conflicts(repo)?;
    for conflict in conflicts {
        staged_data
            .merge_conflicts
            .push(conflict.to_entry_merge_conflict());
    }

    Ok(staged_data)
}

pub fn status_from_opts(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    //log::debug!("status_from_opts {:?}", opts.paths);
    let staged_db_maybe = open_staged_db(repo)?;
    let head_commit = repositories::commits::head_commit_maybe(repo)?;
    let dir_hashes = get_dir_hashes(repo, &head_commit)?;

    let read_progress = ProgressBar::new_spinner();
    read_progress.set_style(ProgressStyle::default_spinner());
    read_progress.enable_steady_tick(Duration::from_millis(100));

    let mut total_entries = 0;

    let mut untracked = UntrackedData::new();
    let mut modified = HashSet::new();
    let mut removed = HashSet::new();

    for dir in opts.paths.iter() {
        let relative_dir = util::fs::path_relative_to_dir(dir, &repo.path)?;
        let (sub_untracked, sub_modified, sub_removed) = find_changes(
            repo,
            opts,
            &relative_dir,
            &staged_db_maybe,
            &dir_hashes,
            &read_progress,
            &mut total_entries,
        )?;
        untracked.merge(sub_untracked);
        modified.extend(sub_modified);
        removed.extend(sub_removed);
    }

    log::debug!("find_changes untracked: {:?}", untracked);
    log::debug!("find_changes modified: {:?}", modified);
    log::debug!("find_changes removed: {:?}", removed);

    let mut staged_data = StagedData::empty();
    staged_data.untracked_dirs = untracked.dirs.into_iter().collect();
    staged_data.untracked_files = untracked.files;
    staged_data.modified_files = modified;
    staged_data.removed_files = removed;

    // Find merge conflicts
    let conflicts = repositories::merge::list_conflicts(repo)?;
    //log::debug!("list_conflicts found {} conflicts", conflicts.len());
    for conflict in conflicts {
        staged_data
            .merge_conflicts
            .push(conflict.to_entry_merge_conflict());
    }

    let Some(staged_db) = staged_db_maybe else {
        log::debug!("status_from_dir no staged db, returning early");
        return Ok(staged_data);
    };

    // TODO: Consider moving this to the top to keep track of removed dirs and avoid unnecessary recursion with count_removed_entries
    let mut dir_entries = HashMap::new();
    for dir in opts.paths.iter() {
        let (sub_dir_entries, _) =
            read_staged_entries_below_path(repo, &staged_db, dir, &read_progress)?;
        dir_entries.extend(sub_dir_entries);
        // log::debug!("status_from_dir dir_entries: {:?}", dir_entries);
    }
    read_progress.finish_and_clear();

    status_from_dir_entries(&mut staged_data, dir_entries)
}

// Get status with pre-existing staged data
pub fn status_from_opts_and_staged_data(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    staged_data: &mut StagedData,
) -> Result<(), OxenError> {
    //log::debug!("status_from_opts {:?}", opts.paths);
    let head_commit = repositories::commits::head_commit_maybe(repo)?;
    let dir_hashes = get_dir_hashes(repo, &head_commit)?;

    let read_progress = ProgressBar::new_spinner();
    read_progress.set_style(ProgressStyle::default_spinner());
    read_progress.enable_steady_tick(Duration::from_millis(100));

    let mut total_entries = 0;

    let mut untracked = UntrackedData::new();
    let mut modified = HashSet::new();
    let mut removed = HashSet::new();

    for dir in opts.paths.iter() {
        let relative_dir = util::fs::path_relative_to_dir(dir, &repo.path)?;
        let (sub_untracked, sub_modified, sub_removed) = find_local_changes(
            repo,
            opts,
            &relative_dir,
            staged_data,
            &dir_hashes,
            &read_progress,
            &mut total_entries,
        )?;
        untracked.merge(sub_untracked);
        modified.extend(sub_modified);
        removed.extend(sub_removed);
    }

    log::debug!("find_changes untracked: {:?}", untracked);
    log::debug!("find_changes modified: {:?}", modified);
    log::debug!("find_changes removed: {:?}", removed);

    staged_data.untracked_dirs = untracked.dirs.into_iter().collect();
    staged_data.untracked_files = untracked.files;
    staged_data.modified_files = modified;
    staged_data.removed_files = removed;

    // Find merge conflicts
    let conflicts = repositories::merge::list_conflicts(repo)?;
    //log::debug!("list_conflicts found {} conflicts", conflicts.len());
    for conflict in conflicts {
        staged_data
            .merge_conflicts
            .push(conflict.to_entry_merge_conflict());
    }

    Ok(())
}

pub fn status_from_dir_entries(
    staged_data: &mut StagedData,
    dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
) -> Result<StagedData, OxenError> {
    let mut summarized_dir_stats = SummarizedStagedDirStats {
        num_files_staged: 0,
        total_files: 0,
        paths: HashMap::new(),
    };

    //log::debug!("dir_entries.len(): {:?}", dir_entries.len());

    for (dir, entries) in dir_entries {
        log::debug!(
            "dir_entries dir: {:?} entries.len(): {:?}",
            dir,
            entries.len()
        );
        let mut stats = StagedDirStats {
            path: dir.clone(),
            num_files_staged: 0,
            total_files: 0,
            status: StagedEntryStatus::Added,
        };

        let mut removed_stats = StagedDirStats {
            path: dir.clone(),
            num_files_staged: 0,
            total_files: 0,
            status: StagedEntryStatus::Removed,
        };

        let mut is_removed = false;

        for entry in &entries {
            match &entry.node.node {
                EMerkleTreeNode::Directory(node) => {
                    log::debug!("dir_entries dir_node: {}", node);
                    // Correction for empty dir status
                    is_removed = true;

                    // Cannot be removed if it's staged
                    if !staged_data.staged_dirs.contains_key(&dir) {
                        staged_data
                            .removed_files
                            .remove(&PathBuf::from(&node.name()));
                    }
                }
                EMerkleTreeNode::File(node) => {
                    // TODO: It's not always added. It could be modified.
                    log::debug!("dir_entries file_node: {}", entry);
                    let file_path = PathBuf::from(node.name());
                    if entry.status == StagedEntryStatus::Modified {
                        staged_data.modified_files.insert(file_path.clone());
                    }
                    let staged_entry = StagedEntry {
                        hash: node.hash().to_string(),
                        status: entry.status.clone(),
                    };

                    staged_data
                        .staged_files
                        .insert(file_path.clone(), staged_entry);
                    maybe_add_schemas(node, staged_data)?;

                    // Cannot be removed if it's staged
                    if staged_data.staged_files.contains_key(&file_path) {
                        staged_data.removed_files.remove(&file_path);
                        staged_data.modified_files.remove(&file_path);
                    }

                    if entry.status == StagedEntryStatus::Removed {
                        removed_stats.num_files_staged += 1;
                    } else {
                        stats.num_files_staged += 1;
                    }
                }
                _ => {
                    return Err(OxenError::basic_str(format!(
                        "status_from_dir found unexpected node type: {:?}",
                        entry.node
                    )));
                }
            }
        }

        // Empty dirs should be added to summarized_dir_stats (entries.len() == 0)
        if entries.is_empty() {
            if is_removed || staged_data.removed_files.contains(&dir) {
                summarized_dir_stats.add_stats(&removed_stats);
            } else {
                summarized_dir_stats.add_stats(&stats);
            }
        }

        if stats.num_files_staged > 0 {
            summarized_dir_stats.add_stats(&stats);
        }

        if removed_stats.num_files_staged > 0 {
            summarized_dir_stats.add_stats(&removed_stats);
        }
    }

    staged_data.staged_dirs = summarized_dir_stats;
    find_moved_files(staged_data)?;

    Ok(staged_data.clone())
}

fn find_moved_files(staged_data: &mut StagedData) -> Result<(), OxenError> {
    let files = staged_data.staged_files.clone();
    let files_vec: Vec<(&PathBuf, &StagedEntry)> = files.iter().collect();

    // Find pairs of added-removed with same hash and add them to moved.
    // We won't mutate StagedEntries here, the "moved" property is read-only
    let mut added_map: HashMap<String, Vec<&PathBuf>> = HashMap::new();
    let mut removed_map: HashMap<String, Vec<&PathBuf>> = HashMap::new();

    for (path, entry) in files_vec.iter() {
        match entry.status {
            StagedEntryStatus::Added => {
                added_map.entry(entry.hash.clone()).or_default().push(path);
            }
            StagedEntryStatus::Removed => {
                removed_map
                    .entry(entry.hash.clone())
                    .or_default()
                    .push(path);
            }
            _ => continue,
        }
    }

    for (hash, added_paths) in added_map.iter_mut() {
        if let Some(removed_paths) = removed_map.get_mut(hash) {
            while !added_paths.is_empty() && !removed_paths.is_empty() {
                if let (Some(added_path), Some(removed_path)) =
                    (added_paths.pop(), removed_paths.pop())
                {
                    // moved_entries.push((added_path, removed_path, hash.to_string()));
                    staged_data.moved_files.push((
                        added_path.clone(),
                        removed_path.clone(),
                        hash.to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn maybe_add_schemas(node: &FileNode, staged_data: &mut StagedData) -> Result<(), OxenError> {
    if let Some(GenericMetadata::MetadataTabular(m)) = &node.metadata() {
        let schema = m.tabular.schema.clone();
        let path = PathBuf::from(node.name());
        let staged_schema = StagedSchema {
            schema,
            status: StagedEntryStatus::Added,
        };
        staged_data.staged_schemas.insert(path, staged_schema);
    }

    Ok(())
}

pub fn read_staged_entries(
    repo: &LocalRepository,
    db: &DBWithThreadMode<SingleThreaded>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, usize), OxenError> {
    read_staged_entries_below_path(repo, db, Path::new(""), read_progress)
}

/// Duplicate function using staged db manager in workspaces
pub fn read_staged_entries_with_staged_db_manager(
    repo: &LocalRepository,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, usize), OxenError> {
    read_staged_entries_below_path_with_staged_db_manager(repo, Path::new(""), read_progress)
}

/// Duplicate function using staged db manager in workspaces
pub fn read_staged_entries_below_path_with_staged_db_manager(
    repo: &LocalRepository,
    start_path: impl AsRef<Path>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, usize), OxenError> {
    with_staged_db_manager(repo, |staged_db_manager| {
        staged_db_manager.read_staged_entries_below_path(start_path, read_progress)
    })
}

pub fn read_staged_entries_below_path(
    repo: &LocalRepository,
    db: &DBWithThreadMode<SingleThreaded>,
    start_path: impl AsRef<Path>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, usize), OxenError> {
    let start_path = util::fs::path_relative_to_dir(start_path.as_ref(), &repo.path)?;
    let mut total_entries = 0;
    let iter = db.iterator(IteratorMode::Start);
    let mut dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>> = HashMap::new();
    for item in iter {
        match item {
            // key = file path, value = EntryMetaData
            Ok((key, value)) => {
                // log::debug!("Key is {key:?}, value is {value:?}");
                let key = str::from_utf8(&key)?;
                let path = Path::new(key);
                if !path.starts_with(&start_path) {
                    continue;
                }

                // Older versions may have a corrupted StagedMerkleTreeNode that was staged
                // Ignore these when reading the staged db
                let entry: Result<StagedMerkleTreeNode, rmp_serde::decode::Error> =
                    rmp_serde::from_slice(&value);
                let Ok(entry) = entry else {
                    log::error!("read_staged_entries error decoding {key} path: {path:?}");
                    continue;
                };
                log::debug!("read_staged_entries key {key} entry: {entry} path: {path:?}");

                if let EMerkleTreeNode::Directory(_) = &entry.node.node {
                    // add the dir as a key in dir_entries
                    log::debug!("read_staged_entries adding dir {:?}", path);
                    dir_entries.entry(path.to_path_buf()).or_default();
                }

                // add the file or dir as an entry under its parent dir
                if let Some(parent) = path.parent() {
                    log::debug!(
                        "read_staged_entries adding file {:?} to parent {:?}",
                        path,
                        parent
                    );
                    dir_entries
                        .entry(parent.to_path_buf())
                        .or_default()
                        .push(entry);
                }

                total_entries += 1;
                read_progress.set_message(format!("Found {} entries", total_entries));
            }
            Err(err) => {
                log::error!("Could not get staged entry: {}", err);
            }
        }
    }

    log::debug!(
        "read_staged_entries dir_entries.len(): {:?}",
        dir_entries.len()
    );
    if log::max_level() == log::Level::Debug {
        for (dir, entries) in dir_entries.iter() {
            log::debug!("commit dir_entries dir {:?}", dir);
            for entry in entries.iter() {
                log::debug!("\tcommit dir_entries entry {}", entry);
            }
        }
    }

    Ok((dir_entries, total_entries))
}

fn find_changes(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    search_node_path: impl AsRef<Path>,
    staged_db: &Option<DBWithThreadMode<SingleThreaded>>,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    progress: &ProgressBar,
    total_entries: &mut usize,
) -> Result<(UntrackedData, HashSet<PathBuf>, HashSet<PathBuf>), OxenError> {
    let search_node_path = search_node_path.as_ref();
    let full_path = repo.path.join(search_node_path);
    log::debug!(
        "find_changes search_node_path: {:?} full_path: {:?}",
        search_node_path,
        full_path
    );

    if let Some(ignore) = &opts.ignore {
        if ignore.contains(search_node_path) || ignore.contains(&full_path) {
            return Ok((UntrackedData::new(), HashSet::new(), HashSet::new()));
        }
    }

    let mut untracked = UntrackedData::new();
    let mut modified = HashSet::new();
    let mut removed = HashSet::new();
    let gitignore: Option<Gitignore> = oxenignore::create(repo);

    let mut entries: Vec<PathBuf> = Vec::new();
    if full_path.is_dir() {
        let Ok(dir_entries) = std::fs::read_dir(&full_path) else {
            return Err(OxenError::basic_str(format!(
                "Could not read dir {:?}",
                full_path
            )));
        };
        for entry in dir_entries {
            entries.push(entry?.path());
        }
    } else {
        entries.push(full_path.to_owned());
    }
    let mut untracked_count = 0;
    let search_node = maybe_get_node(repo, dir_hashes, search_node_path)?;
    let dir_children = maybe_get_dir_children(&search_node)?;

    for path in entries {
        progress.set_message(format!(
            "🐂 checking ({total_entries} files) scanning {:?}",
            search_node_path
        ));
        *total_entries += 1;
        let relative_path = util::fs::path_relative_to_dir(&path, &repo.path)?;
        let node_path = util::fs::path_relative_to_dir(&relative_path, search_node_path)?;
        log::debug!(
            "find_changes entry relative_path: {:?} in node_path {:?} search_node_path: {:?}",
            relative_path,
            node_path,
            search_node_path
        );

        if oxenignore::is_ignored(&relative_path, &gitignore, path.is_dir()) {
            continue;
        }

        if path.is_dir() {
            log::debug!("find_changes entry is a directory {:?}", path);
            // If it's a directory, recursively find changes below it
            let (sub_untracked, sub_modified, sub_removed) = find_changes(
                repo,
                opts,
                &relative_path,
                staged_db,
                dir_hashes,
                progress,
                total_entries,
            )?;
            untracked.merge(sub_untracked);
            modified.extend(sub_modified);
            removed.extend(sub_removed)
        } else if is_staged(&relative_path, staged_db)? {
            log::debug!("find_changes entry is staged {:?}", path);
            // check this after handling directories, because we still need to recurse into staged directories
            untracked.all_untracked = false;
            continue;
        } else if let Some(node) = maybe_get_child_node(&node_path, &dir_children)? {
            log::debug!("find_changes entry is a child node {:?}", path);
            // If we have a dir node, it's either tracked (clean) or modified
            // Either way, we know the directory is not all_untracked
            untracked.all_untracked = false;
            if let EMerkleTreeNode::File(file_node) = &node.node {
                let is_modified = util::fs::is_modified_from_node(&path, file_node)?;
                log::debug!("is_modified {} {:?}", is_modified, relative_path);
                if is_modified {
                    modified.insert(relative_path.clone());
                }
            }
        } else {
            log::debug!("find_changes entry is not a child node {:?}", path);
            // If it's none of the above conditions
            // then check if it's untracked or modified
            let mut found_file = false;
            if let Some(search_node) = &search_node {
                if let EMerkleTreeNode::File(file_node) = &search_node.node {
                    found_file = true;
                    if util::fs::is_modified_from_node(&path, file_node)? {
                        modified.insert(relative_path.clone());
                    }
                }
            }
            log::debug!("find_changes found_file {:?} {:?}", found_file, path);

            if !found_file {
                untracked.add_file(relative_path.clone());
                untracked_count += 1;
            }
        }
    }

    // Only add the untracked directory if it's not the root directory
    // and it's not staged or committed
    if untracked.all_untracked
        && search_node_path != Path::new("")
        && !is_staged(search_node_path, staged_db)?
        && full_path.is_dir()
        && search_node.is_none()
    {
        untracked.add_dir(search_node_path.to_path_buf(), untracked_count);
        // Clear individual files as they're now represented by the directory
        untracked.files.clear();
    }

    // Check for removed files
    if let Some(dir_hash) = dir_hashes.get(search_node_path) {
        // if we have subtree paths, don't check for removed files that are outside of the subtree
        if let Some(subtree_paths) = repo.subtree_paths() {
            if !subtree_paths.contains(&search_node_path.to_path_buf()) {
                return Ok((untracked, modified, removed));
            }

            if subtree_paths.len() == 1 && subtree_paths[0] == PathBuf::from("") {
                // If the subtree is the root, we need to check for removed files in the root
                let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
                if let Some(node) = dir_node {
                    for child in CommitMerkleTree::node_files_and_folders(&node)? {
                        if let EMerkleTreeNode::File(file_node) = &child.node {
                            let file_path = full_path.join(file_node.name());
                            if !file_path.exists() {
                                removed.insert(search_node_path.join(file_node.name()));
                            }
                        }
                    }
                }
                return Ok((untracked, modified, removed));
            }
        }

        let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
        if let Some(node) = dir_node {
            for child in CommitMerkleTree::node_files_and_folders(&node)? {
                if let EMerkleTreeNode::File(file_node) = &child.node {
                    let file_path = full_path.join(file_node.name());
                    if !file_path.exists() {
                        removed.insert(search_node_path.join(file_node.name()));
                    }
                } else if let EMerkleTreeNode::Directory(dir) = &child.node {
                    let dir_path = full_path.join(dir.name());
                    let relative_dir_path = search_node_path.join(dir.name());
                    if !dir_path.exists() {
                        // Only call this for non-existant dirs, because existant dirs already trigger a find_changes call

                        let mut count: usize = 0;
                        count_removed_entries(
                            repo,
                            &relative_dir_path,
                            dir.hash(),
                            &gitignore,
                            &mut count,
                        )?;

                        *total_entries += count;
                        removed.insert(relative_dir_path);
                    }
                }
            }
        }
    }

    Ok((untracked, modified, removed))
}

fn find_local_changes(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    search_node_path: impl AsRef<Path>,
    staged_data: &StagedData,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    progress: &ProgressBar,
    total_entries: &mut usize,
) -> Result<(UntrackedData, HashSet<PathBuf>, HashSet<PathBuf>), OxenError> {
    let search_node_path = search_node_path.as_ref();
    let full_path = repo.path.join(search_node_path);

    log::debug!(
        "find_changes search_node_path: {:?} full_path: {:?}",
        search_node_path,
        full_path
    );

    if let Some(ignore) = &opts.ignore {
        if ignore.contains(search_node_path) || ignore.contains(&full_path) {
            return Ok((UntrackedData::new(), HashSet::new(), HashSet::new()));
        }
    }

    let mut untracked = UntrackedData::new();
    let mut modified = HashSet::new();
    let mut removed = HashSet::new();
    let gitignore: Option<Gitignore> = oxenignore::create(repo);

    let mut entries: Vec<PathBuf> = Vec::new();
    if full_path.is_dir() {
        let Ok(dir_entries) = std::fs::read_dir(&full_path) else {
            return Err(OxenError::basic_str(format!(
                "Could not read dir {:?}",
                full_path
            )));
        };
        for entry in dir_entries {
            entries.push(entry?.path());
        }
    } else {
        entries.push(full_path.to_owned());
    }
    let mut untracked_count = 0;
    let search_node = maybe_get_node(repo, dir_hashes, search_node_path)?;
    let dir_children = maybe_get_dir_children(&search_node)?;

    for path in entries {
        progress.set_message(format!(
            "🐂 checking ({total_entries} files) scanning {:?}",
            search_node_path
        ));
        *total_entries += 1;
        let relative_path = util::fs::path_relative_to_dir(&path, &repo.path)?;
        let node_path = util::fs::path_relative_to_dir(&relative_path, search_node_path)?;
        log::debug!(
            "find_changes entry relative_path: {:?} in node_path {:?} search_node_path: {:?}",
            relative_path,
            node_path,
            search_node_path
        );

        if oxenignore::is_ignored(&relative_path, &gitignore, path.is_dir()) {
            continue;
        }

        if path.is_dir() {
            log::debug!("find_changes entry is a directory {:?}", path);
            // If it's a directory, recursively find changes below it
            let (sub_untracked, sub_modified, sub_removed) = find_local_changes(
                repo,
                opts,
                &relative_path,
                staged_data,
                dir_hashes,
                progress,
                total_entries,
            )?;
            untracked.merge(sub_untracked);
            modified.extend(sub_modified);
            removed.extend(sub_removed)
        } else if in_staged_data(&relative_path, staged_data)? {
            log::debug!("find_changes entry is staged {:?}", path);
            // check this after handling directories, because we still need to recurse into staged directories
            untracked.all_untracked = false;
            continue;
        } else if let Some(node) = maybe_get_child_node(&node_path, &dir_children)? {
            log::debug!("find_changes entry is a child node {:?}", path);
            // If we have a dir node, it's either tracked (clean) or modified
            // Either way, we know the directory is not all_untracked
            untracked.all_untracked = false;
            if let EMerkleTreeNode::File(file_node) = &node.node {
                let is_modified = util::fs::is_modified_from_node(&path, file_node)?;
                log::debug!("is_modified {} {:?}", is_modified, relative_path);
                if is_modified {
                    modified.insert(relative_path.clone());
                }
            }
        } else {
            log::debug!("find_changes entry is not a child node {:?}", path);
            // If it's none of the above conditions
            // then check if it's untracked or modified
            let mut found_file = false;
            if let Some(search_node) = &search_node {
                if let EMerkleTreeNode::File(file_node) = &search_node.node {
                    found_file = true;
                    if util::fs::is_modified_from_node(&path, file_node)? {
                        modified.insert(relative_path.clone());
                    }
                }
            }
            log::debug!("find_changes found_file {:?} {:?}", found_file, path);

            if !found_file {
                untracked.add_file(relative_path.clone());
                untracked_count += 1;
            }
        }
    }

    // Only add the untracked directory if it's not the root directory
    // and it's not staged or committed
    if untracked.all_untracked
        && search_node_path != Path::new("")
        && !in_staged_data(search_node_path, staged_data)?
        && full_path.is_dir()
        && search_node.is_none()
    {
        untracked.add_dir(search_node_path.to_path_buf(), untracked_count);
        // Clear individual files as they're now represented by the directory
        untracked.files.clear();
    }

    // Check for removed files
    if let Some(dir_hash) = dir_hashes.get(search_node_path) {
        // if we have subtree paths, don't check for removed files that are outside of the subtree
        if let Some(subtree_paths) = repo.subtree_paths() {
            if !subtree_paths.contains(&search_node_path.to_path_buf()) {
                return Ok((untracked, modified, removed));
            }

            if subtree_paths.len() == 1 && subtree_paths[0] == PathBuf::from("") {
                // If the subtree is the root, we need to check for removed files in the root
                let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
                if let Some(node) = dir_node {
                    for child in CommitMerkleTree::node_files_and_folders(&node)? {
                        if let EMerkleTreeNode::File(file_node) = &child.node {
                            let file_path = full_path.join(file_node.name());
                            if !file_path.exists() {
                                removed.insert(search_node_path.join(file_node.name()));
                            }
                        }
                    }
                }
                return Ok((untracked, modified, removed));
            }
        }

        let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
        if let Some(node) = dir_node {
            for child in CommitMerkleTree::node_files_and_folders(&node)? {
                if let EMerkleTreeNode::File(file_node) = &child.node {
                    let file_path = full_path.join(file_node.name());
                    if !file_path.exists() {
                        removed.insert(search_node_path.join(file_node.name()));
                    }
                } else if let EMerkleTreeNode::Directory(dir) = &child.node {
                    let dir_path = full_path.join(dir.name());
                    let relative_dir_path = search_node_path.join(dir.name());
                    if !dir_path.exists() {
                        // Only call this for non-existant dirs, because existant dirs already trigger a find_changes call

                        let mut count: usize = 0;
                        count_removed_entries(
                            repo,
                            &relative_dir_path,
                            dir.hash(),
                            &gitignore,
                            &mut count,
                        )?;

                        *total_entries += count;
                        removed.insert(relative_dir_path);
                    }
                }
            }
        }
    }

    Ok((untracked, modified, removed))
}

// Traverse the merkle tree to count removed entries under a dir node
fn count_removed_entries(
    repo: &LocalRepository,
    relative_path: &Path,
    dir_hash: &MerkleHash,
    gitignore: &Option<Gitignore>,
    removed_entries: &mut usize,
) -> Result<(), OxenError> {
    if oxenignore::is_ignored(relative_path, gitignore, true) {
        return Ok(());
    }

    let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
    if let Some(ref node) = dir_node {
        for child in CommitMerkleTree::node_files_and_folders(node)? {
            if let EMerkleTreeNode::File(_) = &child.node {
                // Any files nodes accessed here are children of a removed dir, so they must also be removed
                *removed_entries += 1;
            } else if let EMerkleTreeNode::Directory(dir) = child.node {
                let relative_dir_path = relative_path.join(dir.name());
                count_removed_entries(
                    repo,
                    &relative_dir_path,
                    dir.hash(),
                    gitignore,
                    removed_entries,
                )?;
            }
        }
    }

    Ok(())
}

// Helper functions (implement these based on your existing code)
fn open_staged_db(
    repo: &LocalRepository,
) -> Result<Option<DBWithThreadMode<SingleThreaded>>, OxenError> {
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    if db_path.join("CURRENT").exists() {
        // Read the staged files from the staged db
        let opts = db::key_val::opts::default();
        let db: DBWithThreadMode<SingleThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), true)?;
        Ok(Some(db))
    } else {
        Ok(None)
    }
}

fn get_dir_hashes(
    repo: &LocalRepository,
    head_commit_maybe: &Option<Commit>,
) -> Result<HashMap<PathBuf, MerkleHash>, OxenError> {
    if let Some(head_commit) = head_commit_maybe {
        Ok(CommitMerkleTree::dir_hashes(repo, head_commit)?)
    } else {
        Ok(HashMap::new())
    }
}

fn maybe_get_node(
    repo: &LocalRepository,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let path = path.as_ref();
    if let Some(hash) = dir_hashes.get(path) {
        CommitMerkleTree::read_depth(repo, hash, 1)
    } else {
        CommitMerkleTree::read_file(repo, dir_hashes, path)
    }
}

fn is_staged(
    path: &Path,
    staged_db: &Option<DBWithThreadMode<SingleThreaded>>,
) -> Result<bool, OxenError> {
    if let Some(staged_db) = staged_db {
        let key = path.to_str().unwrap();
        if staged_db.get(key.as_bytes())?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn in_staged_data(path: &Path, staged_data: &StagedData) -> Result<bool, OxenError> {
    if staged_data.staged_files.contains_key(path)
        || staged_data.staged_dirs.paths.contains_key(path)
    {
        return Ok(true);
    }
    Ok(false)
}

#[derive(Debug)]
struct UntrackedData {
    dirs: HashMap<PathBuf, usize>,
    files: Vec<PathBuf>,
    all_untracked: bool,
}

// TODO: After implementing this I realized that it has a lot in common with
// SummarizedStagedDirStats, and even with the StagedData struct. Since our
// status structure is probably pretty stable at this point, it might be worth
// looking into combining these structs to reduce duplication. However, we do
// handle staged and untracked data differently in a few places, so it might
// be more effort than it's worth.

impl UntrackedData {
    fn new() -> Self {
        Self {
            dirs: HashMap::new(),
            files: Vec::new(),
            all_untracked: true,
        }
    }

    fn add_dir(&mut self, path: PathBuf, count: usize) {
        // Check if this directory is a parent of any existing entries. It will
        // never be a child since we process child directories first.
        let subdirs: Vec<_> = self
            .dirs
            .keys()
            .filter(|k| k.starts_with(&path) && **k != path)
            .cloned()
            .collect();

        let total_count: usize = subdirs.iter().map(|k| self.dirs[k]).sum::<usize>() + count;

        for subdir in subdirs {
            self.dirs.remove(&subdir);
        }

        self.dirs.insert(path, total_count);
    }

    fn add_file(&mut self, file_path: PathBuf) {
        self.files.push(file_path);
    }

    fn merge(&mut self, other: UntrackedData) {
        // Since we process child directories first, we can just extend
        self.dirs.extend(other.dirs);
        self.files.extend(other.files);
        self.all_untracked = self.all_untracked && other.all_untracked;
    }
}

fn maybe_get_child_node(
    path: impl AsRef<Path>,
    dir_children: &Option<HashMap<PathBuf, MerkleTreeNode>>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let Some(children) = dir_children else {
        return Ok(None);
    };

    let child = children.get(path.as_ref());
    Ok(child.cloned())
}

fn maybe_get_dir_children(
    dir_node: &Option<MerkleTreeNode>,
) -> Result<Option<HashMap<PathBuf, MerkleTreeNode>>, OxenError> {
    let Some(node) = dir_node else {
        return Ok(None);
    };

    if let EMerkleTreeNode::Directory(_) = &node.node {
        let children = repositories::tree::list_files_and_folders_map(node)?;
        Ok(Some(children))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::time::SystemTime;

    #[tokio::test]
    async fn test_merge_watcher_with_staged_empty_watcher() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create empty watcher status
            let watcher_status = WatcherStatus {
                untracked: HashSet::new(),
                modified: HashSet::new(),
                removed: HashSet::new(),
                scan_complete: true,
                last_updated: SystemTime::now(),
            };

            let opts = StagedDataOpts::default();

            // Run merge function
            let result = merge_watcher_with_staged(&repo, &opts, watcher_status)?;

            // Verify result has empty collections
            assert_eq!(result.untracked_files.len(), 0);
            assert_eq!(result.modified_files.len(), 0);
            assert_eq!(result.removed_files.len(), 0);
            assert_eq!(result.untracked_dirs.len(), 0);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_merge_watcher_with_untracked_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create watcher status with untracked files
            let mut untracked = HashSet::new();
            untracked.insert(PathBuf::from("file1.txt"));
            untracked.insert(PathBuf::from("dir/file2.txt"));
            untracked.insert(PathBuf::from("dir/subdir/file3.txt"));

            let watcher_status = WatcherStatus {
                untracked: untracked.clone(),
                modified: HashSet::new(),
                removed: HashSet::new(),
                scan_complete: true,
                last_updated: SystemTime::now(),
            };

            let opts = StagedDataOpts::default();

            // Run merge function
            let result = merge_watcher_with_staged(&repo, &opts, watcher_status)?;

            // Verify untracked files are present
            assert_eq!(result.untracked_files.len(), 3);
            assert!(result.untracked_files.contains(&PathBuf::from("file1.txt")));
            assert!(result
                .untracked_files
                .contains(&PathBuf::from("dir/file2.txt")));
            assert!(result
                .untracked_files
                .contains(&PathBuf::from("dir/subdir/file3.txt")));

            // Verify untracked directories are extracted
            assert_eq!(result.untracked_dirs.len(), 2);
            let dir_map: HashMap<PathBuf, usize> = result.untracked_dirs.into_iter().collect();
            assert_eq!(dir_map.get(&PathBuf::from("dir")), Some(&1));
            assert_eq!(dir_map.get(&PathBuf::from("dir/subdir")), Some(&1));

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_merge_watcher_with_modified_and_removed() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create watcher status with modified and removed files
            let mut modified = HashSet::new();
            modified.insert(PathBuf::from("modified1.txt"));
            modified.insert(PathBuf::from("modified2.txt"));

            let mut removed = HashSet::new();
            removed.insert(PathBuf::from("removed1.txt"));
            removed.insert(PathBuf::from("dir/removed2.txt"));

            let watcher_status = WatcherStatus {
                untracked: HashSet::new(),
                modified,
                removed,
                scan_complete: true,
                last_updated: SystemTime::now(),
            };

            let opts = StagedDataOpts::default();

            // Run merge function
            let result = merge_watcher_with_staged(&repo, &opts, watcher_status)?;

            // Verify modified files
            assert_eq!(result.modified_files.len(), 2);
            assert!(result
                .modified_files
                .contains(&PathBuf::from("modified1.txt")));
            assert!(result
                .modified_files
                .contains(&PathBuf::from("modified2.txt")));

            // Verify removed files
            assert_eq!(result.removed_files.len(), 2);
            assert!(result
                .removed_files
                .contains(&PathBuf::from("removed1.txt")));
            assert!(result
                .removed_files
                .contains(&PathBuf::from("dir/removed2.txt")));

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_merge_watcher_with_path_filtering() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create watcher status with files in different directories
            let mut untracked = HashSet::new();
            untracked.insert(PathBuf::from("dir1/file1.txt"));
            untracked.insert(PathBuf::from("dir2/file2.txt"));
            untracked.insert(PathBuf::from("dir3/file3.txt"));

            let mut modified = HashSet::new();
            modified.insert(PathBuf::from("dir1/modified.txt"));
            modified.insert(PathBuf::from("dir2/modified.txt"));

            let watcher_status = WatcherStatus {
                untracked,
                modified,
                removed: HashSet::new(),
                scan_complete: true,
                last_updated: SystemTime::now(),
            };

            // Create opts with specific path filter
            let opts = StagedDataOpts {
                paths: vec![repo.path.join("dir1")],
                ..StagedDataOpts::default()
            };

            // Run merge function
            let result = merge_watcher_with_staged(&repo, &opts, watcher_status)?;

            // Verify only dir1 files are included
            assert_eq!(result.untracked_files.len(), 1);
            assert!(result
                .untracked_files
                .contains(&PathBuf::from("dir1/file1.txt")));

            assert_eq!(result.modified_files.len(), 1);
            assert!(result
                .modified_files
                .contains(&PathBuf::from("dir1/modified.txt")));

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_merge_watcher_with_oxenignore() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create .oxenignore file
            let oxenignore_path = repo.path.join(".oxenignore");
            test::write_txt_file_to_path(&oxenignore_path, "*.log\ntemp/\n")?;

            // Create watcher status with ignored and non-ignored files
            let mut untracked = HashSet::new();
            untracked.insert(PathBuf::from("file.txt"));
            untracked.insert(PathBuf::from("debug.log")); // Should be ignored
            untracked.insert(PathBuf::from("temp/file.txt")); // Should be ignored
            untracked.insert(PathBuf::from("data/file.txt"));

            let watcher_status = WatcherStatus {
                untracked,
                modified: HashSet::new(),
                removed: HashSet::new(),
                scan_complete: true,
                last_updated: SystemTime::now(),
            };

            let opts = StagedDataOpts::default();

            // Run merge function
            let result = merge_watcher_with_staged(&repo, &opts, watcher_status)?;

            // Verify ignored files are filtered out
            assert_eq!(result.untracked_files.len(), 2);
            assert!(result.untracked_files.contains(&PathBuf::from("file.txt")));
            assert!(result
                .untracked_files
                .contains(&PathBuf::from("data/file.txt")));
            assert!(!result.untracked_files.contains(&PathBuf::from("debug.log")));
            assert!(!result
                .untracked_files
                .contains(&PathBuf::from("temp/file.txt")));

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_merge_watcher_extracts_directories() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create watcher status with files in nested directories
            let mut untracked = HashSet::new();
            untracked.insert(PathBuf::from("a/file1.txt"));
            untracked.insert(PathBuf::from("a/file2.txt"));
            untracked.insert(PathBuf::from("a/b/file3.txt"));
            untracked.insert(PathBuf::from("a/b/file4.txt"));
            untracked.insert(PathBuf::from("a/b/c/file5.txt"));
            untracked.insert(PathBuf::from("d/file6.txt"));

            let watcher_status = WatcherStatus {
                untracked,
                modified: HashSet::new(),
                removed: HashSet::new(),
                scan_complete: true,
                last_updated: SystemTime::now(),
            };

            let opts = StagedDataOpts::default();

            // Run merge function
            let result = merge_watcher_with_staged(&repo, &opts, watcher_status)?;

            // Verify all files are present
            assert_eq!(result.untracked_files.len(), 6);

            // Verify directories are correctly extracted with counts
            let dir_map: HashMap<PathBuf, usize> = result.untracked_dirs.into_iter().collect();
            assert_eq!(dir_map.get(&PathBuf::from("a")), Some(&2)); // 2 files directly in 'a'
            assert_eq!(dir_map.get(&PathBuf::from("a/b")), Some(&2)); // 2 files directly in 'a/b'
            assert_eq!(dir_map.get(&PathBuf::from("a/b/c")), Some(&1)); // 1 file in 'a/b/c'
            assert_eq!(dir_map.get(&PathBuf::from("d")), Some(&1)); // 1 file in 'd'

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_merge_watcher_all_file_types() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create watcher status with all types of changes
            let mut untracked = HashSet::new();
            untracked.insert(PathBuf::from("new1.txt"));
            untracked.insert(PathBuf::from("new2.txt"));

            let mut modified = HashSet::new();
            modified.insert(PathBuf::from("changed1.txt"));
            modified.insert(PathBuf::from("changed2.txt"));

            let mut removed = HashSet::new();
            removed.insert(PathBuf::from("deleted1.txt"));
            removed.insert(PathBuf::from("deleted2.txt"));

            let watcher_status = WatcherStatus {
                untracked: untracked.clone(),
                modified: modified.clone(),
                removed: removed.clone(),
                scan_complete: true,
                last_updated: SystemTime::now(),
            };

            let opts = StagedDataOpts::default();

            // Run merge function
            let result = merge_watcher_with_staged(&repo, &opts, watcher_status)?;

            // Verify all file types are present
            // Convert Vec to HashSet for comparison
            let result_untracked: HashSet<PathBuf> = result.untracked_files.into_iter().collect();
            assert_eq!(result_untracked, untracked);
            assert_eq!(result.modified_files, modified);
            assert_eq!(result.removed_files, removed);

            // Verify we have merge conflicts (should be empty for test repo)
            assert_eq!(result.merge_conflicts.len(), 0);

            Ok(())
        })
    }
}
