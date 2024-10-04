use crate::constants::OXEN_HIDDEN_DIR;
use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{
    Commit, LocalRepository, MerkleHash, StagedData, StagedDirStats, StagedEntry,
    StagedEntryStatus, StagedSchema, SummarizedStagedDirStats,
};
use crate::{repositories, util};

use filetime::FileTime;
use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, IteratorMode, SingleThreaded};
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::time::Duration;

use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;

pub fn status(repo: &LocalRepository) -> Result<StagedData, OxenError> {
    status_from_dir(repo, Path::new(""))
}

pub fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    println!("status_from_dir {:?}", dir.as_ref());
    let staged_db_maybe = open_staged_db(repo)?;
    let head_commit = repositories::commits::head_commit_maybe(repo)?;
    let dir_hashes = get_dir_hashes(repo, &head_commit)?;
    let relative_dir = util::fs::path_relative_to_dir(dir.as_ref(), &repo.path)?;

    let read_progress = ProgressBar::new_spinner();
    read_progress.set_style(ProgressStyle::default_spinner());
    read_progress.enable_steady_tick(Duration::from_millis(100));

    let (untracked, modified, removed) =
        find_changes(repo, &relative_dir, &staged_db_maybe, &dir_hashes)?;

    log::debug!("find_changes untracked: {:?}", untracked);
    log::debug!("find_changes modified: {:?}", modified);
    log::debug!("find_changes removed: {:?}", removed);

    let mut staged_data = StagedData::empty();
    staged_data.untracked_dirs = untracked.dirs.into_iter().collect();
    staged_data.untracked_files = untracked.files;
    staged_data.modified_files = modified;
    staged_data.removed_files = removed;

    let Some(staged_db) = staged_db_maybe else {
        log::debug!("status_from_dir no staged db, returning early");
        return Ok(staged_data);
    };

    let (dir_entries, _) = read_staged_entries_below_path(repo, &staged_db, &dir, &read_progress)?;
    // log::debug!("status_from_dir dir_entries: {:?}", dir_entries);
    read_progress.finish_and_clear();

    status_from_dir_entries(&mut staged_data, dir_entries)
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

    log::debug!("dir_entries.len(): {:?}", dir_entries.len());
    for (dir, entries) in dir_entries {
        log::debug!(
            "dir_entries dir: {:?} entries.len(): {:?}",
            dir,
            entries.len()
        );
        let stats = StagedDirStats {
            path: dir.clone(),
            num_files_staged: 0,
            total_files: 0,
            status: StagedEntryStatus::Added,
        };
        for entry in entries {
            match &entry.node.node {
                EMerkleTreeNode::Directory(node) => {
                    log::debug!("dir_entries dir_node: {}", node);
                }
                EMerkleTreeNode::File(node) => {
                    // TODO: It's not always added. It could be modified.
                    log::debug!("dir_entries file_node: {}", entry);
                    let file_path = PathBuf::from(&node.name);
                    if entry.status == StagedEntryStatus::Modified {
                        staged_data.modified_files.push(dir.join(&file_path));
                    }
                    let staged_entry = StagedEntry {
                        hash: node.hash.to_string(),
                        status: entry.status,
                    };
                    staged_data
                        .staged_files
                        .insert(file_path.clone(), staged_entry);
                    maybe_add_schemas(node, staged_data)?;

                    // Cannot be removed if it's staged
                    if staged_data.removed_files.contains(&file_path) {
                        staged_data.removed_files.remove(&file_path);
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
        summarized_dir_stats.add_stats(&stats);
    }

    staged_data.staged_dirs = summarized_dir_stats;

    Ok(staged_data.clone())
}

fn maybe_add_schemas(node: &FileNode, staged_data: &mut StagedData) -> Result<(), OxenError> {
    if let Some(GenericMetadata::MetadataTabular(m)) = &node.metadata {
        let schema = m.tabular.schema.clone();
        let path = PathBuf::from(&node.name);
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
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, u64), OxenError> {
    read_staged_entries_below_path(repo, db, Path::new(""), read_progress)
}

pub fn read_staged_entries_below_path(
    repo: &LocalRepository,
    db: &DBWithThreadMode<SingleThreaded>,
    start_path: impl AsRef<Path>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, u64), OxenError> {
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
                let entry: StagedMerkleTreeNode = rmp_serde::from_slice(&value).unwrap();
                log::debug!("read_staged_entries key {key} entry: {entry} path: {path:?}");
                let full_path = repo.path.join(path);

                if full_path.is_dir() {
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
    for (dir, entries) in dir_entries.iter() {
        log::debug!("commit dir_entries dir {:?}", dir);
        for entry in entries.iter() {
            log::debug!("\tcommit dir_entries entry {}", entry);
        }
    }

    Ok((dir_entries, total_entries))
}

fn find_changes(
    repo: &LocalRepository,
    relative_dir: impl AsRef<Path>,
    staged_db: &Option<DBWithThreadMode<SingleThreaded>>,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
) -> Result<(UntrackedData, Vec<PathBuf>, HashSet<PathBuf>), OxenError> {
    let relative_dir = relative_dir.as_ref();
    log::debug!("find_changes dir: {:?}", relative_dir);
    println!("find_changes {relative_dir:?} START");
    let mut untracked = UntrackedData::new();
    let mut modified = Vec::new();
    let mut removed = HashSet::new();

    let full_dir = repo.path.join(relative_dir);

    let Ok(entries) = std::fs::read_dir(&full_dir) else {
        return Err(OxenError::basic_str(format!(
            "Could not read dir {:?}",
            full_dir
        )));
    };
    let mut untracked_count = 0;
    let dir_node = get_dir_node(repo, dir_hashes, relative_dir)?;

    for entry in entries.flatten() {
        let path = entry.path();
        let relative_path = util::fs::path_relative_to_dir(&path, &repo.path)?;

        if is_ignored(&relative_path) {
            continue;
        }

        if is_staged(&relative_path, staged_db)? {
            untracked.all_untracked = false;
            continue;
        }

        if path.is_dir() {
            // If it's a directory, recursively find changes below it
            let (sub_untracked, sub_modified, sub_removed) =
                find_changes(repo, &relative_path, staged_db, dir_hashes)?;
            // if !sub_untracked.is_empty() {
            untracked.merge(sub_untracked);
            println!(
                "##### {:?} merge sub_untracked {:?}",
                relative_dir, untracked
            );
            // } else {
            //     all_untracked = false;
            // }
            modified.extend(sub_modified);
            removed.extend(sub_removed);
        } else if let Some(node) =
            maybe_get_child_node(&relative_path.file_name().unwrap(), &dir_node)?
        {
            log::debug!("##### got child node {}", node);
            // If we have a dir node, it's either tracked (clean) or modified
            // Either way, we know the directory is not all_untracked
            untracked.all_untracked = false;
            if is_modified(&node, &path)? {
                modified.push(relative_path);
            }
        } else {
            // If it's none of the above conditions, then it's untracked
            untracked.add_file(relative_path);
            untracked_count += 1;
        }
    }

    // Only add the untracked directory if it's not the root directory
    if untracked.all_untracked && relative_dir != Path::new("") {
        untracked.add_dir(relative_dir.to_path_buf(), untracked_count);
        // Clear individual files as they're now represented by the directory
        untracked.files.clear();
    }

    // Check for removed files
    if let Some(dir_hash) = dir_hashes.get(relative_dir) {
        let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 2)?;
        if let Some(node) = dir_node {
            for child in CommitMerkleTree::node_files_and_folders(&node)? {
                if let EMerkleTreeNode::File(file) = &child.node {
                    let file_path = full_dir.join(&file.name);
                    if !file_path.exists() {
                        removed.insert(relative_dir.join(&file.name));
                    }
                }
            }
        }
    }

    println!("find_changes {relative_dir:?} untracked: {:?}", untracked);
    // println!("find_changes {relative_dir:?} modified: {:?}", modified);
    // println!("find_changes {relative_dir:?} removed: {:?}", removed);
    Ok((untracked, modified, removed))
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
        Ok(CommitMerkleTree::dir_hashes(repo, &head_commit)?)
    } else {
        Ok(HashMap::new())
    }
}

fn get_dir_node(
    repo: &LocalRepository,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    dir: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let dir = dir.as_ref();
    if let Some(hash) = dir_hashes.get(dir) {
        CommitMerkleTree::read_depth(repo, hash, 2)
    } else {
        Ok(None)
    }
}

fn is_ignored(path: &Path) -> bool {
    // Skip hidden .oxen files
    if path.starts_with(OXEN_HIDDEN_DIR) {
        return true;
    }
    false
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

fn find_untracked_and_modified_paths(
    repo: &LocalRepository,
    start_path: impl AsRef<Path>,
    staged_data: &mut StagedData,
    staged_db: &Option<DBWithThreadMode<SingleThreaded>>,
    progress: &ProgressBar,
) -> Result<(), OxenError> {
    let start_path = start_path.as_ref();
    let mut seen_paths: HashSet<PathBuf> = HashSet::new();
    let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;

    // Candidate directories are the start path and all the directories in the
    // current tree that are descendants of the start path
    let mut candidate_dirs: HashSet<PathBuf> = HashSet::new();
    candidate_dirs.insert(start_path.to_path_buf());

    log::debug!(
        "find_untracked_and_modified_paths start_path: {:?}",
        start_path
    );

    // Add all the directories that are direct children of the start path
    let relative_start_path = util::fs::path_relative_to_dir(start_path, &repo.path)?;
    let repo_start_path = repo.path.join(relative_start_path);

    if repo_start_path.exists() {
        let dirs = std::fs::read_dir(&repo_start_path)?;
        for dir in dirs {
            let dir = dir?.path();
            if dir.is_dir() {
                let dir = util::fs::path_relative_to_dir(&dir, &repo.path)?;
                // Skip hidden .oxen files
                if dir.starts_with(OXEN_HIDDEN_DIR) {
                    continue;
                }
                candidate_dirs.insert(dir);
            }
        }
    }

    // Add all the directories that are in the head commit
    let dir_hashes = if let Some(head_commit) = maybe_head_commit {
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, &head_commit)?;
        for (dir, _) in &dir_hashes {
            let dir = repo.path.join(dir);
            if dir.starts_with(&repo_start_path) && dir != repo_start_path {
                candidate_dirs.insert(dir);
            }
        }
        dir_hashes
    } else {
        HashMap::new()
    };

    // Use a HashMap so we can uniquely store each untracked directory with its count
    let mut untracked_dirs: HashMap<PathBuf, usize> = HashMap::new();

    // List the directories in the current tree, and check if they have untracked files
    // Files in working directory as candidates
    let mut total_files = 0;
    for candidate_dir in &candidate_dirs {
        let relative_dir = util::fs::path_relative_to_dir(candidate_dir, &repo.path)?;
        log::debug!(
            "find_untracked_and_modified_paths finding untracked files in {:?} relative {:?}",
            candidate_dir,
            relative_dir
        );
        let dir_node = if let Some(hash) = dir_hashes.get(&relative_dir) {
            log::debug!(
                "find_untracked_and_modified_paths dir node for {:?} is {:?}",
                relative_dir,
                hash
            );
            CommitMerkleTree::read_depth(repo, hash, 2)?
        } else {
            None
        };

        let full_dir = repo.path.join(&relative_dir);
        let read_dir = std::fs::read_dir(&full_dir);
        if read_dir.is_ok() {
            log::debug!(
                "find_untracked_and_modified_paths adding untracked candidate from dir {:?}",
                candidate_dir
            );

            // Consider the current directory and all its children
            let mut paths = vec![candidate_dir.to_path_buf()];
            for path in read_dir? {
                let path = path?.path();
                paths.push(path);
            }

            let mut all_untracked = true;
            let mut untracked_files = Vec::new();

            for path in paths {
                total_files += 1;
                progress.set_message(format!("Checking {} untracked files", total_files));

                let relative_path = util::fs::path_relative_to_dir(&path, &repo.path)?;
                log::debug!(
                    "find_untracked_and_modified_paths checking relative path {:?} in {:?}",
                    relative_path,
                    relative_dir
                );

                // Don't add the directories that are in the current tree
                if candidate_dirs.contains(&path) {
                    continue;
                }

                // Skip hidden .oxen files
                if relative_path.starts_with(OXEN_HIDDEN_DIR) {
                    continue;
                }

                // Skip duplicates
                if !seen_paths.insert(path.to_path_buf()) {
                    continue;
                }

                // Skip if the file is staged
                if let Some(staged_db) = staged_db {
                    let key = relative_path.to_str().unwrap();
                    if staged_db.get(key.as_bytes())?.is_some() {
                        all_untracked = false;
                        continue;
                    }
                }

                let file_name = path
                    .file_name()
                    .ok_or(OxenError::basic_str("path has no file name"))?;
                if let Some(node) = maybe_get_child_node(file_name, &dir_node)? {
                    // if the node exists in the current tree, then it's not untracked
                    log::debug!(
                        "find_untracked_and_modified_paths checking if modified {:?}",
                        relative_path
                    );
                    if is_modified(&node, &path)? {
                        log::debug!(
                            "find_untracked_and_modified_paths file {:?} is modified",
                            path
                        );
                        staged_data.modified_files.push(relative_path);
                    }
                    all_untracked = false;
                } else {
                    if path.is_file() {
                        untracked_files.push(relative_path);
                    } else if path.is_dir() {
                        // Recursively check if the subdirectory is entirely untracked
                        let mut sub_staged_data = StagedData::empty();
                        find_untracked_and_modified_paths(
                            repo,
                            &path,
                            &mut sub_staged_data,
                            staged_db,
                            progress,
                        )?;

                        if sub_staged_data.untracked_dirs.is_empty()
                            && sub_staged_data.untracked_files.is_empty()
                        {
                            all_untracked = false;
                        }

                        // Merge sub_staged_data into the current staged_data
                        staged_data
                            .modified_files
                            .extend(sub_staged_data.modified_files);
                        staged_data
                            .removed_files
                            .extend(sub_staged_data.removed_files);
                        staged_data
                            .untracked_files
                            .extend(sub_staged_data.untracked_files);

                        // Merge untracked directories
                        for (sub_dir, count) in sub_staged_data.untracked_dirs {
                            *untracked_dirs.entry(sub_dir).or_default() += count;
                        }
                    }
                }
            }

            log::debug!("##### find_untracked_and_modified_paths start_path: {start_path:?} untracked_dirs: {untracked_dirs:?}");

            // After processing all paths in the directory
            if all_untracked && !untracked_files.is_empty() {
                *untracked_dirs
                    .entry(relative_dir.to_path_buf())
                    .or_default() += untracked_files.len();
            } else {
                staged_data.untracked_files.extend(untracked_files);
            }
        } else {
            log::error!(
                "find_untracked_and_modified_paths error reading dir {:?}",
                full_dir
            );
        }

        // Loop over the children in the dir node and check if they are removed
        if let Some(dir_node) = dir_node {
            let full_dir = repo.path.join(&relative_dir);
            let files = CommitMerkleTree::node_files_and_folders(&dir_node)?;
            for child in files {
                log::debug!(
                    "find_untracked_and_modified_paths checking if child {} is removed",
                    child
                );
                if let EMerkleTreeNode::File(file) = &child.node {
                    if is_removed(file, &full_dir) {
                        log::debug!(
                            "find_untracked_and_modified_paths is removed! dir {:?} child {}",
                            full_dir,
                            child
                        );
                        staged_data
                            .removed_files
                            .insert(relative_dir.join(&file.name));
                    }
                }
            }
        }
    }

    // Convert the HashMap to the final format and store it in staged_data
    staged_data.untracked_dirs = untracked_dirs.into_iter().collect();

    log::debug!(
        "find_untracked_and_modified_paths done removed_files: {:?}",
        staged_data.removed_files
    );

    Ok(())
}

fn maybe_get_child_node(
    path: impl AsRef<Path>,
    dir_node: &Option<MerkleTreeNode>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let Some(node) = dir_node else {
        log::debug!("##### maybe_get_child_node no parent");
        return Ok(None);
    };

    log::debug!(
        "##### maybe_get_child_node {:?} dir {}",
        path.as_ref(),
        node.dir().unwrap().name
    );
    node.get_by_path(path)
}

fn is_modified(node: &MerkleTreeNode, full_path: impl AsRef<Path>) -> Result<bool, OxenError> {
    // Check the file timestamps vs the commit timestamps
    let metadata = std::fs::metadata(full_path)?;
    let mtime = FileTime::from_last_modification_time(&metadata);

    let (node_modified_seconds, node_modified_nanoseconds) = match &node.node {
        EMerkleTreeNode::File(file) => {
            let node_modified_seconds = file.last_modified_seconds;
            let node_modified_nanoseconds = file.last_modified_nanoseconds;
            (node_modified_seconds, node_modified_nanoseconds)
        }
        EMerkleTreeNode::Directory(dir) => {
            let node_modified_seconds = dir.last_modified_seconds;
            let node_modified_nanoseconds = dir.last_modified_nanoseconds;
            (node_modified_seconds, node_modified_nanoseconds)
        }
        _ => {
            return Err(OxenError::basic_str("unsupported node type"));
        }
    };

    if node_modified_nanoseconds != mtime.nanoseconds()
        || node_modified_seconds != mtime.unix_seconds()
    {
        return Ok(true);
    }

    Ok(false)
}

fn is_removed(node: &FileNode, dir_path: impl AsRef<Path>) -> bool {
    let path = dir_path.as_ref().join(&node.name);
    log::debug!("is_removed checking if {:?} is removed", path);
    !path.exists()
}
