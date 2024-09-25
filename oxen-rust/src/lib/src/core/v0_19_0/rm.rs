use crate::core::db;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::repositories;
use crate::util;

use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::model::merkle_tree::node::FileNode;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use rocksdb::IteratorMode;
use tokio::time::Duration;
use walkdir::WalkDir;

use crate::core::v0_19_0::add::CumulativeStats;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use std::sync::Mutex;

use crate::constants::STAGED_DIR;
use crate::constants::VERSIONS_DIR;
use crate::model::Commit;
use crate::model::StagedEntryStatus;

use rmp_serde::Serializer;
use serde::Serialize;

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;

use crate::constants::FILES_DIR;
use crate::constants::OXEN_HIDDEN_DIR;

use rocksdb::{DBWithThreadMode, MultiThreaded};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub async fn rm(
    paths: &HashSet<PathBuf>,
    repo: &LocalRepository,
    opts: &RmOpts,
) -> Result<(), OxenError> {
    if repo.is_shallow_clone() {
        return Err(OxenError::repo_is_shallow());
    }

    /*
    if opts.remote {
        return remove_remote(repo, opts).await;
    }
    */

    // TODO: Accurately calculate stats for remove_staged
    if opts.staged {
        return remove_staged(repo, paths);
    }

    remove(paths, repo, opts)?;

    Ok(())
}

pub fn remove_staged_recursively(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
    // Iterate over staged_db and check if the path starts with the given path
    let iter = staged_db.iterator(IteratorMode::Start);
    for item in iter {
        match item {
            Ok((key, _)) => match str::from_utf8(&key) {
                Ok(key) => {
                    let db_path = PathBuf::from(key);
                    if db_path.starts_with(path) {
                        remove_staged_entry(&db_path, &staged_db)?;
                    }
                }
                _ => {
                    return Err(OxenError::basic_str("Could not read utf8 val..."));
                }
            },
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(())
}

/*
fn remove(
    paths: &HashSet<PathBuf>,
    repo: &LocalRepository,
    opts: &RmOpts,
) -> Result<(), OxenError> {
    // Should always be a head commit if we are calling rm
    let commit = repositories::commits::head_commit(repo)?;
    let tree = repositories::tree::get_by_commit(repo, &commit)?;

    // Full Path "data/tests/repo_uuid/images/path.jpg"

    // Repo Path "data/tests/repo_uuid/"

    // Path to remove "images/path.jpg"

    // FileNode "path.jpg"

    for path in paths {
        let path = util::fs::path_relative_to_dir(&path, &repo.path)?;
        let Some(node) = tree.get_by_path(&path)? else {
            log::error!("Path {} not found in tree", path.display());
            continue;
        };

        match &node.node {
            EMerkleTreeNode::File(file_node) => {
                remove_file(repo, &path, &file_node)?;
            }
            EMerkleTreeNode::Directory(dir_node) => {
                // remove_dir(repo, &commit, path.to_path_buf())?;
                todo!()
            }
            _ => {
                log::error!("Unsupported node type: {:?}", node.node);
            }
        }
    }

    Ok(())
}
 */

fn remove(
    paths: &HashSet<PathBuf>,
    repo: &LocalRepository,
    opts: &RmOpts,
) -> Result<CumulativeStats, OxenError> {
    let start = std::time::Instant::now();
    log::debug!("paths: {:?}", paths);

    // Head commit should always exist here, because we're removing committed files
    let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? else {
        let error = format!("Error: head commit not found");
        return Err(OxenError::basic_str(error));
    };

    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };

    for path in paths {

        // Get parent node
        let path = util::fs::path_relative_to_dir(path, &repo.path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        let parent_node: MerkleTreeNode = if let Some(dir_node) = CommitMerkleTree::dir_with_children(repo, &head_commit, parent_path)? {
            dir_node
        } else {
            let error = format!("Error: parent dir not found in tree for {path:?}");
            return Err(OxenError::basic_str(error));
        };

        log::debug!("Path is: {path:?}");

        // Get file name without parent path for lookup in Merkle Tree
        let relative_path = util::fs::path_relative_to_dir(path.clone(), &parent_path)?;
        log::debug!("Relative path is: {relative_path:?}");

        // Lookup node in Merkle Tree
        if let Some(node) = parent_node.get_by_path(relative_path.clone())? {
            if let EMerkleTreeNode::Directory(_) = &node.node {

                if !opts.recursive {
                    let error = format!("`oxen rm` on directory {path:?} requires -r");
                    return Err(OxenError::basic_str(error));
                }

                total += remove_dir(&repo, &head_commit, &path)?;
                // Remove dir from working directory
                let full_path = repo.path.join(relative_path);
                log::debug!("REMOVING DIR: {full_path:?}");
                if full_path.exists() {
                    // user might have removed dir manually before using `oxen rm`
                    util::fs::remove_dir_all(&full_path)?;
                }
                // TODO: Currently, there's no way to avoid re-staging the parent dirs with glob paths
                // Potentially, we can could a mutex global to all paths?
            } else if let EMerkleTreeNode::File(file_node) = &node.node {

                remove_file(&repo, &path, &file_node)?;
                let full_path = repo.path.join(relative_path);
                log::debug!("REMOVING FILE: {full_path:?}");
                if full_path.exists() {
                     // user might have removed file manually before using `oxen rm`
                    util::fs::remove_file(&full_path)?;
                }
            } else {
                let error = format!("Error: Unexpected file type");
                return Err(OxenError::basic_str(error));
            }
        } else {
            let error = format!("Error: {path:?} must be committed in order to use `oxen rm`");
            return Err(OxenError::basic_str(error));
        }

    }


    // Stop the timer, and round the duration to the nearest second
    let duration = Duration::from_millis(start.elapsed().as_millis() as u64);
    log::debug!("---END--- oxen rm: {:?} duration: {:?}", paths, duration);

    // TODO: Add function to CumulativeStats to output that print statement
    println!(
        "🐂 oxen removed {} files ({}) in {}",
        total.total_files,
        bytesize::ByteSize::b(total.total_bytes),
        humantime::format_duration(duration)
    );

    Ok(total)
}

pub fn remove_staged(repo: &LocalRepository, paths: &HashSet<PathBuf>) -> Result<(), OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    for path in paths {
        remove_staged_entry(&path, &staged_db)?;
    }

    Ok(())
}

fn remove_staged_entry(
    path: &PathBuf,
    staged_db: &DBWithThreadMode<MultiThreaded>,
) -> Result<(), OxenError> {
    log::debug!(
        "remove_staged path: {:?} from staged db {:?}",
        path,
        staged_db
    );
    staged_db.delete(path.to_str().unwrap())?;
    Ok(())
}

pub fn remove_file(
    repo: &LocalRepository,
    path: &Path,
    node: &FileNode,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let repo_path = repo.path.clone();
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    // Remove the file from the versions db
    // Take first 2 chars of hash as dir prefix and last N chars as the dir suffix
    let dst_dir = util::fs::version_dir_from_hash(&repo_path, node.hash.to_string());

    if dst_dir.exists() {
        // Remove the file from the versions db
        util::fs::remove_dir_all(&dst_dir)?;
    }

    let mut node = node.clone();
    // Node must have full path for delete to work
    node.name = path.to_string_lossy().to_string();
    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Removed,
        node: MerkleTreeNode::from_file(node),
    };

    // Write removed node to staged db
    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();
    let relative_path = util::fs::path_relative_to_dir(path, &repo_path)?;
    let relative_path_str = relative_path.to_str().unwrap();
    log::debug!(
        "writing removed file to staged db: {} -> {}",
        relative_path_str,
        staged_entry
    );
    staged_db.put(relative_path_str, &buf).unwrap();

    // Remove the file from disk
    let full_path = repo_path.join(&relative_path);
    if full_path.exists() {
        util::fs::remove_file(&full_path)?;
    } else {
        log::debug!("file not found on disk: {}", full_path.display());
    }

    // Write all the parent dirs as removed
    let mut parent_path = relative_path.to_path_buf();
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, &repo_path).unwrap();
        parent_path = parent.to_path_buf();

        let relative_path_str = relative_path.to_str().unwrap();

        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Modified,
            node: MerkleTreeNode::default_dir_from_path(&relative_path),
        };

        log::debug!("writing modified dir to staged db: {}", dir_entry);
        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db.put(relative_path_str, &buf).unwrap();

        if relative_path == Path::new("") {
            break;
        }
    }

    Ok(Some(staged_entry))
}

/*
pub fn remove_file(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
    file_node: &FileNode
) -> Result<CumulativeStats, OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let path = util::fs::path_relative_to_dir(path, &repo.path)?;

    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };

    // TODO: This is ugly, but the only current solution to get the stats from the removed file
    match process_remove_file_and_parents(
        &repo,
        &path,
        &staged_db,
        &file_node,
    ) {
        Ok(Some(node)) => {
            if let EMerkleTreeNode::File(file_node) = &node.node.node {;
                total.total_bytes += file_node.num_bytes;
                total.total_files += 1;
                total
                    .data_type_counts
                    .entry(file_node.data_type.clone())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
            Ok(total)
        }
        Err(e) => {
            let error = format!("Error adding file {path:?}: {:?}", e);
            return Err(OxenError::basic_str(error));
        }
        _ => {
            let error = format!("Error adding file {path:?}");
            return Err(OxenError::basic_str(error));
        }
    }
}

// Stages the file_node as removed, and all its parents in the repo as modified
fn process_remove_file_and_parents(
    repo: &LocalRepository,
    path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    file_node: &FileNode,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {

    let repo_path = repo.path.clone();
    let mut update_node = file_node.clone();
    update_node.name = path.to_string_lossy().to_string();

    let node = MerkleTreeNode::from_file(update_node);

    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Removed,
        node,
    };

    // Remove the file from the versions db
    // Take first 2 chars of hash as dir prefix and last N chars as the dir suffix
    let dst_dir = util::fs::version_dir_from_hash(&repo_path, &node.hash.to_string());

    if dst_dir.exists() {
        util::fs::remove_dir_all(&dst_dir)?;
    }

    // Write removed node to staged db
    log::debug!("writing removed file to staged db: {}", staged_entry);
    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();

    let node_path = path.to_str().unwrap();
    staged_db.put(node_path, &buf).unwrap();

    // Add all the parent dirs to the staged db
    let mut parent_path = path.to_path_buf();
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, repo_path.clone())?;
        parent_path = parent.to_path_buf();

        let relative_path_str = relative_path.to_str().unwrap();

        // Ensures that removed entries don't have their parents re-added by oxen rm
        // RocksDB's DBWithThreadMode only has this function to check if a key exists in the DB, so I added the else condition to make this reliable

        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Modified,
            node: MerkleTreeNode::default_dir_from_path(&relative_path),
        };

        log::debug!("writing dir to staged db: {}", dir_entry);
        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db.put(relative_path_str, &buf).unwrap();

        if relative_path == Path::new("") {
            break;
        }
    }

    Ok(Some(staged_entry))
}
*/

pub fn process_remove_file(
    repo: &LocalRepository,
    path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    file_node: &FileNode,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let repo_path = repo.path.clone();
    let relative_path = util::fs::path_relative_to_dir(path, repo_path)?;

    let mut update_node = file_node.clone();
    update_node.name = path.to_string_lossy().to_string();

    let node = MerkleTreeNode::from_file(update_node);

    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Removed,
        node,
    };

    // Write removed node to staged db
    log::debug!("writing removed file to staged db: {}", staged_entry);
    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();

    let relative_path_str = relative_path.to_str().unwrap();
    staged_db.put(relative_path_str, &buf).unwrap();

    Ok(Some(staged_entry))
}

pub fn remove_dir(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<CumulativeStats, OxenError> {
    log::debug!("remove_dir called");
    let versions_path = util::fs::oxen_hidden_dir(&repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let relative_path = util::fs::path_relative_to_dir(path, &repo.path)?;

    let dir_node = match CommitMerkleTree::dir_with_children_recursive(&repo, &commit, &path)? {
        Some(node) => node,
        None => {
            let error = format!("Error: {path:?} must be committed in order to use `oxen rm`");
            return Err(OxenError::basic_str(error));
        }
    };

    process_remove_dir(repo, path, &dir_node, &staged_db)
}

// Stage dir and all its children for removal
fn process_remove_dir(
    repo: &LocalRepository,
    path: &Path,
    dir_node: &MerkleTreeNode,
    staged_db: &DBWithThreadMode<MultiThreaded>,
) -> Result<CumulativeStats, OxenError> {


    let start = std::time::Instant::now();
    log::debug!("Process Remove Dir");

    let progress_1 = Arc::new(ProgressBar::new_spinner());
    progress_1.set_style(ProgressStyle::default_spinner());
    progress_1.enable_steady_tick(Duration::from_millis(100));

    // root_path is the path of the directory rm was called on
    let root_path = path;
    let repo = repo.clone();
    let repo_path = repo.path.clone();

    let progress_1_clone = Arc::clone(&progress_1);

    // recursive helper function
    let cumulative_stats = r_process_remove_dir(&repo, &path, dir_node, &staged_db);

    // Add all the parent dirs to the staged db
    let mut parent_path = path.to_path_buf();
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, repo_path.clone())?;
        parent_path = parent.to_path_buf();

        let relative_path_str = relative_path.to_str().unwrap();

        // Ensures that removed entries don't have their parents re-added by oxen rm
        // RocksDB's DBWithThreadMode only has this function to check if a key exists in the DB, so I added the else condition to make this reliable

        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Modified,
            node: MerkleTreeNode::default_dir_from_path(&relative_path),
        };

        log::debug!("writing dir to staged db: {}", dir_entry);
        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db.put(relative_path_str, &buf).unwrap();

        if relative_path == Path::new("") {
            break;
        }
    }

    progress_1_clone.finish_and_clear();

    cumulative_stats
}


 // Recursively remove all files and directories starting from a particular directory
 // WARNING: This function relies on the initial dir having the correct relative path to the repo

 // TODO: Refactor to singular match statement/loop
 // TODO: Currently, this function is only called sequentially. Consider using Arc/AtomicU64 to parallelize
 fn r_process_remove_dir(repo: &LocalRepository,
                         path: &Path,
                         node: &MerkleTreeNode,
                         staged_db: &DBWithThreadMode<MultiThreaded>,
                    ) -> Result<CumulativeStats, OxenError>
{

    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };

    // Iterate through children, removing files
    for child in &node.children {
        match &child.node {
            EMerkleTreeNode::Directory(dir_node) => {

                // Update path, and move to the next level of recurstion
                let new_path = path.join(&dir_node.name);
                total += r_process_remove_dir(repo, &new_path, child, staged_db)?;
            }
            EMerkleTreeNode::VNode(_) => {

                // Move to the next level of recursion
                total += r_process_remove_dir(repo, path, child, staged_db)?;
            },
            EMerkleTreeNode::File(file_node) => {

                // Add the relative path of the dir to the path
                let new_path = path.join(&file_node.name);

                // Remove the file node and add its stats to the totals
                match process_remove_file(repo, &new_path, staged_db, &file_node) {
                    Ok(Some(node)) => {
                        if let EMerkleTreeNode::File(file_node) = &node.node.node {;
                            total.total_bytes += file_node.num_bytes;
                            total.total_files += 1;
                            total
                                .data_type_counts
                                .entry(file_node.data_type.clone())
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                        }
                    }
                    Err(e) => {
                        let error = format!("Error adding file {new_path:?}: {:?}", e);
                        return Err(OxenError::basic_str(error));
                    }
                    _ => {
                        let error = format!("Error adding file {new_path:?}");
                        return Err(OxenError::basic_str(error));
                    }
                }
            },
            _ => {
                let error = format!("Error: Unexpected node type");
                return Err(OxenError::basic_str(error));
            }
        }
    }

    match &node.node {

        // if node is a Directory, stage it for removal
        EMerkleTreeNode::Directory(_) => {
            // node has the correct relative path to the dir, so no need for updates
            let staged_entry = StagedMerkleTreeNode {
                status: StagedEntryStatus::Removed,
                node: node.clone(),
            };

            // Write removed node to staged db
            log::debug!("writing removed dir to staged db: {}", staged_entry);
            let mut buf = Vec::new();
            staged_entry
                .serialize(&mut Serializer::new(&mut buf))
                .unwrap();

            let relative_path_str = path.to_str().unwrap();
            staged_db.put(relative_path_str, &buf).unwrap();
        }

        // if node is a VNode, do nothing
        EMerkleTreeNode::VNode(_) => {}

        // node should always be a directory or vnode, so any other types result in an error
        _ => {
            return Err(OxenError::basic_str(format!(
                "Unexpected node type: {:?}",
                node.node.dtype()
            )))
            }
    }

    Ok(total)
}