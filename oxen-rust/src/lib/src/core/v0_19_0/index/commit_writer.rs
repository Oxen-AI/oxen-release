use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;

use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};
use std::ops::AddAssign;
use std::path::PathBuf;
use std::str;
use std::time::Duration;
use std::time::Instant;
use time::OffsetDateTime;

use crate::config::UserConfig;
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::constants::{HEAD_FILE, STAGED_DIR};
use crate::core::db;
use crate::core::db::key_val::path_db;
use crate::core::db::key_val::str_val_db;
use crate::core::v0_10_0::index::RefWriter;
use crate::core::v0_19_0::index::merkle_tree::node::MerkleNodeDB;
use crate::core::v0_19_0::index::merkle_tree::node::{
    FileChunkType, FileNode, FileStorageType, MerkleTreeNodeType, VNode,
};
use crate::core::v0_19_0::index::merkle_tree::CommitMerkleTree;
use crate::core::v0_19_0::status;
use crate::core::v0_19_0::structs::EntryMetaDataWithPath;
use crate::error::OxenError;
use crate::model::NewCommit;
use crate::model::{Commit, EntryDataType, LocalRepository};

use crate::{repositories, util};

use super::merkle_tree::node::MerkleTreeNodeData;
use super::merkle_tree::node::{CommitNode, DirNode};

#[derive(Clone)]
pub struct EntryVNode {
    pub id: u128,
    pub entries: Vec<EntryMetaDataWithPath>,
}

impl EntryVNode {
    pub fn new(id: u128) -> Self {
        EntryVNode {
            id,
            entries: vec![],
        }
    }
}

pub struct DirTreeNode {
    pub dir: PathBuf,
    pub stats: AggregateStats,
    pub children: Vec<DirTreeNode>,
}

pub struct DirTree {
    pub root: DirTreeNode,
}

impl DirTree {
    pub fn print(&self) {
        log::debug!("DirTree 🌲");
        self.print_recursive(&self.root, 0);
    }

    fn print_recursive(&self, node: &DirTreeNode, depth: usize) {
        log::debug!(
            "  {}DirNode: {:?} {:?}",
            "  ".repeat(depth),
            node.dir,
            node.stats
        );
        for child in &node.children {
            self.print_recursive(child, depth + 1);
        }
    }
}

#[derive(Clone, Debug)]
struct AggregateStats {
    pub total_files: usize,
    pub total_dirs: usize,
    pub total_bytes: usize,
    pub data_type_counts: HashMap<EntryDataType, usize>,
}

impl AddAssign<AggregateStats> for AggregateStats {
    fn add_assign(&mut self, other: AggregateStats) {
        self.total_files += other.total_files;
        self.total_dirs += other.total_dirs;
        self.total_bytes += other.total_bytes;
        for (data_type, count) in other.data_type_counts {
            *self.data_type_counts.entry(data_type).or_insert(0) += count;
        }
    }
}

impl Default for AggregateStats {
    fn default() -> Self {
        AggregateStats {
            total_files: 0,
            total_dirs: 0,
            total_bytes: 0,
            data_type_counts: HashMap::new(),
        }
    }
}

pub fn commit(repo: &LocalRepository, message: impl AsRef<str>) -> Result<Commit, OxenError> {
    // time the commit
    let start_time = Instant::now();
    let message = message.as_ref();

    // Read the staged files from the staged db
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    log::debug!(
        "0.19.0::commit_writer::commit staged db path: {:?}",
        db_path
    );
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let commit_progress_bar = ProgressBar::new_spinner();
    commit_progress_bar.set_style(ProgressStyle::default_spinner());
    commit_progress_bar.enable_steady_tick(Duration::from_millis(100));

    // Read all the staged entries
    let (dir_entries, total_changes) =
        status::read_staged_entries(&staged_db, &commit_progress_bar)?;

    // let mut dir_tree = entries_to_dir_tree(&dir_entries)?;
    // dir_tree.print();

    // log::debug!("🫧======================🫧");

    // if the HEAD file exists, we have parents
    // otherwise this is the first commit
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);

    let maybe_head_commit = if head_path.exists() {
        let commit = repositories::commits::head_commit(repo)?;
        Some(commit)
    } else {
        None
    };

    let mut parent_ids = vec![];
    if let Some(parent) = &maybe_head_commit {
        parent_ids.push(parent.hash_u128());
    }

    // agg_stats(&repo, &maybe_head_commit, &mut dir_tree, &dir_entries)?;
    // dir_tree.print();

    /*
    Load all the merkle tree nodes that are in the staged dir entries. Then traverse up their
    parent directories and update the merkle tree nodes for those directories until you get to the root.

    For example - if we only modified files/dir_1/file_1.txt, then we need to update
    the merkle tree nodes for files/dir_1 and files and the root dir and commit node.

    But if we only modified README.md, then we only need to update the merkle tree nodes for the root dir and
    commit node.
    */

    // Next - just load the merkle tree nodes that match the dir_entries map
    //        we can traverse and update their parents if they exist
    //        if they don't exist, we need to create them

    let directories = dir_entries
        .keys()
        .map(|path| path.to_path_buf())
        .collect::<Vec<_>>();

    let mut existing_nodes: HashMap<PathBuf, MerkleTreeNodeData> = HashMap::new();
    if let Some(commit) = &maybe_head_commit {
        existing_nodes = CommitMerkleTree::load_nodes(&repo, &commit, &directories)?;
    }

    // TODO: Second commit
    //       - Find the vnodes we need to copy/modify
    //       - Write new vnodes for updated/added/deleted files
    // Right now it is just making a new commit with the staged files, not connecting them to the merkle tree

    // Sort children and split into VNodes
    let vnode_entries = split_into_vnodes(&dir_entries, &existing_nodes)?;

    // Compute the commit hash
    let cfg = UserConfig::get()?;
    let timestamp = OffsetDateTime::now_utc();
    let new_commit = NewCommit {
        parent_ids: parent_ids.iter().map(|id| format!("{:x}", id)).collect(),
        message: message.to_string(),
        author: cfg.name.clone(),
        email: cfg.email.clone(),
        timestamp: timestamp,
    };
    let commit_id = compute_commit_id(&new_commit)?;

    let node = CommitNode {
        id: commit_id,
        parent_ids: parent_ids,
        message: message.to_string(),
        author: cfg.name,
        email: cfg.email,
        timestamp: timestamp,
        ..Default::default()
    };

    commit_progress_bar.set_message(format!("Commiting {} changes", total_changes));

    let dir_hash_db_path = CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, commit_id);
    let dir_hash_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&dir_hash_db_path))?;

    // Commit node has no parent
    let parent_id = None;
    let mut commit_db = MerkleNodeDB::open_read_write(repo, &node, parent_id)?;
    write_commit_entries(
        &repo,
        commit_id,
        &mut commit_db,
        &dir_hash_db,
        &vnode_entries,
    )?;
    commit_progress_bar.finish_and_clear();

    // Write HEAD file and update branch
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);
    log::debug!("Looking for HEAD file at {:?}", head_path);
    let ref_writer = RefWriter::new(repo)?;
    let commit_id = format!("{:x}", commit_id);
    if !head_path.exists() {
        log::debug!("HEAD file does not exist, creating new branch");
        let branch_name = DEFAULT_BRANCH_NAME;
        ref_writer.set_head(branch_name);
        ref_writer.set_branch_commit_id(branch_name, &commit_id)?;
    }
    ref_writer.set_head_commit_id(&commit_id)?;

    // Clear the staged db
    path_db::clear(&staged_db)?;

    // Print that we finished
    println!(
        "🐂 commit {} in {}",
        commit_id,
        humantime::format_duration(Duration::from_millis(
            start_time.elapsed().as_millis() as u64
        ))
        .to_string()
    );

    Ok(node.to_commit())
}

fn entries_to_dir_tree(
    entries: &HashMap<PathBuf, Vec<EntryMetaDataWithPath>>,
) -> Result<DirTree, OxenError> {
    let mut root = DirTreeNode {
        dir: PathBuf::from(""),
        stats: AggregateStats::default(),
        children: vec![],
    };

    for (path, _) in entries.iter() {
        insert_dir_path(&mut root, path)?;
    }

    Ok(DirTree { root })
}

fn insert_dir_path(node: &mut DirTreeNode, path: &Path) -> Result<(), OxenError> {
    let mut current = node;
    for component in path.components() {
        if let std::path::Component::Normal(name) = component {
            let name = name
                .to_str()
                .ok_or_else(|| OxenError::basic_str("Invalid path component"))?;
            let child_index = current
                .children
                .iter()
                .position(|c| c.dir.file_name().unwrap_or_default() == name);

            if let Some(index) = child_index {
                current = &mut current.children[index];
            } else {
                let new_node = DirTreeNode {
                    dir: current.dir.join(name),
                    children: vec![],
                    stats: AggregateStats::default(),
                };
                current.children.push(new_node);
                current = current.children.last_mut().unwrap();
            }
        }
    }
    Ok(())
}

fn agg_stats(
    repo: &LocalRepository,
    commit: &Option<Commit>,
    dir_tree: &mut DirTree,
    entries: &HashMap<PathBuf, Vec<EntryMetaDataWithPath>>,
) -> Result<(), OxenError> {
    agg_stats_recursive(repo, commit, &mut dir_tree.root, entries, 0)?;
    Ok(())
}

fn agg_stats_recursive(
    repo: &LocalRepository,
    commit: &Option<Commit>,
    node: &mut DirTreeNode,
    entries: &HashMap<PathBuf, Vec<EntryMetaDataWithPath>>,
    depth: usize,
) -> Result<AggregateStats, OxenError> {
    let mut stats = AggregateStats {
        total_files: 0,
        total_dirs: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };

    // Recursively aggregate stats for children
    for child in &mut node.children {
        stats += agg_stats_recursive(repo, commit, child, entries, depth + 1)?;
    }

    // Write the dir node and the vnodes
    if let Some(children) = entries.get(&node.dir) {
        for entry in children.iter() {
            log::debug!(
                "    {} {:?} has {:?} bytes",
                "  ".repeat(depth),
                entry.path,
                entry.num_bytes
            );
            if entry.data_type != EntryDataType::Dir {
                stats.total_files += 1;
                stats.total_bytes += entry.num_bytes as usize;
            }

            if entry.data_type == EntryDataType::Dir {
                stats.total_dirs += 1;
            }
        }
    } else {
        // look up the old stats
        log::debug!("looking up old stats for {:?}", node.dir);
        let old_dir = CommitMerkleTree::from_path(repo, &commit.as_ref().unwrap(), &node.dir)?;
        let dir_node = old_dir.root.dir()?;
        stats.total_files = dir_node.num_files();
        stats.total_dirs += 1;
        stats.total_bytes = dir_node.num_bytes as usize;
    }

    log::debug!(
        "  {} {:?} {} files {} dirs {} bytes",
        "  ".repeat(depth),
        node.dir,
        stats.total_files,
        stats.total_dirs,
        bytesize::ByteSize::b(stats.total_bytes as u64),
    );

    node.stats = stats.clone();

    Ok(stats)
}

fn node_data_to_entry(
    base_dir: impl AsRef<Path>,
    node: &MerkleTreeNodeData,
) -> Result<Option<EntryMetaDataWithPath>, OxenError> {
    let base_dir = base_dir.as_ref();
    match node.dtype {
        MerkleTreeNodeType::Dir => {
            let dir_node = node.dir()?;
            Ok(Some(EntryMetaDataWithPath {
                path: base_dir.join(dir_node.name),
                data_type: EntryDataType::Dir,
                hash: node.hash,
                num_bytes: dir_node.num_bytes,
            }))
        }
        MerkleTreeNodeType::File => {
            let file_node = node.file()?;
            Ok(Some(EntryMetaDataWithPath {
                path: base_dir.join(file_node.name),
                data_type: file_node.data_type,
                hash: node.hash,
                num_bytes: file_node.num_bytes,
            }))
        }
        _ => Ok(None),
    }
}

fn get_node_dir_children(
    base_dir: impl AsRef<Path>,
    node: &MerkleTreeNodeData,
) -> Result<HashSet<EntryMetaDataWithPath>, OxenError> {
    let Ok(dir_node) = node.dir() else {
        let err_msg = format!("Node is not a directory {:?}", node);
        return Err(OxenError::basic_str(err_msg));
    };

    // let dir_path = dir_path.as_ref().to_path_buf();
    let mut children = HashSet::new();

    let dir_children = CommitMerkleTree::node_files_and_folders(&node)?;
    children = dir_children
        .into_iter()
        .map(|child| node_data_to_entry(&base_dir, &child))
        .flatten()
        .flatten()
        .collect();

    Ok(children)
}

// This should return the directory to vnode mapping that we need to update
fn split_into_vnodes(
    entries: &HashMap<PathBuf, Vec<EntryMetaDataWithPath>>,
    existing_nodes: &HashMap<PathBuf, MerkleTreeNodeData>,
) -> Result<HashMap<PathBuf, Vec<EntryVNode>>, OxenError> {
    let mut results: HashMap<PathBuf, Vec<EntryVNode>> = HashMap::new();

    log::debug!("split_into_vnodes entries keys: {:?}", entries.keys());
    log::debug!(
        "split_into_vnodes existing_nodes keys: {:?}",
        existing_nodes.keys()
    );

    // Create the VNode buckets per directory
    for (directory, new_children) in entries {
        let mut children = HashSet::new();

        // Lookup children in the existing merkle tree
        if let Some(existing_node) = existing_nodes.get(directory) {
            log::debug!("got existing node for {:?}", directory);
            children = get_node_dir_children(&directory, &existing_node)?;
            log::debug!(
                "got {} existing children for dir {:?}",
                children.len(),
                directory
            );
        } else {
            log::debug!("no existing node for {:?}", directory);
        };

        // Update the children with the new entries from status
        // TODO: Handle updates and deletes, this is pure addition right now
        for child in new_children.iter() {
            children.insert(child.clone());
        }

        // Log the children
        for child in children.iter() {
            log::debug!("  child {:?} has {:?} bytes", child.path, child.num_bytes);
        }

        // log2(N / 10000)
        let total_children = children.len();
        let num_vnodes = (total_children as f32 / 10000_f32).log2();
        let num_vnodes = 2u128.pow(num_vnodes.ceil() as u32);
        log::debug!(
            "{} VNodes for {} children in {:?}",
            num_vnodes,
            total_children,
            directory
        );
        let mut vnode_children: Vec<EntryVNode> = vec![EntryVNode::new(0); num_vnodes as usize];

        // Split entries into vnodes
        for child in children.into_iter() {
            let bucket = child.hash % num_vnodes;
            vnode_children[bucket as usize].entries.push(child.clone());
        }

        // Compute hashes and sort entries
        for vnode in vnode_children.iter_mut() {
            // Sort the entries in the vnode by path
            // to make searching for entries faster
            vnode.entries.sort_by(|a, b| a.path.cmp(&b.path));

            // Compute hash for the vnode
            let mut vnode_hasher = xxhash_rust::xxh3::Xxh3::new();
            vnode_hasher.update(b"vnode");
            // make sure to hash the directory name so that it will be unique
            // even if the entries are the same (for example two empty directories at different levels)
            vnode_hasher.update(directory.to_str().unwrap().as_bytes());
            for entry in vnode.entries.iter() {
                vnode_hasher.update(&entry.hash.to_le_bytes());
            }
            vnode.id = vnode_hasher.digest128();
        }

        // Sort before we hash
        results.insert(directory.to_owned(), vnode_children);
    }

    log::debug!("split_into_vnodes results: {:?}", results.len());
    for (dir, vnodes) in results.iter() {
        log::debug!("dir {:?} has {} vnodes", dir, vnodes.len());
        for vnode in vnodes.iter() {
            log::debug!("  vnode {:x} has {} entries", vnode.id, vnode.entries.len());
            for entry in vnode.entries.iter() {
                log::debug!("    entry {:?} has {:?} bytes", entry.path, entry.num_bytes);
            }
        }
    }

    Ok(results)
}

fn compute_commit_id(new_commit: &NewCommit) -> Result<u128, OxenError> {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.update(b"commit");
    hasher.update(format!("{:?}", new_commit.parent_ids).as_bytes());
    hasher.update(new_commit.message.as_bytes());
    hasher.update(new_commit.author.as_bytes());
    hasher.update(new_commit.email.as_bytes());
    hasher.update(&new_commit.timestamp.unix_timestamp().to_le_bytes());
    Ok(hasher.digest128())
}

fn write_commit_entries(
    repo: &LocalRepository,
    commit_id: u128,
    commit_db: &mut MerkleNodeDB,
    dir_hash_db: &DBWithThreadMode<MultiThreaded>,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
) -> Result<(), OxenError> {
    // Write the root dir, then recurse into the vnodes and subdirectories
    let mut total_written = 0;
    let root_path = PathBuf::from("");
    let dir_node = compute_dir_node(commit_id, entries, &root_path)?;
    commit_db.add_child(&dir_node)?;
    total_written += 1;

    str_val_db::put(
        dir_hash_db,
        root_path.to_str().unwrap(),
        &format!("{:x}", dir_node.hash),
    )?;
    let dir_db = MerkleNodeDB::open_read_write(repo, &dir_node, Some(commit_id))?;
    r_create_dir_node(
        repo,
        commit_id,
        &mut Some(dir_db),
        dir_hash_db,
        entries,
        root_path,
        &mut total_written,
    )?;

    Ok(())
}

fn r_create_dir_node(
    repo: &LocalRepository,
    commit_id: u128,
    maybe_dir_db: &mut Option<MerkleNodeDB>,
    dir_hash_db: &DBWithThreadMode<MultiThreaded>,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
    path: impl AsRef<Path>,
    total_written: &mut u64,
) -> Result<(), OxenError> {
    let path = path.as_ref().to_path_buf();

    let Some(vnodes) = entries.get(&path) else {
        let err_msg = format!(
            "r_create_dir_node No entries found for directory {:?}",
            path
        );
        return Err(OxenError::basic_str(err_msg));
    };

    log::debug!("Processing dir {:?} with {} vnodes", path, vnodes.len());
    for vnode in vnodes.iter() {
        let vnode_obj = VNode {
            id: vnode.id,
            ..Default::default()
        };
        if let Some(dir_db) = maybe_dir_db {
            dir_db.add_child(&vnode_obj)?;
            *total_written += 1;
        }
        log::debug!(
            "Processing vnode {:x} with {} entries",
            vnode.id,
            vnode.entries.len()
        );

        // Maybe because we don't need to overwrite vnode dbs that already exist,
        // but still need to recurse and create the children
        let mut maybe_vnode_db = MerkleNodeDB::open_read_write_if_not_exists(
            repo,
            &vnode_obj,
            maybe_dir_db.as_ref().map(|db| db.node_id),
        )?;
        for entry in vnode.entries.iter() {
            log::debug!(
                "Processing entry {:?} [{:?}] in vnode {:x}",
                entry.path,
                entry.data_type,
                vnode.id
            );
            match entry.data_type {
                EntryDataType::Dir => {
                    // I think here you just need to look up the old one and reference it
                    if entries.contains_key(&entry.path) {
                        let dir_node = compute_dir_node(commit_id, entries, &entry.path)?;
                        if let Some(vnode_db) = &mut maybe_vnode_db {
                            vnode_db.add_child(&dir_node)?;
                            *total_written += 1;
                        }

                        // Always write the dir hash to the dir_hashes db
                        str_val_db::put(
                            dir_hash_db,
                            entry.path.to_str().unwrap(),
                            &format!("{:x}", dir_node.hash),
                        )?;

                        // if the vnode is new, we need a new dir db
                        let mut child_db = if maybe_vnode_db.is_some() {
                            let dir_db =
                                MerkleNodeDB::open_read_write(repo, &dir_node, Some(vnode.id))?;
                            Some(dir_db)
                        } else {
                            // Otherwise, check if the dir is new before opening a new db
                            MerkleNodeDB::open_read_write_if_not_exists(
                                repo,
                                &dir_node,
                                Some(vnode.id),
                            )?
                        };

                        r_create_dir_node(
                            repo,
                            commit_id,
                            &mut child_db,
                            dir_hash_db,
                            entries,
                            &entry.path,
                            total_written,
                        )?;
                    } else {
                        log::debug!("r_create_dir_node skipping {:?}", entry.path);
                        // Look up the old dir node and reference it
                        let old_dir_node = CommitMerkleTree::read_node(repo, entry.hash, false)?;
                        let dir_node = old_dir_node.dir()?;
                        if let Some(vnode_db) = &mut maybe_vnode_db {
                            vnode_db.add_child(&dir_node)?;
                            *total_written += 1;
                        }
                    }
                }
                _ => {
                    let file_name = entry.path.file_name().unwrap_or_default().to_str().unwrap();

                    // Just single file chunk for now
                    let chunks = vec![entry.hash];
                    let file_node = FileNode {
                        name: file_name.to_owned(),
                        hash: entry.hash,
                        num_bytes: entry.num_bytes,
                        chunk_type: FileChunkType::SingleFile,
                        storage_backend: FileStorageType::Disk,
                        last_commit_id: commit_id,
                        last_modified_seconds: 0,
                        last_modified_nanoseconds: 0,
                        chunk_hashes: chunks,
                        data_type: entry.data_type.clone(),
                        mime_type: "".to_string(),
                        extension: "".to_string(),
                        dtype: MerkleTreeNodeType::File,
                    };
                    if let Some(vnode_db) = &mut maybe_vnode_db {
                        vnode_db.add_child(&file_node)?;
                        *total_written += 1;
                    }
                }
            }
        }
    }

    log::debug!(
        "Finished processing dir {:?} total written {} entries",
        path,
        total_written
    );

    Ok(())
}

fn get_children(
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
    dir_path: impl AsRef<Path>,
) -> Result<Vec<PathBuf>, OxenError> {
    let dir_path = dir_path.as_ref().to_path_buf();
    let mut children = vec![];

    for (path, _) in entries.iter() {
        if path.starts_with(&dir_path) {
            children.push(path.clone());
        }
    }

    return Ok(children);
}

fn compute_dir_node(
    commit_id: u128,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
    path: impl AsRef<Path>,
) -> Result<DirNode, OxenError> {
    let path = path.as_ref().to_path_buf();
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.update(b"dir");
    hasher.update(path.to_str().unwrap().as_bytes());

    let mut num_bytes = 0;
    let mut data_type_counts: HashMap<String, usize> = HashMap::new();

    let children = get_children(entries, &path)?;
    log::debug!("Aggregating dir {:?} with {:?} children", path, children);
    for child in children.iter() {
        let Some(vnodes) = entries.get(child) else {
            let err_msg = format!("compute_dir_node No entries found for directory {:?}", path);
            return Err(OxenError::basic_str(err_msg));
        };

        for vnode in vnodes.iter() {
            for entry in vnode.entries.iter() {
                log::debug!("Aggregating entry {:?} [{:?}]", entry.path, entry.data_type);
                match entry.data_type {
                    EntryDataType::Dir => {
                        log::debug!("No need to aggregate {:?}", entry.path);
                    }
                    _ => {
                        hasher.update(&entry.hash.to_le_bytes());
                        num_bytes += entry.num_bytes;

                        *data_type_counts
                            .entry(entry.data_type.to_string())
                            .or_insert(0) += 1;
                    }
                }
            }
        }
    }

    let hash = hasher.digest128();
    let file_name = path.file_name().unwrap_or_default().to_str().unwrap();
    log::debug!(
        "Aggregated dir {:?} num_bytes {:?} data_type_counts {:?}",
        path,
        num_bytes,
        data_type_counts
    );

    let node = DirNode {
        dtype: MerkleTreeNodeType::Dir,
        name: file_name.to_owned(),
        hash,
        num_bytes,
        last_commit_id: commit_id,
        last_modified_seconds: 0,
        last_modified_nanoseconds: 0,
        data_type_counts,
    };
    Ok(node)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::command;
    use crate::core::v0_19_0::index::merkle_tree::CommitMerkleTree;
    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;
    use crate::test::add_n_files_m_dirs;
    use crate::util;

    #[test]
    fn test_first_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = command::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 2)?;
            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            return Ok(());

            /*
            [Commit] 861d5cd233eff0940060bd76ce24f10a
              [Dir] ""
                [VNode]
                  [File] README.md
                  [File] files.csv
                  [Dir] files
                    [VNode]
                      [Dir] dir_0
                        [VNode]
                          [File] file4.txt
                          [File] file0.txt
                          [File] file2.txt
                          [File] file6.txt
                          [File] file8.txt
                      [Dir] dir_1
                        [VNode]
                          [File] file7.txt
                          [File] file3.txt
                          [File] file5.txt
                          [File] file1.txt
                          [File] file9.txt
            */

            // Make sure we have 4 vnodes
            let vnodes = tree.total_vnodes();
            assert_eq!(vnodes, 4);

            // Make sure the root is a commit node
            let root = &tree.root;
            let commit = root.commit();
            assert!(commit.is_ok());

            // Make sure the root commit has 1 child, the root dir node
            let root_commit_children = &root.children;
            assert_eq!(root_commit_children.len(), 1);

            let dir_node_data = root_commit_children.iter().next().unwrap();
            let dir_node = dir_node_data.dir();
            assert!(dir_node.is_ok());
            assert_eq!(dir_node.unwrap().name, "");

            // Make sure dir node has one child, the VNode
            let vnode_data = dir_node_data.children.iter().next().unwrap();
            let vnode = vnode_data.vnode();
            assert!(vnode.is_ok());

            // Make sure the vnode has 3 children, the 2 files and the dir
            let vnode_children = &vnode_data.children;
            assert_eq!(vnode_children.len(), 3);

            // Check that files.csv is in the merkle tree
            let has_paths_csv = tree.has_path(&Path::new("files.csv"))?;
            assert!(has_paths_csv);

            // Check that README.md is in the merkle tree
            let has_readme = tree.has_path(&Path::new("README.md"))?;
            assert!(has_readme);

            // Check that files/dir_0/file0.txt is in the merkle tree
            let has_path0 = tree.has_path(&Path::new("files/dir_0/file0.txt"))?;
            assert!(has_path0);

            Ok(())
        })
    }

    #[test]
    fn test_commit_only_dirs_at_top_level() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = command::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Add a new file to files/dir_0/
            let new_file = repo.path.join("all_files/dir_0/new_file.txt");
            util::fs::create_dir_all(&new_file.parent().unwrap())?;
            util::fs::write_to_path(&new_file, "New file")?;
            command::add(&repo, &repo.path)?;

            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            let has_path0 = tree.has_path(&Path::new("all_files/dir_0/new_file.txt"))?;
            assert!(has_path0);

            Ok(())
        })
    }

    #[test]
    fn test_commit_single_file_deep_in_dir() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = command::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Add a new file to files/dir_0/
            let new_file = repo.path.join("files/dir_0/new_file.txt");
            util::fs::create_dir_all(&new_file.parent().unwrap())?;
            util::fs::write_to_path(&new_file, "New file")?;
            command::add(&repo, &new_file)?;

            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            let has_path0 = tree.has_path(&Path::new("files/dir_0/new_file.txt"))?;
            assert!(has_path0);

            Ok(())
        })
    }

    #[test]
    fn test_second_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = command::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3)?;
            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let first_commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let first_tree = CommitMerkleTree::from_commit(&repo, &first_commit)?;
            first_tree.print();

            // Add a new file to files/dir_1/
            let new_file = repo.path.join("files/dir_1/new_file.txt");
            util::fs::write_to_path(&new_file, "New file")?;
            command::add(&repo, &new_file)?;

            // Commit the data
            let second_commit = super::commit(&repo, "Second commit")?;

            // Make sure commit hashes are different
            assert!(first_commit.id != second_commit.id);

            // Make sure the head commit is updated
            let head_commit = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, second_commit.id);

            // Read the merkle tree
            let second_tree = CommitMerkleTree::from_commit(&repo, &second_commit)?;
            second_tree.print();

            assert_eq!(second_tree.total_vnodes(), 5);

            assert!(!first_tree.has_path(&Path::new("files/dir_1/new_file.txt"))?);
            assert!(second_tree.has_path(&Path::new("files/dir_1/new_file.txt"))?);

            // Make sure the last commit id is updated on new_file.txt
            let updated_node = second_tree.get_by_path(&Path::new("files/dir_1/new_file.txt"))?;
            assert!(updated_node.is_some());
            let updated_file_node = updated_node.unwrap().file()?;
            let updated_commit_id = format!("{:x}", updated_file_node.last_commit_id);
            assert_eq!(updated_commit_id, second_commit.id);

            // Make sure that last commit id is not updated on other files in the dir
            let other_file_node = second_tree.get_by_path(&Path::new("files/dir_1/file7.txt"))?;
            assert!(other_file_node.is_some());
            let other_file_node = other_file_node.unwrap().file()?;
            let other_commit_id = format!("{:x}", other_file_node.last_commit_id);
            assert_eq!(other_commit_id, first_commit.id);

            // Make sure last commit is updated on the dir
            let dir_node = second_tree.get_by_path(&Path::new("files/dir_1"))?;
            assert!(dir_node.is_some());
            let dir_node = dir_node.unwrap().dir()?;
            let dir_commit_id = format!("{:x}", dir_node.last_commit_id);
            assert_eq!(dir_commit_id, second_commit.id);

            // Make sure the hashes of the directories are valid
            // We should update the hashes of dir_1 and all it's parents, but none of the siblings
            let first_tree_dir_1 = first_tree.get_by_path(&Path::new("files/dir_1"))?;
            let second_tree_dir_1 = second_tree.get_by_path(&Path::new("files/dir_1"))?;
            assert!(first_tree_dir_1.is_some());
            assert!(second_tree_dir_1.is_some());
            assert!(first_tree_dir_1.unwrap().hash != second_tree_dir_1.unwrap().hash);

            // Make sure there is one vnode in each dir
            let first_tree_vnodes = first_tree.get_vnodes_for_dir(&Path::new("files/dir_1"))?;
            let second_tree_vnodes = second_tree.get_vnodes_for_dir(&Path::new("files/dir_1"))?;
            assert_eq!(first_tree_vnodes.len(), 1);
            assert_eq!(second_tree_vnodes.len(), 1);

            // And that the vnode hashes are different
            assert!(first_tree_vnodes[0].hash != second_tree_vnodes[0].hash);

            // Siblings should be the same
            let first_tree_dir_0 = first_tree.get_by_path(&Path::new("files/dir_0"))?;
            let second_tree_dir_0 = second_tree.get_by_path(&Path::new("files/dir_0"))?;
            assert!(first_tree_dir_0.is_some());
            assert!(second_tree_dir_0.is_some());
            assert_eq!(
                first_tree_dir_0.unwrap().hash,
                second_tree_dir_0.unwrap().hash
            );

            // Parent should be updated
            let first_tree_files = first_tree.get_by_path(&Path::new("files"))?;
            let second_tree_files = second_tree.get_by_path(&Path::new("files"))?;
            assert!(first_tree_files.is_some());
            assert!(second_tree_files.is_some());
            assert!(first_tree_files.unwrap().hash != second_tree_files.unwrap().hash);

            // Root should be updated
            let first_tree_root = first_tree.get_by_path(&Path::new(""))?;
            let second_tree_root = second_tree.get_by_path(&Path::new(""))?;
            assert!(first_tree_root.is_some());
            assert!(second_tree_root.is_some());
            assert!(first_tree_root.unwrap().hash != second_tree_root.unwrap().hash);

            Ok(())
        })
    }
}
