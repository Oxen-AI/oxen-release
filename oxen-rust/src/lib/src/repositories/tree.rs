use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::core::v0_19_0::index::merkle_node_db::node_db_path;
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{
    DirNodeWithPath, EMerkleTreeNode, FileNode, FileNodeWithDir, MerkleTreeNode,
};
use crate::model::{Commit, LocalRepository, MerkleHash};

pub fn get_by_commit(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<CommitMerkleTree, OxenError> {
    CommitMerkleTree::from_commit(repo, commit)
}

pub fn get_node_by_id(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let load_recursive = false;
    CommitMerkleTree::read_node(repo, hash, load_recursive)
}

pub fn get_node_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let load_recursive = false;
    let node = CommitMerkleTree::from_path(repo, commit, path, load_recursive)?;
    Ok(Some(node.root))
}

pub fn get_file_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    let load_recursive = false;
    let tree = CommitMerkleTree::from_path(repo, commit, path, load_recursive)?;
    match tree.root.node {
        EMerkleTreeNode::File(file_node) => Ok(Some(file_node.clone())),
        _ => Ok(None),
    }
}

pub fn get_dir_with_children(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    CommitMerkleTree::dir_with_children(repo, commit, path)
}

pub fn get_dir_without_children(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    CommitMerkleTree::dir_without_children(repo, commit, path)
}

pub fn get_node_data_by_id(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let Some(node) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Ok(None);
    };
    Ok(Some(node))
}

pub fn list_missing_file_hashes(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let Some(node) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Err(OxenError::basic_str(format!("Node {} not found", hash)));
    };
    node.list_missing_file_hashes(repo)
}

pub fn list_missing_node_hashes(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let mut results = HashSet::new();
    for hash in hashes {
        let dir_prefix = node_db_path(repo, hash);
        if dir_prefix.exists() {
            results.insert(*hash);
        }
    }
    Ok(results)
}

pub fn child_hashes(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Vec<MerkleHash>, OxenError> {
    let Some(node) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Err(OxenError::basic_str(format!("Node {} not found", hash)));
    };
    let mut children = vec![];
    for child in node.children {
        children.push(child.hash);
    }
    Ok(children)
}

/// Collect MerkleTree into Directories and Files
pub fn list_files_and_dirs(
    tree: &CommitMerkleTree,
) -> Result<(HashSet<FileNodeWithDir>, HashSet<DirNodeWithPath>), OxenError> {
    let mut file_nodes = HashSet::new();
    let mut dir_nodes = HashSet::new();
    r_list_files_and_dirs(&tree.root, PathBuf::new(), &mut file_nodes, &mut dir_nodes)?;
    Ok((file_nodes, dir_nodes))
}

fn r_list_files_and_dirs(
    node: &MerkleTreeNode,
    traversed_path: impl AsRef<Path>,
    file_nodes: &mut HashSet<FileNodeWithDir>,
    dir_nodes: &mut HashSet<DirNodeWithPath>,
) -> Result<(), OxenError> {
    let traversed_path = traversed_path.as_ref();
    for child in &node.children {
        match &child.node {
            EMerkleTreeNode::File(file_node) => {
                file_nodes.insert(FileNodeWithDir {
                    file_node: file_node.to_owned(),
                    dir: traversed_path.to_owned(),
                });
            }
            EMerkleTreeNode::Directory(dir_node) => {
                let new_path = traversed_path.join(&dir_node.name);
                if new_path != PathBuf::from("") {
                    dir_nodes.insert(DirNodeWithPath {
                        dir_node: dir_node.to_owned(),
                        path: new_path.to_owned(),
                    });
                }
                r_list_files_and_dirs(child, new_path, file_nodes, dir_nodes)?;
            }
            EMerkleTreeNode::VNode(_) => {
                r_list_files_and_dirs(child, traversed_path, file_nodes, dir_nodes)?;
            }
            _ => {}
        }
    }
    Ok(())
}
