use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::core::v0_19_0::index::merkle_node_db::node_db_path;
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{
    DirNodeWithPath, EMerkleTreeNode, FileNode, FileNodeWithDir, MerkleTreeNode,
};
use crate::model::{Commit, EntryDataType, LocalRepository, MerkleHash};

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

pub fn get_dir_with_children_recursive(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    CommitMerkleTree::dir_with_children_recursive(repo, commit, path)
}

pub fn get_entries(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Vec<FileNode>, OxenError> {
    if let Some(dir_node) = CommitMerkleTree::dir_with_children(repo, commit, &path)? {
        log::debug!("get_entries found dir node: {dir_node:?}");
        CommitMerkleTree::dir_entries(&dir_node)
    } else {
        return Err(OxenError::basic_str(format!(
            "Error: path not found in tree: {:?}",
            path.as_ref()
        )));
    }
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
        log::debug!("Found child: {child}");
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

/*
pub fn list_files_by_type(
    repo: &LocalRepository,
    commit: &Commit,
    data_type: &EntryDataType,
) -> Result<HashSet<FileNode>, OxenError> {
    let mut file_nodes = HashSet::new();
    r_list_files_and_dirs(&tree.root, PathBuf::new(), &mut file_nodes, &mut dir_nodes)?;
    Ok((file_nodes, dir_nodes))
}

pub fn list_tabular_files_in_repo(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<FileNode>, OxenError> {
    let entries = get_entries(repo, commit, "")?;
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use uuid::Uuid;

    use crate::core::v0_10_0::cache;
    use crate::core::v0_10_0::index;
    use crate::error::OxenError;
    use crate::opts::PaginateOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[test]
    fn test_list_tabular() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create a deeply nested directory
            let dir_path = repo
                .path
                .join("data")
                .join("train")
                .join("images")
                .join("cats");
            util::fs::create_dir_all(&dir_path)?;

            // Add two tabular files to it
            let filename = "cats.tsv";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            let filename = "dogs.csv";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "1,2,3\nhello,world,sup\n")?;

            // And write a file in the same dir that is not tabular
            let filename = "README.md";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // And write a tabular file to the root dir
            let filename = "labels.tsv";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // And write a non tabular file to the root dir
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // Add and commit all
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            // List files
            let entries = repositories::entries::list_tabular_files_in_repo(&repo, &commit)?;

            assert_eq!(entries.len(), 3);

            // Add another tabular file
            let filename = "dogs.tsv";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // Add and commit all
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding additional file")?;

            let entries = repositories::entries::list_tabular_files_in_repo(&repo, &commit)?;

            assert_eq!(entries.len(), 4);

            // Remove the deeply nested dir
            util::fs::remove_dir_all(&dir_path)?;

            repositories::add(&repo, dir_path)?;
            let commit = repositories::commit(&repo, "Removing dir")?;

            let entries = repositories::entries::list_tabular_files_in_repo(&repo, &commit)?;
            assert_eq!(entries.len(), 2);

            Ok(())
        })
    }
}
*/