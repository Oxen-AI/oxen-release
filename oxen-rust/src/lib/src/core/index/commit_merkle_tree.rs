use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str;

use rocksdb::{DBWithThreadMode, MultiThreaded};

use crate::constants::TREE_DIR;
use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db::merkle_node_db::MerkleNodeDB;
use crate::core::db::{self, str_val_db};

use crate::core::index::commit_merkle_tree_node::CommitMerkleTreeNode;
use crate::error::OxenError;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::util;

use super::commit_merkle_tree_node::MerkleTreeNodeType;

#[derive(Debug, Deserialize, Serialize)]
pub enum MerkleNodeType {
    Dir,
    VNode,
    File,
    Schema,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MerkleNode {
    pub dtype: MerkleNodeType,
    pub path: String,
}

pub struct CommitMerkleTree {}

impl CommitMerkleTree {
    // Commit db is the directories per commit
    // .oxen/history/{COMMIT_ID}/dir_hashes
    fn commit_db_dir(repo: &LocalRepository, commit: &Commit) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(&commit.id)
            .join(DIR_HASHES_DIR)
    }

    // Global merkle tree db
    // .oxen/tree/{hash}
    fn tree_db_dir(repo: &LocalRepository, node: &CommitMerkleTreeNode) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(TREE_DIR)
            .join(&node.hash)
    }

    pub fn read(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitMerkleTreeNode, OxenError> {
        let root_path = Path::new("");
        CommitMerkleTree::read_path(repo, commit, root_path)
    }

    pub fn read_path(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<CommitMerkleTreeNode, OxenError> {
        let node_path = path.as_ref();
        let node_db_dir = CommitMerkleTree::commit_db_dir(repo, commit);
        let node_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&db::opts::default(), node_db_dir, false)?;
        let mut node_path_str = node_path.to_str().unwrap();

        // If it ends with a /, remove it
        if node_path_str.ends_with('/') {
            node_path_str = &node_path_str[..node_path_str.len() - 1];
        }

        let node_hash: Option<String> = str_val_db::get(&node_db, node_path_str)?;
        if let Some(node_hash) = node_hash {
            // We are reading a directory
            CommitMerkleTree::read_dir(repo, node_path, node_hash, true)
        } else {
            // We are reading a file
            CommitMerkleTree::read_file(repo, &node_db, node_path)
        }
    }

    // TODO: How do we quickly look up a file in the merkle tree?
    fn read_file(
        repo: &LocalRepository,
        node_db: &DBWithThreadMode<MultiThreaded>,
        path: impl AsRef<Path>,
    ) -> Result<CommitMerkleTreeNode, OxenError> {
        // Get the directory from the path
        let path = path.as_ref();
        let node_path = path.parent().unwrap();
        let file_name = path.file_name().unwrap().to_str().unwrap();

        // TODO: This is ugly, abstract lookup of initial dir out
        let node_path_str = node_path.to_str().unwrap();

        let node_hash: Option<String> = str_val_db::get(node_db, node_path_str)?;
        let Some(node_hash) = node_hash else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for path: {}",
                node_path_str
            )));
        };

        // SUPER INTERESTING
        // loading 240 rocksdb dbs (even small dbs) is slow
        // takes over 1s, which means if we can optimize the load time
        // of a VNode we are cooking

        // In contrast, just skipping to the dir vnode, scanning, and opening
        // one is super fast ~8ms total, which makes sense,
        // because it's like 4ms per open, and 4*240 = 960ms

        // So if we can write our own fast kv format for these small dbs
        // we potentially will get a big boost

        let vnodes = CommitMerkleTree::read_dir(repo, node_path, node_hash, false)?;
        for vnode in vnodes.children.into_iter() {
            let file_path_hash = util::hasher::hash_path(path);
            // println!("File: {:?} -> {}", path, file_path_hash);
            // println!("Is in VNode? {:?}", vnode);

            // Check if first two chars of hashes match
            if file_path_hash.get(0..2).unwrap() == vnode.path.to_str().unwrap() {
                // println!("Found file in VNode! {:?}", vnode);
                let children = CommitMerkleTree::read_dir(repo, node_path, vnode.hash, false)?;
                for child in children.children.into_iter() {
                    if child.path.to_str().unwrap() == file_name {
                        return Ok(child);
                    }
                }
            }
        }

        Err(OxenError::basic_str(format!(
            "Merkle tree hash not found for path: {}",
            node_path_str
        )))
    }

    fn read_dir(
        repo: &LocalRepository,
        path: impl AsRef<Path>,
        node_hash: String,
        recurse: bool,
    ) -> Result<CommitMerkleTreeNode, OxenError> {
        // Dir hashes are stored with extra quotes in the db, remove them
        let node_hash = node_hash.replace('"', "");

        let mut node = CommitMerkleTreeNode {
            path: path.as_ref().to_path_buf(),
            hash: node_hash,
            dtype: MerkleTreeNodeType::Dir,
            children: HashSet::new(),
        };
        CommitMerkleTree::read_children_from_node(repo, &mut node, recurse)?;
        Ok(node)
    }

    fn read_children_from_node(
        repo: &LocalRepository,
        node: &mut CommitMerkleTreeNode,
        recurse: bool,
    ) -> Result<(), OxenError> {
        let tree_db_dir = CommitMerkleTree::tree_db_dir(repo, node);
        if !tree_db_dir.exists() {
            log::error!("Could not open {:?}", tree_db_dir);
            return Ok(());
        }

        if node.dtype != MerkleTreeNodeType::Dir && node.dtype != MerkleTreeNodeType::VNode {
            return Ok(());
        }

        // let tree_db: DBWithThreadMode<MultiThreaded> =
        //     DBWithThreadMode::open_for_read_only(&db::opts::default(), &tree_db_dir, false)?;
        // let iter = tree_db.iterator(IteratorMode::Start);

        // for (key, val) in iter.flatten() {
        //     let key = str::from_utf8(&key)?;
        //     let val: MerkleNode = rmp_serde::from_slice(&val).unwrap();

        let mut tree_db = MerkleNodeDB::open(&tree_db_dir, true)?;
        let vals: HashMap<u128, MerkleNode> = tree_db.map()?;

        for (hash, val) in vals {
            let key = format!("{:x}", hash);
            match &val.dtype {
                MerkleNodeType::Dir => {
                    let mut child = CommitMerkleTreeNode {
                        path: PathBuf::from(&val.path),
                        hash: key.to_owned(),
                        dtype: MerkleTreeNodeType::Dir,
                        children: HashSet::new(),
                    };
                    if recurse {
                        CommitMerkleTree::read_children_from_node(repo, &mut child, recurse)?;
                    }
                    node.children.insert(child);
                }
                MerkleNodeType::VNode => {
                    let mut child = CommitMerkleTreeNode {
                        path: PathBuf::from(&val.path),
                        hash: key.to_owned(),
                        dtype: MerkleTreeNodeType::VNode,
                        children: HashSet::new(),
                    };
                    if recurse {
                        CommitMerkleTree::read_children_from_node(repo, &mut child, recurse)?;
                    }
                    node.children.insert(child);
                }
                MerkleNodeType::File => {
                    let child = CommitMerkleTreeNode {
                        path: PathBuf::from(&val.path),
                        hash: key.to_owned(),
                        dtype: MerkleTreeNodeType::File,
                        children: HashSet::new(),
                    };
                    node.children.insert(child);
                }
                MerkleNodeType::Schema => {
                    let child = CommitMerkleTreeNode {
                        path: PathBuf::from(&val.path),
                        hash: key.to_owned(),
                        dtype: MerkleTreeNodeType::Schema,
                        children: HashSet::new(),
                    };
                    node.children.insert(child);
                }
            }
        }

        Ok(())
    }

    pub fn print_depth(node: &CommitMerkleTreeNode, depth: i32) {
        CommitMerkleTree::r_print(node, 0, depth);
    }

    pub fn print(node: &CommitMerkleTreeNode) {
        // print all the way down
        CommitMerkleTree::r_print(node, 0, -1);
    }

    fn r_print(node: &CommitMerkleTreeNode, indent: i32, depth: i32) {
        if depth != -1 && depth > 0 && indent >= depth {
            return;
        }

        if MerkleTreeNodeType::VNode == node.dtype {
            println!(
                "{}[{:?}] {:?} -> {} ({})",
                "  ".repeat(indent as usize),
                node.dtype,
                node.path,
                node.hash,
                node.children.len()
            );
        } else {
            println!(
                "{}[{:?}] {:?} -> {}",
                "  ".repeat(indent as usize),
                node.dtype,
                node.path,
                node.hash
            );
        }
        for child in &node.children {
            CommitMerkleTree::r_print(child, indent + 1, depth);
        }
    }
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;

    use super::*;

    #[test]
    fn test_read_commit_merkle_tree() -> Result<(), OxenError> {
        let repo_path = Path::new("data")
            .join("test")
            .join("commit_dbs")
            .join("repo");
        let repo = LocalRepository::new(&repo_path)?;
        let commit = Commit {
            id: String::from("64f2e2e90a49d4fe9f52b95a053ad3fe"),
            parent_ids: vec![],
            message: String::from("initial commit"),
            author: String::from("Ox"),
            email: String::from("ox@oxen.ai"),
            timestamp: OffsetDateTime::now_utc(),
            root_hash: None,
        };

        /*
        Our test db looks like this:
        .
        └── images
            ├── test
            │   ├── dandelion
            │   ├── roses
            │   └── tulips
            └── train
                ├── daisy
                ├── roses
                └── tulips
        */

        let root = CommitMerkleTree::read(&repo, &commit)?;

        assert_eq!(root.hash, "64f2e2e90a49d4fe9f52b95a053ad3fe");
        assert_eq!(root.children.len(), 1);

        // Make sure "images" and "train" are in the root children
        assert!(root
            .children
            .iter()
            .any(|x| x.path == PathBuf::from("images")));

        // Get the "images" child
        let images = root.get_by_path(PathBuf::from("images"));
        assert!(images.is_some());
        assert_eq!(images.unwrap().children.len(), 2);

        // Make sure "test" and "train" are in the "images" children
        assert!(images
            .unwrap()
            .children
            .iter()
            .any(|x| x.path == PathBuf::from("images/test")));
        assert!(images
            .unwrap()
            .children
            .iter()
            .any(|x| x.path == PathBuf::from("images/train")));

        // Get the "test" child
        let test = images.unwrap().get_by_path(PathBuf::from("images/test"));
        assert!(test.is_some());
        assert_eq!(test.unwrap().children.len(), 3);

        // Get the "dandelion" child
        let dandelion = test
            .unwrap()
            .get_by_path(PathBuf::from("images/test/dandelion"));
        assert!(dandelion.is_some());

        Ok(())
    }
}
