use std::collections::HashSet;
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use std::str;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};

use crate::constants::HISTORY_DIR;
use crate::constants::TREE_DIR;
use crate::core::db::{self, str_val_db};

use crate::error::OxenError;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::util;
use crate::core::index::commit_merkle_tree_node::CommitMerkleTreeNode;

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

pub struct CommitMerkleTree {
    pub root: CommitMerkleTreeNode,
}

impl CommitMerkleTree {
    pub fn new(repo: &LocalRepository, commit: &Commit) -> Result<CommitMerkleTree, OxenError> {
        let root = CommitMerkleTree::read_tree(repo, commit)?;
        Ok(CommitMerkleTree { root })
    }

    pub fn get_tree_node(&self, path: impl AsRef<Path>) -> Option<&CommitMerkleTreeNode> {
        self.root.get_by_path(path)
    }

    // Commit db is the directories per commit
    // .oxen/history/{COMMIT_ID}/tree/path
    fn commit_db_dir(repo: &LocalRepository, commit: &Commit, path: impl AsRef<Path>) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(&commit.id)
            .join(TREE_DIR)
            .join(path.as_ref())
    }

    // Global merkle tree db
    // .oxen/tree/{hash}
    fn tree_db_dir(repo: &LocalRepository, node: &CommitMerkleTreeNode) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(TREE_DIR)
            .join(&node.hash)
    }

    fn read_tree(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitMerkleTreeNode, OxenError> {
        let root_path = Path::new("");
        let root_db_dir = CommitMerkleTree::commit_db_dir(repo, commit, root_path);
        let root_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&db::opts::default(), &root_db_dir, false)?;
        let root_hash: Option<String> = str_val_db::get(&root_db, "")?;
        let Some(root_hash) = root_hash else {
            return Err(OxenError::basic_str("Root hash not found"));
        };

        let mut root = CommitMerkleTreeNode {
            path: root_path.to_path_buf(),
            // unwrap is safe because we checked that there is exactly one child
            hash: root_hash,
            data: ".".to_string(),
            dtype: MerkleTreeNodeType::Dir,
            children: HashSet::new(),
        };
        CommitMerkleTree::read_children_from_node(repo, commit, &mut root)?;

        Ok(root)
    }

    fn read_children_from_node(
        repo: &LocalRepository,
        commit: &Commit,
        node: &mut CommitMerkleTreeNode,
    ) -> Result<(), OxenError> {
        let tree_db_dir = CommitMerkleTree::tree_db_dir(repo, &node);
        if !tree_db_dir.exists() {
            log::error!("Could not open {:?}", tree_db_dir);
            return Ok(());
        }

        if node.dtype != MerkleTreeNodeType::Dir &&
           node.dtype != MerkleTreeNodeType::VNode {
            return Ok(());
        }

        let tree_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&db::opts::default(), &tree_db_dir, false)?;
        let iter = tree_db.iterator(IteratorMode::Start);
    
        for item in iter {
            if let Ok((key, val)) = item {
                let key = str::from_utf8(&key)?;
                let val: MerkleNode = rmp_serde::from_slice(&val).unwrap();

                match &val.dtype {
                    MerkleNodeType::Dir => {
                        let mut child = CommitMerkleTreeNode {
                            path: PathBuf::from(&node.path).join(val.path.clone()),
                            hash: key.to_owned(),
                            dtype: MerkleTreeNodeType::Dir,
                            data: val.path,
                            children: HashSet::new(),
                        };
                        CommitMerkleTree::read_children_from_node(repo, commit, &mut child)?;
                        node.children.insert(child);
                    }
                    MerkleNodeType::VNode => {
                        let mut child = CommitMerkleTreeNode {
                            path: PathBuf::from(&node.path).join(val.path.clone()),
                            hash: key.to_owned(),
                            dtype: MerkleTreeNodeType::VNode,
                            data: val.path,
                            children: HashSet::new(),
                        };
                        CommitMerkleTree::read_children_from_node(repo, commit, &mut child)?;
                        node.children.insert(child);
                    }
                    MerkleNodeType::File => {
                        let child = CommitMerkleTreeNode {
                            path: PathBuf::from(&node.path).join(val.path.clone()),
                            hash: key.to_owned(),
                            dtype: MerkleTreeNodeType::File,
                            data: val.path,
                            children: HashSet::new(),
                        };
                        node.children.insert(child);
                    }
                    MerkleNodeType::Schema => {
                        let child = CommitMerkleTreeNode {
                            path: PathBuf::from(&node.path).join(val.path.clone()),
                            hash: key.to_owned(),
                            dtype: MerkleTreeNodeType::Schema,
                            data: val.path,
                            children: HashSet::new(),
                        };
                        node.children.insert(child);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn print_depth(&self, depth: i32) {
        self.r_print(&self.root, 0, depth);
    }

    pub fn print(&self) {
        // print all the way down
        self.r_print(&self.root, 0, -1);
    }

    fn r_print(&self, node: &CommitMerkleTreeNode, indent: i32, depth: i32) {
        if depth != -1 && depth > 0 && indent >= depth {
            return;
        }

        if MerkleTreeNodeType::VNode == node.dtype {
            println!("{}[{:?}] {} -> {} ({})", "  ".repeat(indent as usize), node.dtype, node.data, node.hash, node.children.len());
        } else {
            println!("{}[{:?}] {} -> {}", "  ".repeat(indent as usize), node.dtype, node.path.to_string_lossy(), node.hash);
        }
        for child in &node.children {
            self.r_print(child, indent + 1, depth);
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

        let tree = CommitMerkleTree::new(&repo, &commit)?;

        assert_eq!(tree.root.hash, "64f2e2e90a49d4fe9f52b95a053ad3fe");
        assert_eq!(tree.root.children.len(), 1);

        // Make sure "images" and "train" are in the root children
        assert!(tree
            .root
            .children
            .iter()
            .any(|x| x.path == PathBuf::from("images")));

        // Get the "images" child
        let images = tree.root.get_by_path(PathBuf::from("images"));
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
