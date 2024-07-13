use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rocksdb::{DBWithThreadMode, MultiThreaded};

use crate::constants::TREE_DIR;
use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db::merkle_node_db::MerkleNodeDB;
use crate::core::db::{self, str_val_db};

use crate::core::index::merkle_tree::node::{CommitMerkleTreeNode, MerkleTreeNodeType};
use crate::error::OxenError;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::util;

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
            // We are reading a node with children
            CommitMerkleTree::read_node(repo, node_hash, true)
        } else {
            // We are reading a file
            CommitMerkleTree::read_file(repo, &node_db, node_path)
        }
    }

    pub fn read_node(
        repo: &LocalRepository,
        node_hash: String,
        recurse: bool,
    ) -> Result<CommitMerkleTreeNode, OxenError> {
        // Dir hashes are stored with extra quotes in the db, remove them
        let node_hash = node_hash.replace('"', "");
        let mut node = CommitMerkleTreeNode::root(&node_hash);
        CommitMerkleTree::read_children_from_node(repo, &mut node, recurse)?;
        Ok(node)
    }

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

        let vnodes = CommitMerkleTree::read_node(repo, node_hash, false)?;
        for node in vnodes.children.into_iter() {
            let file_path_hash = util::hasher::hash_path(path);
            // println!("File: {:?} -> {}", path, file_path_hash);
            // println!("Is in VNode? {:?}", vnode);

            // TODO: More robust type matching
            let vnode = node.vnode()?;

            // Check if first two chars of hashes match
            if file_path_hash.get(0..2).unwrap() == vnode.path {
                // println!("Found file in VNode! {:?}", vnode);
                let children = CommitMerkleTree::read_node(repo, node.hash, false)?;
                for child in children.children.into_iter() {
                    // TODO: More robust type matching
                    let file = child.file()?;
                    if file.path == file_name {
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

        log::debug!("read_children_from_node tree_db_dir: {:?}", tree_db_dir);

        if node.dtype != MerkleTreeNodeType::Dir && node.dtype != MerkleTreeNodeType::VNode {
            return Ok(());
        }

        let mut tree_db = MerkleNodeDB::open(&tree_db_dir, true)?;
        let children: HashMap<u128, CommitMerkleTreeNode> = tree_db.map()?;
        log::debug!("read_children_from_node Got {} children", children.len());

        for (key, child) in children {
            let mut child = child.to_owned();
            log::debug!("read_children_from_node child: {:?} -> {:?}", key, child);
            match &child.dtype {
                // Directories, VNodes, and Files have children
                MerkleTreeNodeType::Dir | MerkleTreeNodeType::VNode => {
                    if recurse {
                        CommitMerkleTree::read_children_from_node(repo, &mut child, recurse)?;
                    }
                    node.children.insert(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk
                | MerkleTreeNodeType::Schema
                | MerkleTreeNodeType::File => {
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
        log::debug!("r_print depth {:?} indent {:?}", depth, indent);
        log::debug!(
            "r_print node dtype {:?} hash {} data.len() {} children.len() {}",
            node.dtype,
            node.hash,
            node.data.len(),
            node.children.len()
        );
        if depth != -1 && depth > 0 && indent >= depth {
            return;
        }

        match node.dtype {
            MerkleTreeNodeType::VNode => {
                let vnode = node.vnode().unwrap();
                println!(
                    "{}[{:?}] {:?} -> {} ({})",
                    "  ".repeat(indent as usize),
                    node.dtype,
                    vnode.path,
                    node.hash,
                    node.children.len()
                )
            }
            MerkleTreeNodeType::Dir => {
                let dir = node.dir().unwrap();
                println!(
                    "{}[{:?}] {:?} -> {} ({})",
                    "  ".repeat(indent as usize),
                    node.dtype,
                    dir.path,
                    node.hash,
                    node.children.len()
                )
            }
            MerkleTreeNodeType::File => {
                let file = node.file().unwrap();
                println!(
                    "{}[{:?}] {:?} -> {} ({}) {}",
                    "  ".repeat(indent as usize),
                    node.dtype,
                    file.path,
                    node.hash,
                    node.children.len(),
                    bytesize::ByteSize::b(file.num_bytes)
                )
            }
            MerkleTreeNodeType::Schema => {
                let schema = node.schema().unwrap();
                println!(
                    "{}[{:?}] {:?} -> {} ({})",
                    "  ".repeat(indent as usize),
                    node.dtype,
                    schema.path,
                    node.hash,
                    node.children.len()
                )
            }
            MerkleTreeNodeType::FileChunk => {
                let _chunk = node.file_chunk().unwrap();
                println!(
                    "{} {:?} -> {} ({})",
                    "  ".repeat(indent as usize),
                    node.dtype,
                    node.hash,
                    node.children.len()
                )
            }
        }

        for child in &node.children {
            CommitMerkleTree::r_print(child, indent + 1, depth);
        }
    }
}

#[cfg(test)]
mod tests {
    // use time::OffsetDateTime;

    use super::*;

    #[test]
    fn test_read_commit_merkle_tree() -> Result<(), OxenError> {
        // let repo_path = Path::new("data")
        //     .join("test")
        //     .join("commit_dbs")
        //     .join("repo");
        // let repo = LocalRepository::new(&repo_path)?;
        // let commit = Commit {
        //     id: String::from("64f2e2e90a49d4fe9f52b95a053ad3fe"),
        //     parent_ids: vec![],
        //     message: String::from("initial commit"),
        //     author: String::from("Ox"),
        //     email: String::from("ox@oxen.ai"),
        //     timestamp: OffsetDateTime::now_utc(),
        //     root_hash: None,
        // };

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
        todo!();

        /*
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
        */

        // Ok(())
    }
}
