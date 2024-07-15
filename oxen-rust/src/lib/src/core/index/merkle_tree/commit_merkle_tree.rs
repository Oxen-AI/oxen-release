use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rocksdb::{DBWithThreadMode, MultiThreaded};

use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::constants::{NODES_DIR, TREE_DIR};
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
    // .oxen/tree/nodes/{hash}
    fn node_db_dir(repo: &LocalRepository, hash: impl AsRef<str>) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(TREE_DIR)
            .join(NODES_DIR)
            .join(hash.as_ref())
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
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
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
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            CommitMerkleTree::read_node(repo, node_hash, true)
        } else {
            // We are skipping to a file in the tree using the dir_hashes db
            log::debug!("Look up file 📄 {:?}", node_path);
            CommitMerkleTree::read_file(repo, &node_db, node_path)
        }
    }

    // TODO: Read in the merkle tree db first, to get the name and dtype from the index file
    pub fn read_node(
        repo: &LocalRepository,
        node_hash: String,
        recurse: bool,
    ) -> Result<CommitMerkleTreeNode, OxenError> {
        // Dir hashes are stored with extra quotes in the db, remove them
        let node_hash = node_hash.replace('"', "");
        let mut node = CommitMerkleTreeNode::root(&node_hash);
        let mut node_db = CommitMerkleTree::open_node_db(repo, node_hash)?;
        CommitMerkleTree::read_children_from_node(repo, &mut node_db, &mut node, recurse)?;
        Ok(node)
    }

    fn open_node_db(
        repo: &LocalRepository,
        node_hash: impl AsRef<str>,
    ) -> Result<MerkleNodeDB, OxenError> {
        let node_db_dir = CommitMerkleTree::node_db_dir(repo, node_hash);
        if !node_db_dir.exists() {
            log::error!("Could not open {:?}", node_db_dir);
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for path: '{:?}'",
                node_db_dir
            )));
        }
        MerkleNodeDB::open_read_only(&node_db_dir)
    }

    /// This uses the dir_hashes db to skip right to a file in the tree
    fn read_file(
        repo: &LocalRepository,
        node_db: &DBWithThreadMode<MultiThreaded>,
        path: impl AsRef<Path>,
    ) -> Result<CommitMerkleTreeNode, OxenError> {
        // Get the directory from the path
        let path = path.as_ref();
        let parent_path = path.parent().unwrap();
        let file_name = path.file_name().unwrap().to_str().unwrap();

        // TODO: This is ugly, abstract lookup of initial dir out
        let parent_path_str = parent_path.to_str().unwrap();

        log::debug!(
            "read_file path {:?} parent_path {:?} file_name {:?}",
            path,
            parent_path,
            file_name
        );

        // Look up the directory hash
        let node_hash: Option<String> = str_val_db::get(node_db, parent_path_str)?;
        let Some(node_hash) = node_hash else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for parent: '{}'",
                parent_path_str
            )));
        };

        let vnodes = CommitMerkleTree::read_node(repo, node_hash, false)?;
        log::debug!("read_file got {} vnodes children", vnodes.children.len());
        for node in vnodes.children.into_iter() {
            let file_path_hash = util::hasher::hash_path(path);
            log::debug!("Node Hash: {:?} -> {}", path, file_path_hash);
            log::debug!("Is in VNode? {:?}", node.dtype);

            // TODO: More robust type matching
            let vnode = node.vnode()?;
            let children = &node.children;
            log::debug!("Num VNode children {:?}", children.len());

            // Find the bucket based on number of children
            let total_children = children.len();
            let num_vnodes = (total_children as f32 / 10000_f32).log2();
            let num_vnodes = 2u128.pow(num_vnodes.ceil() as u32);
            let hash_int = u128::from_str_radix(&node.hash, 16).unwrap();
            let bucket = hash_int % num_vnodes;

            log::warn!("Make sure we calc correct bucket: {}", bucket);

            // Check if we are in the correct bucket
            // Downcast is safe from u128 to u32
            if bucket as u32 == vnode.id {
                log::debug!("Found file in VNode! {:?}", vnode);
                let children = CommitMerkleTree::read_node(repo, node.hash, false)?;
                log::debug!("Num children {:?}", children.children.len());

                for child in children.children.into_iter() {
                    log::debug!("Got child {:?}", child.dtype);
                    if child.dtype == MerkleTreeNodeType::File {
                        let file = child.file()?;
                        log::debug!("Got file {:?}", file.name);
                        if file.name == file_name {
                            return Ok(child);
                        }
                    }
                }
            }
        }

        Err(OxenError::basic_str(format!(
            "Merkle tree vnode not found for path: `{}`",
            parent_path_str
        )))
    }

    fn read_children_from_node(
        repo: &LocalRepository,
        node_db: &mut MerkleNodeDB,
        node: &mut CommitMerkleTreeNode,
        recurse: bool,
    ) -> Result<(), OxenError> {
        log::debug!("read_children_from_node tree_db_dir: {:?}", node_db.path());
        let dtype = node_db.dtype();

        if dtype != MerkleTreeNodeType::Dir && dtype != MerkleTreeNodeType::VNode {
            return Ok(());
        }

        let children: HashMap<u128, CommitMerkleTreeNode> = node_db.map()?;
        log::debug!("read_children_from_node Got {} children", children.len());

        for (key, child) in children {
            let mut child = child.to_owned();
            log::debug!("read_children_from_node child: {:?} -> {}", key, child);
            match &child.dtype {
                // Directories, VNodes, and Files have children
                MerkleTreeNodeType::Dir | MerkleTreeNodeType::VNode => {
                    if recurse {
                        let mut node_db = CommitMerkleTree::open_node_db(repo, &child.hash)?;
                        CommitMerkleTree::read_children_from_node(
                            repo,
                            &mut node_db,
                            &mut child,
                            recurse,
                        )?;
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
                    vnode.id,
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
                    "{}[{:?}] {:?} -> {} ({} chunks) {}",
                    "  ".repeat(indent as usize),
                    node.dtype,
                    file.name,
                    node.hash,
                    file.chunk_hashes.len(),
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
