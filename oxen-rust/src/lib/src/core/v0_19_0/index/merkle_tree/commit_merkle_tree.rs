use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rocksdb::{DBWithThreadMode, MultiThreaded};

use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::db::key_val::str_val_db;
use crate::core::db::merkle::merkle_node_db::MerkleNodeDB;

use crate::core::v0_10_0::index::CommitReader;
use crate::core::v0_19_0::index::merkle_tree::node::{MerkleTreeNodeData, MerkleTreeNodeType};
use crate::error::OxenError;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::LocalRepository;
use crate::model::{Commit, EntryDataType, MetadataEntry};
use crate::util;

pub struct CommitMerkleTree {}

impl CommitMerkleTree {
    // Commit db is the directories per commit
    // This helps us skip to a directory in the tree
    // .oxen/history/{COMMIT_ID}/dir_hashes
    fn commit_db_dir(repo: &LocalRepository, commit: &Commit) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(&commit.id)
            .join(DIR_HASHES_DIR)
    }

    pub fn read(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<MerkleTreeNodeData, OxenError> {
        let root_path = Path::new("");
        CommitMerkleTree::read_path(repo, commit, root_path)
    }

    pub fn read_root(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<MerkleTreeNodeData, OxenError> {
        let node_hash = u128::from_str_radix(&commit.id, 16).unwrap();
        CommitMerkleTree::read_node(repo, node_hash, true)
    }

    pub fn read_path(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<MerkleTreeNodeData, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let node_db_dir = CommitMerkleTree::commit_db_dir(repo, commit);
        let opts = db::key_val::opts::default();
        let node_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, node_db_dir, false)?;
        let mut node_path_str = node_path.to_str().unwrap();

        // If it ends with a /, remove it
        if node_path_str.ends_with('/') {
            node_path_str = &node_path_str[..node_path_str.len() - 1];
        }

        let node_hash: Option<String> = str_val_db::get(&node_db, node_path_str)?;
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            let hash = u128::from_str_radix(&node_hash, 16).unwrap();
            CommitMerkleTree::read_node(repo, hash, true)
        } else {
            // We are skipping to a file in the tree using the dir_hashes db
            log::debug!("Look up file 📄 {:?}", node_path);
            CommitMerkleTree::read_file(repo, &node_db, node_path)
        }
    }

    // TODO: Read in the merkle tree db first, to get the name and dtype from the index file
    pub fn read_node(
        repo: &LocalRepository,
        hash: u128,
        recurse: bool,
    ) -> Result<MerkleTreeNodeData, OxenError> {
        log::debug!("Read node root hash [{:x}]", hash);
        let mut node = MerkleTreeNodeData::root_commit(repo, hash)?;
        let mut node_db = MerkleNodeDB::open_read_only(repo, hash)?;

        CommitMerkleTree::read_children_from_node(repo, &mut node_db, &mut node, recurse)?;
        Ok(node)
    }

    /// This uses the dir_hashes db to skip right to a file in the tree
    fn read_file(
        repo: &LocalRepository,
        node_db: &DBWithThreadMode<MultiThreaded>,
        path: impl AsRef<Path>,
    ) -> Result<MerkleTreeNodeData, OxenError> {
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

        let node_hash = u128::from_str_radix(&node_hash, 16).unwrap();
        let vnodes = CommitMerkleTree::read_node(repo, node_hash, false)?;
        log::debug!("read_file got {} vnodes children", vnodes.children.len());
        for node in vnodes.children.into_iter() {
            let file_path_hash = util::hasher::hash_path_name(path);
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
            let hash_int = node.hash;
            let bucket = hash_int % num_vnodes;

            log::warn!("Make sure we calc correct bucket: {}", bucket);

            // Check if we are in the correct bucket
            if bucket == vnode.id {
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
        node: &mut MerkleTreeNodeData,
        recurse: bool,
    ) -> Result<(), OxenError> {
        let dtype = node.dtype;
        log::debug!("read_children_from_node tree_db_dir: {:?} dtype {:?}", node_db.path(), dtype);

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        let children: HashMap<u128, MerkleTreeNodeData> = node_db.map()?;
        log::debug!("read_children_from_node Got {} children", children.len());

        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!("read_children_from_node child: {:?} -> {}", key, child);
            match &child.dtype {
                // Directories, VNodes, and Files have children
                MerkleTreeNodeType::Commit | MerkleTreeNodeType::Dir | MerkleTreeNodeType::VNode => {
                    if recurse {
                        let mut node_db = MerkleNodeDB::open_read_only(repo, child.hash)?;
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

    pub fn dir(
        repo: &LocalRepository,
        node: &MerkleTreeNodeData,
        search_directory: impl AsRef<Path>,
    ) -> Result<Option<MetadataEntry>, OxenError> {
        let commit_reader = CommitReader::new(repo)?;

        let mut entry: Option<MetadataEntry> = None;
        let search_directory = search_directory.as_ref();
        let current_directory = PathBuf::from("");
        CommitMerkleTree::p_dir(
            &commit_reader,
            node,
            search_directory,
            current_directory,
            &mut entry,
        )?;
        Ok(entry)
    }

    fn p_dir(
        commit_reader: &CommitReader,
        node: &MerkleTreeNodeData,
        search_directory: impl AsRef<Path>,
        current_directory: impl AsRef<Path>,
        entry: &mut Option<MetadataEntry>,
    ) -> Result<(), OxenError> {
        let search_directory = search_directory.as_ref();
        let current_directory = current_directory.as_ref();
        for child in &node.children {
            match &child.dtype {
                MerkleTreeNodeType::Commit | MerkleTreeNodeType::VNode => {
                    CommitMerkleTree::p_dir(
                        commit_reader,
                        child,
                        search_directory,
                        current_directory,
                        entry,
                    )?;
                }
                MerkleTreeNodeType::Dir => {
                    let child_dir = child.dir().unwrap();
                    let current_directory = current_directory.join(&child_dir.name);
                    if current_directory == search_directory {
                        let commit_id = format!("{:x}", &child_dir.last_commit_id);
                        let commit = commit_reader.get_commit_by_id(&commit_id)?;
                        let metadata = MetadataEntry {
                            filename: child_dir.name.clone(),
                            is_dir: true,
                            latest_commit: commit,
                            resource: None,
                            size: child_dir.num_bytes,
                            data_type: EntryDataType::Dir,
                            mime_type: "".to_string(),
                            extension: "".to_string(),
                            metadata: None,
                            is_queryable: None,
                        };
                        *entry = Some(metadata);
                    }
                    CommitMerkleTree::p_dir(
                        commit_reader,
                        child,
                        search_directory,
                        current_directory,
                        entry,
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn dir_entries(
        repo: &LocalRepository,
        node: &MerkleTreeNodeData,
        search_directory: impl AsRef<Path>,
    ) -> Result<Vec<MetadataEntry>, OxenError> {
        let commit_reader = CommitReader::new(repo)?;
        let mut entries: Vec<MetadataEntry> = Vec::new();
        let current_directory = PathBuf::from("");
        CommitMerkleTree::p_dir_entries(
            &commit_reader,
            node,
            search_directory,
            current_directory,
            &mut entries,
        )?;
        Ok(entries)
    }

    fn p_dir_entries(
        commit_reader: &CommitReader,
        node: &MerkleTreeNodeData,
        search_directory: impl AsRef<Path>,
        current_directory: impl AsRef<Path>,
        entries: &mut Vec<MetadataEntry>,
    ) -> Result<(), OxenError> {
        let search_directory = search_directory.as_ref();
        let current_directory = current_directory.as_ref();
        for child in &node.children {
            match &child.dtype {
                MerkleTreeNodeType::Commit | MerkleTreeNodeType::VNode => {
                    CommitMerkleTree::p_dir_entries(
                        commit_reader,
                        child,
                        search_directory,
                        current_directory,
                        entries,
                    )?;
                }
                MerkleTreeNodeType::Dir => {
                    let child_dir = child.dir().unwrap();
                    if current_directory == search_directory && !child_dir.name.is_empty() {
                        let commit_id = format!("{:x}", &child_dir.last_commit_id);
                        let commit = commit_reader.get_commit_by_id(&commit_id)?;
                        let data_types = child_dir.data_types();
                        let metadata = MetadataEntry {
                            filename: child_dir.name.clone(),
                            is_dir: true,
                            latest_commit: commit,
                            resource: None,
                            size: child_dir.num_bytes,
                            data_type: EntryDataType::Dir,
                            mime_type: "inode/directory".to_string(),
                            extension: "".to_string(),
                            metadata: Some(GenericMetadata::MetadataDir(MetadataDir::new(
                                data_types,
                            ))),
                            is_queryable: None,
                        };
                        entries.push(metadata);
                    }
                    let current_directory = current_directory.join(&child_dir.name);
                    CommitMerkleTree::p_dir_entries(
                        commit_reader,
                        child,
                        search_directory,
                        current_directory,
                        entries,
                    )?;
                }
                MerkleTreeNodeType::File => {
                    let child_file = child.file().unwrap();
                    if current_directory == search_directory {
                        let commit_id = format!("{:x}", child_file.last_commit_id);
                        let commit = commit_reader.get_commit_by_id(&commit_id)?;

                        let metadata = MetadataEntry {
                            filename: child_file.name.clone(),
                            is_dir: false,
                            latest_commit: commit,
                            resource: None,
                            size: child_file.num_bytes,
                            data_type: child_file.data_type,
                            mime_type: child_file.mime_type,
                            extension: child_file.extension,
                            metadata: None,
                            is_queryable: None,
                        };
                        entries.push(metadata);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn print_depth(node: &MerkleTreeNodeData, depth: i32) {
        CommitMerkleTree::r_print(node, 0, depth);
    }

    pub fn print(node: &MerkleTreeNodeData) {
        // print all the way down
        CommitMerkleTree::r_print(node, 0, -1);
    }

    fn r_print(node: &MerkleTreeNodeData, indent: i32, depth: i32) {
        // log::debug!("r_print depth {:?} indent {:?}", depth, indent);
        // log::debug!(
        //     "r_print node dtype {:?} hash {} data.len() {} children.len() {}",
        //     node.dtype,
        //     node.hash,
        //     node.data.len(),
        //     node.children.len()
        // );
        if depth != -1 && depth > 0 && indent >= depth {
            return;
        }

        match node.dtype {
            MerkleTreeNodeType::Commit => {
                let commit = node.commit().unwrap();
                let parent_ids = commit.parent_ids.iter().map(|x| format!("{:x}", x)).collect::<Vec<String>>().join(",");
                println!("[Commit] {:x} -> {} parent_ids {:?}", commit.id, commit.message, parent_ids);
            }
            MerkleTreeNodeType::VNode => {
                let vnode = node.vnode().unwrap();
                println!(
                    "{}[{:?}] {:x} -> {:x} ({})",
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
                    "{}[{:?}] {} -> {:x} {} ({} nodes) ({} files) [{:x}]",
                    "  ".repeat(indent as usize),
                    node.dtype,
                    dir.name,
                    node.hash,
                    bytesize::ByteSize::b(dir.num_bytes),
                    node.children.len(),
                    dir.num_files(),
                    dir.last_commit_id
                )
            }
            MerkleTreeNodeType::File => {
                let file = node.file().unwrap();
                println!(
                    "{}[{:?}] {} -> {:x} {} [{:x}]",
                    "  ".repeat(indent as usize),
                    node.dtype,
                    file.name,
                    node.hash,
                    bytesize::ByteSize::b(file.num_bytes),
                    file.last_commit_id
                )
            }
            MerkleTreeNodeType::Schema => {
                let schema = node.schema().unwrap();
                println!(
                    "{}[{:?}] {} -> {:x} ({})",
                    "  ".repeat(indent as usize),
                    node.dtype,
                    schema.name,
                    node.hash,
                    node.children.len()
                )
            }
            MerkleTreeNodeType::FileChunk => {
                let _chunk = node.file_chunk().unwrap();
                println!(
                    "{} {:?} -> {:x} ({})",
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
