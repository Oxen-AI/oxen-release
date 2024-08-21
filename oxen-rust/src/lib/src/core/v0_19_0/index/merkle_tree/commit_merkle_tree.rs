use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};

use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::v0_19_0::index::merkle_tree::MerkleNodeDB;

use crate::core::v0_10_0::index::CommitReader;
use crate::core::v0_19_0::index::merkle_tree::node::MerkleTreeNodeData;
use crate::error::OxenError;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::LocalRepository;
use crate::model::{Commit, EntryDataType, MetadataEntry};
use crate::model::{MerkleHash, MerkleTreeNodeType};
use crate::util;

use super::node::DirNode;

pub struct CommitMerkleTree {
    pub root: MerkleTreeNodeData,
    pub dir_hashes: HashMap<String, MerkleHash>,
}

impl CommitMerkleTree {
    // Commit db is the directories per commit
    // This helps us skip to a directory in the tree
    // .oxen/history/{COMMIT_ID}/dir_hashes
    fn dir_hash_db_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(&commit.id)
            .join(DIR_HASHES_DIR)
    }

    pub fn dir_hash_db_path_from_commit_id(
        repo: &LocalRepository,
        commit_id: MerkleHash,
    ) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(&commit_id.to_string())
            .join(DIR_HASHES_DIR)
    }

    pub fn from_commit(repo: &LocalRepository, commit: &Commit) -> Result<Self, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        let root = CommitMerkleTree::read_node(repo, &node_hash, true)?;
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        Ok(Self { root, dir_hashes })
    }

    pub fn from_path(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<Self, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path.to_str().unwrap()).cloned();
        let root = if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            CommitMerkleTree::read_node(repo, &node_hash, true)?
        } else {
            // We are skipping to a file in the tree using the dir_hashes db
            log::debug!("Look up file 📄 {:?}", node_path);
            CommitMerkleTree::read_file(repo, &dir_hashes, node_path)?
        };
        Ok(Self { root, dir_hashes })
    }

    /// Read the dir metadata from the path, without reading the children
    pub fn dir_metadata_from_path(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<DirNode, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path.to_str().unwrap()).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            CommitMerkleTree::read_node(repo, &node_hash, false)?.dir()
        } else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for parent: '{}'",
                node_path.to_str().unwrap()
            )));
        }
    }

    pub fn dir_from_path_with_children(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<MerkleTreeNodeData, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path.to_str().unwrap()).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            // Read the node at depth 2 to get VNodes and Sub-Files/Dirs
            CommitMerkleTree::read_depth(repo, node_hash, 2)
        } else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for parent: '{}'",
                node_path.to_str().unwrap()
            )));
        }
    }

    pub fn read_node(
        repo: &LocalRepository,
        hash: &MerkleHash,
        recurse: bool,
    ) -> Result<MerkleTreeNodeData, OxenError> {
        log::debug!("Read node hash [{}]", hash);
        let mut node = MerkleTreeNodeData::from_hash(repo, hash)?;
        let mut node_db = MerkleNodeDB::open_read_only(repo, hash)?;

        CommitMerkleTree::read_children_from_node(repo, &mut node_db, &mut node, recurse)?;
        Ok(node)
    }

    pub fn read_depth(
        repo: &LocalRepository,
        hash: MerkleHash,
        depth: i32,
    ) -> Result<MerkleTreeNodeData, OxenError> {
        log::debug!("Read node hash [{}]", hash);
        let mut node = MerkleTreeNodeData::from_hash(repo, &hash)?;
        let mut node_db = MerkleNodeDB::open_read_only(repo, &hash)?;

        CommitMerkleTree::read_children_until_depth(repo, &mut node_db, &mut node, depth)?;
        Ok(node)
    }

    /// The dir hashes allow you to skip to a directory in the tree
    pub fn dir_hashes(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<HashMap<String, MerkleHash>, OxenError> {
        let node_db_dir = CommitMerkleTree::dir_hash_db_path(repo, commit);
        let opts = db::key_val::opts::default();
        let node_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, node_db_dir, false)?;
        let mut dir_hashes = HashMap::new();
        let iterator = node_db.iterator(IteratorMode::Start);
        for item in iterator {
            match item {
                Ok((key, value)) => {
                    let key = str::from_utf8(&key)?;
                    let value = str::from_utf8(&value)?;
                    let hash = MerkleHash::from_str(&value)?;
                    dir_hashes.insert(key.to_string(), hash);
                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Could not read iterate over db values",
                    ));
                }
            }
        }
        Ok(dir_hashes)
    }

    pub fn load_nodes(
        repo: &LocalRepository,
        commit: &Commit,
        paths: &[PathBuf],
    ) -> Result<HashMap<PathBuf, MerkleTreeNodeData>, OxenError> {
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;

        let mut nodes = HashMap::new();
        for path in paths.into_iter() {
            // Skip to the nodes
            let Some(hash) = dir_hashes.get(path.to_str().unwrap()) else {
                return Err(OxenError::basic_str(format!(
                    "Dir hash not found for path: {:?}",
                    path
                )));
            };
            log::debug!("Loading node for path: {:?} hash: {}", path, hash);
            let node = CommitMerkleTree::read_depth(repo, *hash, 2)?;
            nodes.insert(path.clone(), node);
        }
        Ok(nodes)
    }

    pub fn has_path(&self, path: impl AsRef<Path>) -> Result<bool, OxenError> {
        let path = path.as_ref();
        let node = self.root.get_by_path(path)?;
        Ok(node.is_some())
    }

    pub fn get_by_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNodeData>, OxenError> {
        let path = path.as_ref();
        let node = self.root.get_by_path(path)?;
        Ok(node)
    }

    pub fn get_vnodes_for_dir(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<MerkleTreeNodeData>, OxenError> {
        let path = path.as_ref();
        let nodes = self.root.get_vnodes_for_dir(path)?;
        Ok(nodes)
    }

    pub fn list_dir_paths(&self) -> Result<Vec<PathBuf>, OxenError> {
        self.root.list_dir_paths()
    }

    pub fn dir_files_and_folders(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<MerkleTreeNodeData>, OxenError> {
        let path = path.as_ref();
        let node = self
            .root
            .get_by_path(path)?
            .ok_or(OxenError::basic_str(format!(
                "Merkle tree hash when looking up dir children: '{:?}'",
                path
            )))?;

        CommitMerkleTree::node_files_and_folders(&node)
    }

    pub fn node_files_and_folders(
        node: &MerkleTreeNodeData,
    ) -> Result<Vec<MerkleTreeNodeData>, OxenError> {
        if node.dtype != MerkleTreeNodeType::Dir {
            return Err(OxenError::basic_str(format!(
                "Merkle tree node is not a directory: '{:?}'",
                node.dtype
            )));
        }

        // The dir node will have vnode children
        let mut children = Vec::new();
        for child in &node.children {
            if child.dtype == MerkleTreeNodeType::VNode {
                children.extend(child.children.iter().map(|c| c.clone()));
            }
        }
        Ok(children)
    }

    pub fn total_vnodes(&self) -> usize {
        self.root.total_vnodes()
    }

    /// This uses the dir_hashes db to skip right to a file in the tree
    fn read_file(
        repo: &LocalRepository,
        dir_hashes: &HashMap<String, MerkleHash>,
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
        let node_hash: Option<MerkleHash> = dir_hashes.get(parent_path_str).cloned();
        let Some(node_hash) = node_hash else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for parent: '{}'",
                parent_path_str
            )));
        };

        let vnodes = CommitMerkleTree::read_node(repo, &node_hash, false)?;
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
            let num_vnodes = (total_children as f32 / repo.vnode_size() as f32).ceil() as u128;
            let hash_int = node.hash;
            let bucket = hash_int.to_u128() % num_vnodes;

            log::warn!("Make sure we calc correct bucket: {}", bucket);

            // Check if we are in the correct bucket
            if bucket == vnode.id.to_u128() {
                log::debug!("Found file in VNode! {:?}", vnode);
                let children = CommitMerkleTree::read_node(repo, &node.hash, false)?;
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

    fn read_children_until_depth(
        repo: &LocalRepository,
        node_db: &mut MerkleNodeDB,
        node: &mut MerkleTreeNodeData,
        depth: i32,
    ) -> Result<(), OxenError> {
        let dtype = node.dtype;
        log::debug!(
            "read_children_until_depth tree_db_dir: {:?} dtype {:?}",
            node_db.path(),
            dtype
        );

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        let children: Vec<(MerkleHash, MerkleTreeNodeData)> = node_db.map()?;
        log::debug!("read_children_until_depth Got {} children", children.len());

        for (key, child) in children {
            let mut child = child.to_owned();
            log::debug!("read_children_until_depth child: {} -> {}", key, child);
            match &child.dtype {
                // Directories, VNodes, and Files have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if depth > 0 {
                        let mut node_db = MerkleNodeDB::open_read_only(repo, &child.hash)?;
                        CommitMerkleTree::read_children_until_depth(
                            repo,
                            &mut node_db,
                            &mut child,
                            depth - 1,
                        )?;
                    }
                    node.children.push(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk
                | MerkleTreeNodeType::Schema
                | MerkleTreeNodeType::File => {
                    node.children.push(child);
                }
            }
        }

        Ok(())
    }

    fn read_children_from_node(
        repo: &LocalRepository,
        node_db: &mut MerkleNodeDB,
        node: &mut MerkleTreeNodeData,
        recurse: bool,
    ) -> Result<(), OxenError> {
        let dtype = node.dtype;
        log::debug!(
            "read_children_from_node tree_db_dir: {:?} dtype {:?}",
            node_db.path(),
            dtype
        );

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        let children: Vec<(MerkleHash, MerkleTreeNodeData)> = node_db.map()?;
        log::debug!("read_children_from_node Got {} children", children.len());

        for (key, child) in children {
            let mut child = child.to_owned();
            log::debug!("read_children_from_node child: {} -> {}", key, child);
            match &child.dtype {
                // Directories, VNodes, and Files have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if recurse {
                        let mut node_db = MerkleNodeDB::open_read_only(repo, &child.hash)?;
                        CommitMerkleTree::read_children_from_node(
                            repo,
                            &mut node_db,
                            &mut child,
                            recurse,
                        )?;
                    }
                    node.children.push(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk
                | MerkleTreeNodeType::Schema
                | MerkleTreeNodeType::File => {
                    node.children.push(child);
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
                        let commit_id = child_dir.last_commit_id.to_string();
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
                        let commit_id = child_dir.last_commit_id.to_string();
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
                        let commit_id = child_file.last_commit_id.to_string();
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

    pub fn print(&self) {
        CommitMerkleTree::print_node(&self.root);
    }

    pub fn print_depth(&self, depth: i32) {
        CommitMerkleTree::print_node_depth(&self.root, depth);
    }

    pub fn print_node_depth(node: &MerkleTreeNodeData, depth: i32) {
        CommitMerkleTree::r_print(node, 0, depth);
    }

    pub fn print_node(node: &MerkleTreeNodeData) {
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

        println!("{}{}", "  ".repeat(indent as usize), node);

        for child in &node.children {
            CommitMerkleTree::r_print(child, indent + 1, depth);
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::core::v0_19_0::index::merkle_tree::CommitMerkleTree;
    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;
    use crate::model::MerkleTreeNodeType;
    use crate::repositories;
    use crate::test;
    use crate::test::add_n_files_m_dirs;

    #[test]
    fn test_load_dir_nodes() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3)?;
            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = repositories::commits::commit(&repo, "First commit")?;

            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            /*
            The tree will look something like this

            [Commit] d9fc5c49451ad18335f9f8c1e1c8ac0b -> First commit parent_ids ""
                [Dir]  -> 172861146a4a0f5f0250f117ce93ef1e 60 B (1 nodes) (10 files)
                    [VNode] 3a5d6d3bdc8bf1f3fddcabaa3afcd821 (3 children)
                    [File] README.md -> beb36f69f0b6efd87dbe3bb3dcea661c 18 B
                    [Dir] files -> aefe7cf4ad104b759e46c13cb304ba16 60 B (1 nodes) (10 files)
                        [VNode] affcd15c283c42524ee3f2dc300b90fe (3 children)
                        [Dir] dir_0 -> ee97a66ee8498caa67605c50e9b24275 0 B (1 nodes) (0 files)
                            [VNode] 1756daa4caa26d51431b925250529838 (4 children)
                            [File] file0.txt -> 82d44cc82d2c1c957aeecb14293fb5ec 6 B
                            [File] file3.txt -> 9c8fe1177e78b0fe5ec104db52b5e449 6 B
                            [File] file6.txt -> 3cba14134797f8c246ee520c808817b4 6 B
                            [File] file9.txt -> ab8e4cdc8e9df49fb8d7bc1940df811f 6 B
                        [Dir] dir_1 -> 24467f616e4fba7beacb18b71b87652d 0 B (1 nodes) (0 files)
                            [VNode] 382eb89abe00193ed680c6a541f4b0c4 (3 children)
                            [File] file1.txt -> aab67365636cc292a767ad9e48ca6e1f 6 B
                            [File] file4.txt -> f8d4169182a41cc63bb7ed8fc36de960 6 B
                            [File] file7.txt -> b0335dcbf55c6c08471d8ebefbbf5de9 6 B
                        [Dir] dir_2 -> 7e2fbcd5b9e62847e1aaffd7e9d1aa8 0 B (1 nodes) (0 files)
                            [VNode] b87cfea40ada7cc374833ab2eca4636d (3 children)
                            [File] file2.txt -> 2101009797546bf98de2b0bbcbd59f0 6 B
                            [File] file5.txt -> 253badb52f99edddf74d1261b8c5f03a 6 B
                            [File] file8.txt -> 13fa116ba84c615eda1759b5e6ae5d6e 6 B
                    [File] files.csv -> 152b60b41558d5bfe80b7e451de7b276 151 B
            */

            // Make sure we have written the dir_hashes db
            let dir_hashes = CommitMerkleTree::dir_hashes(&repo, &commit)?;

            println!("Got {} dir_hashes", dir_hashes.len());
            for (key, value) in &dir_hashes {
                println!("dir: {:?} hash: {}", key, value);
            }

            // Should have ["", "files", "files/dir_0", "files/dir_1", "files/dir_2"]
            assert_eq!(dir_hashes.len(), 5);
            assert!(dir_hashes.contains_key(&"".to_string()));
            assert!(dir_hashes.contains_key(&"files".to_string()));
            assert!(dir_hashes.contains_key(&"files/dir_0".to_string()));
            assert!(dir_hashes.contains_key(&"files/dir_1".to_string()));
            assert!(dir_hashes.contains_key(&"files/dir_2".to_string()));

            // Only load the root and files/dir_1
            let paths_to_load: Vec<PathBuf> =
                vec![PathBuf::from(""), PathBuf::from("files").join("dir_1")];
            let loaded_nodes = CommitMerkleTree::load_nodes(&repo, &commit, &paths_to_load)?;

            println!("loaded {} nodes", loaded_nodes.len());
            for (_, node) in loaded_nodes {
                println!("node: {}", node);
                CommitMerkleTree::print_node_depth(&node, 1);
                assert!(node.dtype == MerkleTreeNodeType::Dir);
                assert!(node.parent_id.is_some());
                assert!(node.children.len() > 0);
                let dir = node.dir().unwrap();
                assert!(dir.num_files() > 0);
            }

            Ok(())
        })
    }
}
