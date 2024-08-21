use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::Path;

use std::path::PathBuf;

use super::*;
use crate::core::v0_19_0::index::merkle_tree::node::MerkleNodeDB;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash, MerkleTreeNode, MerkleTreeNodeType};

#[derive(Clone, Eq)]
pub struct MerkleTreeNodeData {
    pub hash: MerkleHash,
    pub dtype: MerkleTreeNodeType,
    pub data: Vec<u8>,
    pub parent_id: Option<MerkleHash>,
    pub children: Vec<MerkleTreeNodeData>,
}

impl MerkleTreeNodeData {
    /// Create an empty root node with a hash
    pub fn from_hash(repo: &LocalRepository, hash: &MerkleHash) -> Result<Self, OxenError> {
        let node_db = MerkleNodeDB::open_read_only(repo, hash)?;
        let dtype = node_db.dtype;
        let parent_id = node_db.parent_id;
        Ok(MerkleTreeNodeData {
            hash: hash.clone(),
            dtype,
            data: node_db.data(),
            parent_id,
            children: Vec::new(),
        })
    }

    /// Check if the node is a leaf node (i.e. it has no children)
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    /// Check if the node has children
    pub fn has_children(&self) -> bool {
        !self.children.is_empty()
    }

    /// Recursively count the total number of vnodes in the tree
    pub fn total_vnodes(&self) -> usize {
        let mut count = 0;
        for child in &self.children {
            if child.dtype == MerkleTreeNodeType::VNode {
                count += 1;
            }
            count += child.total_vnodes();
        }
        count
    }

    /// Count the number of vnodes a dir has
    pub fn num_vnodes(&self) -> u128 {
        let mut count = 0;
        for child in &self.children {
            if child.dtype == MerkleTreeNodeType::VNode {
                count += 1;
            }
        }
        count
    }

    pub fn list_dir_paths(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut dirs = Vec::new();
        let current_path = Path::new("");
        self.list_dir_paths_helper(&current_path, &mut dirs)?;
        Ok(dirs)
    }

    fn list_dir_paths_helper(
        &self,
        current_path: &Path,
        dirs: &mut Vec<PathBuf>,
    ) -> Result<(), OxenError> {
        if self.dtype == MerkleTreeNodeType::Dir {
            dirs.push(current_path.to_path_buf());
        }
        for child in &self.children {
            if child.dtype == MerkleTreeNodeType::Dir {
                let dir_node = child.dir()?;
                let new_path = current_path.join(dir_node.name);
                child.list_dir_paths_helper(&new_path, dirs)?;
            } else {
                child.list_dir_paths_helper(current_path, dirs)?;
            }
        }
        Ok(())
    }

    /// Get all files and dirs in a directory
    pub fn get_all_children(&self) -> Result<Vec<MerkleTreeNodeData>, OxenError> {
        let mut children = Vec::new();
        for child in &self.children {
            children.push(child.clone());
            if child.dtype == MerkleTreeNodeType::Dir {
                children.extend(child.get_all_children()?);
            }
        }
        Ok(children)
    }

    /// Get all the vnodes for a given directory
    pub fn get_vnodes_for_dir(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<MerkleTreeNodeData>, OxenError> {
        let path = path.as_ref();
        let Some(node) = self.get_by_path(path)? else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree directory not found: '{:?}'",
                path
            )));
        };

        if node.dtype != MerkleTreeNodeType::Dir {
            return Err(OxenError::basic_str(format!(
                "Merkle tree node is not a directory: '{:?}'",
                path
            )));
        }

        let mut vnodes = Vec::new();
        for child in &node.children {
            if child.dtype == MerkleTreeNodeType::VNode {
                vnodes.push(child.clone());
            }
        }
        Ok(vnodes)
    }

    /// Search for a file node by path
    pub fn get_by_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNodeData>, OxenError> {
        let path = path.as_ref();
        let traversed_path = Path::new("");
        self.get_by_path_helper(traversed_path, path)
    }

    fn get_by_path_helper(
        &self,
        traversed_path: &Path,
        path: &Path,
    ) -> Result<Option<MerkleTreeNodeData>, OxenError> {
        if traversed_path.components().count() > path.components().count() {
            return Ok(None);
        }

        if self.dtype == MerkleTreeNodeType::File {
            let file_node = self.file()?;
            let file_path = traversed_path.join(file_node.name);
            // log::debug!(
            //     "get_by_path_helper [{:?}] {:?} {:?}",
            //     self.dtype,
            //     file_path,
            //     path
            // );
            if file_path == path {
                return Ok(Some(self.clone()));
            }
        }

        if self.dtype == MerkleTreeNodeType::Dir {
            // log::debug!(
            //     "get_by_path_helper [{:?}] {:?} {:?}",
            //     self.dtype,
            //     traversed_path,
            //     path
            // );
            if traversed_path == path {
                return Ok(Some(self.clone()));
            }
        }

        if self.dtype == MerkleTreeNodeType::Commit
            || self.dtype == MerkleTreeNodeType::Dir
            || self.dtype == MerkleTreeNodeType::VNode
        {
            for child in &self.children {
                if child.dtype == MerkleTreeNodeType::Dir {
                    let dir_node = child.dir()?;
                    if let Some(node) =
                        child.get_by_path_helper(&traversed_path.join(dir_node.name), path)?
                    {
                        return Ok(Some(node));
                    }
                } else {
                    if let Some(node) = child.get_by_path_helper(&traversed_path, path)? {
                        return Ok(Some(node));
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn to_node(&self) -> Result<MerkleTreeNode, OxenError> {
        match self.dtype {
            MerkleTreeNodeType::Commit => Ok(MerkleTreeNode::Commit(self.commit()?)),
            MerkleTreeNodeType::VNode => Ok(MerkleTreeNode::VNode(self.vnode()?)),
            MerkleTreeNodeType::Dir => Ok(MerkleTreeNode::Directory(self.dir()?)),
            MerkleTreeNodeType::File => Ok(MerkleTreeNode::File(self.file()?)),
            MerkleTreeNodeType::FileChunk => Ok(MerkleTreeNode::FileChunk(self.file_chunk()?)),
            MerkleTreeNodeType::Schema => Ok(MerkleTreeNode::Schema(self.schema()?)),
        }
    }

    pub fn deserialize_id(data: &[u8], dtype: MerkleTreeNodeType) -> Result<MerkleHash, OxenError> {
        match dtype {
            MerkleTreeNodeType::Commit => Self::deserialize_commit(data).map(|commit| commit.id),
            MerkleTreeNodeType::VNode => Self::deserialize_vnode(data).map(|vnode| vnode.id),
            MerkleTreeNodeType::Dir => Self::deserialize_dir(data).map(|dir| dir.hash),
            MerkleTreeNodeType::File => Self::deserialize_file(data).map(|file| file.hash),
            MerkleTreeNodeType::FileChunk => {
                Self::deserialize_file_chunk(data).map(|file_chunk| file_chunk.id)
            }
            MerkleTreeNodeType::Schema => Self::deserialize_schema(data).map(|schema| schema.hash),
        }
    }

    pub fn commit(&self) -> Result<CommitNode, OxenError> {
        Self::deserialize_commit(&self.data)
    }

    fn deserialize_commit(data: &[u8]) -> Result<CommitNode, OxenError> {
        rmp_serde::from_slice(data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing commit: {e}")))
    }

    pub fn vnode(&self) -> Result<VNode, OxenError> {
        Self::deserialize_vnode(&self.data)
    }

    fn deserialize_vnode(data: &[u8]) -> Result<VNode, OxenError> {
        rmp_serde::from_slice(data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing vnode: {e}")))
    }

    pub fn dir(&self) -> Result<DirNode, OxenError> {
        Self::deserialize_dir(&self.data)
    }

    fn deserialize_dir(data: &[u8]) -> Result<DirNode, OxenError> {
        rmp_serde::from_slice(data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing dir node: {e}")))
    }

    pub fn file(&self) -> Result<FileNode, OxenError> {
        Self::deserialize_file(&self.data)
    }

    fn deserialize_file(data: &[u8]) -> Result<FileNode, OxenError> {
        rmp_serde::from_slice(data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing file node: {e}")))
    }

    pub fn file_chunk(&self) -> Result<FileChunkNode, OxenError> {
        Self::deserialize_file_chunk(&self.data)
    }

    fn deserialize_file_chunk(data: &[u8]) -> Result<FileChunkNode, OxenError> {
        rmp_serde::from_slice(data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing file chunk node: {e}")))
    }

    pub fn schema(&self) -> Result<SchemaNode, OxenError> {
        Self::deserialize_schema(&self.data)
    }

    fn deserialize_schema(data: &[u8]) -> Result<SchemaNode, OxenError> {
        rmp_serde::from_slice(data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing schema node: {e}")))
    }
}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for MerkleTreeNodeData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "[{:?}]", self.dtype)?;
        writeln!(f, "hash: {}", self.hash.to_string())?;
        writeln!(f, "dtype: {:?}", self.dtype)?;
        writeln!(
            f,
            "data.len(): {:?}",
            bytesize::ByteSize::b(self.data.len() as u64)
        )?;
        writeln!(
            f,
            "parent_id: {}",
            self.parent_id
                .map_or("None".to_string(), |id| id.to_string())
        )?;
        writeln!(f, "children.len(): {}", self.children.len())?;
        writeln!(f, "=============")?;

        for child in &self.children {
            writeln!(f, "{}", child)?;
        }
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for MerkleTreeNodeData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.dtype {
            MerkleTreeNodeType::Commit => {
                let commit = self.commit().unwrap();
                write!(f, "[{:?}] {} {}", self.dtype, self.hash.to_string(), commit)
            }
            MerkleTreeNodeType::VNode => {
                let vnode = self.vnode().unwrap();
                write!(
                    f,
                    "[{:?}] {} {} ({} children)",
                    self.dtype,
                    self.hash.to_string(),
                    vnode,
                    self.children.len()
                )
            }
            MerkleTreeNodeType::Dir => {
                let dir = self.dir().unwrap();
                write!(
                    f,
                    "[{:?}] {} {} ({} children)",
                    self.dtype,
                    self.hash.to_string(),
                    dir,
                    self.children.len()
                )
            }
            MerkleTreeNodeType::File => {
                let file = self.file().unwrap();
                write!(f, "[{:?}] {} {}", self.dtype, self.hash.to_string(), file)
            }
            MerkleTreeNodeType::FileChunk => {
                let file_chunk = self.file_chunk().unwrap();
                write!(
                    f,
                    "[{:?}] {} {}",
                    self.dtype,
                    self.hash.to_string(),
                    file_chunk
                )
            }
            MerkleTreeNodeType::Schema => {
                let schema = self.schema().unwrap();
                write!(f, "[{:?}] {} {}", self.dtype, self.hash, schema)
            }
        }
    }
}

impl PartialEq for MerkleTreeNodeData {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Hash for MerkleTreeNodeData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.to_u128().hash(state);
    }
}
