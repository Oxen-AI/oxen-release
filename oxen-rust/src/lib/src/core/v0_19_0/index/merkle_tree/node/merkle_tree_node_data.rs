use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::Path;

use std::path::PathBuf;

use super::*;
use crate::core::v0_19_0::index::merkle_tree::node::MerkleNodeDB;
use crate::error::OxenError;
use crate::model::LocalRepository;

#[derive(Debug, Clone, Eq)]
pub struct MerkleTreeNodeData {
    pub hash: u128,
    pub dtype: MerkleTreeNodeType,
    pub data: Vec<u8>,
    pub parent_id: Option<u128>,
    pub children: Vec<MerkleTreeNodeData>,
}

impl fmt::Display for MerkleTreeNodeData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} [{:?}] ({})",
            self.hash,
            self.dtype,
            self.children.len()
        )
    }
}

impl MerkleTreeNodeData {
    /// Create an empty root node with a hash
    pub fn root_commit(repo: &LocalRepository, hash: u128) -> Result<Self, OxenError> {
        let node_db = MerkleNodeDB::open_read_only(repo, hash)?;
        Ok(MerkleTreeNodeData {
            hash,
            dtype: MerkleTreeNodeType::Commit,
            data: node_db.data(),
            parent_id: None,
            children: Vec::new(),
        })
    }

    /// Check if the node is a leaf node (i.e. it has no children)
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    /// Count the total number of vnodes in the tree
    pub fn total_vnodes(&self) -> u128 {
        let mut count = 0;
        for child in &self.children {
            if child.dtype == MerkleTreeNodeType::VNode {
                count += 1;
            }
            count += child.total_vnodes();
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

    pub fn deserialize_id(data: &[u8], dtype: MerkleTreeNodeType) -> Result<u128, OxenError> {
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

impl PartialEq for MerkleTreeNodeData {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Hash for MerkleTreeNodeData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}
