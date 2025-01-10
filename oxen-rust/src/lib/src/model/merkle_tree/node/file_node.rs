//! Wrapper around the FileNodeData struct to support old versions of the file node

use crate::core::v_latest::model::merkle_tree::node::file_node::FileNodeData as FileNodeDataV0_25_0;
use crate::error::OxenError;
use crate::model::merkle_tree::node::file_node_types::{FileChunkType, FileStorageType};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{
    EntryDataType, MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

pub struct FileNodeOpts {
    pub name: String,
    pub hash: MerkleHash,
    pub combined_hash: MerkleHash,
    pub metadata_hash: Option<MerkleHash>,
    pub num_bytes: u64,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    pub data_type: EntryDataType,
    pub metadata: Option<GenericMetadata>,
    pub mime_type: String,
    pub extension: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub enum FileNode {
    V0_25_0(FileNodeDataV0_25_0),
}

impl FileNode {
    pub fn new(opts: FileNodeOpts) -> Self {
        FileNode::V0_25_0(FileNodeDataV0_25_0 {
            node_type: MerkleTreeNodeType::File,
            name: opts.name,
            hash: opts.hash,
            combined_hash: opts.combined_hash,
            metadata_hash: opts.metadata_hash,
            num_bytes: opts.num_bytes,
            last_commit_id: MerkleHash::new(0),
            last_modified_seconds: opts.last_modified_seconds,
            last_modified_nanoseconds: opts.last_modified_nanoseconds,
            data_type: opts.data_type,
            metadata: opts.metadata,
            mime_type: opts.mime_type,
            extension: opts.extension,
            chunk_hashes: vec![],
            chunk_type: FileChunkType::SingleFile,
            storage_backend: FileStorageType::Disk,
        })
    }

    pub fn deserialize(data: &[u8]) -> Result<FileNode, OxenError> {
        let file_node: FileNode = match rmp_serde::from_slice(data) {
            Ok(file_node) => file_node,
            Err(_) => {
                // This is a fallback for old versions of the file node
                let file_node: FileNodeDataV0_25_0 = rmp_serde::from_slice(data)?;
                FileNode::V0_25_0(file_node)
            }
        };
        Ok(file_node)
    }

    pub fn node_type(&self) -> MerkleTreeNodeType {
        match self {
            FileNode::V0_25_0(_) => MerkleTreeNodeType::File,
        }
    }

    pub fn hash(&self) -> MerkleHash {
        match self {
            FileNode::V0_25_0(file_node) => file_node.hash,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            FileNode::V0_25_0(file_node) => &file_node.name,
        }
    }

    pub fn set_name(&mut self, name: impl AsRef<str>) {
        match self {
            FileNode::V0_25_0(file_node) => file_node.name = name.as_ref().to_string(),
        }
    }

    pub fn combined_hash(&self) -> MerkleHash {
        match self {
            FileNode::V0_25_0(file_node) => file_node.combined_hash,
        }
    }

    pub fn set_combined_hash(&mut self, combined_hash: MerkleHash) {
        match self {
            FileNode::V0_25_0(file_node) => file_node.combined_hash = combined_hash,
        }
    }

    pub fn metadata_hash(&self) -> Option<MerkleHash> {
        match self {
            FileNode::V0_25_0(file_node) => file_node.metadata_hash,
        }
    }

    pub fn set_metadata_hash(&mut self, metadata_hash: Option<MerkleHash>) {
        match self {
            FileNode::V0_25_0(file_node) => file_node.metadata_hash = metadata_hash,
        }
    }

    pub fn num_bytes(&self) -> u64 {
        match self {
            FileNode::V0_25_0(file_node) => file_node.num_bytes,
        }
    }

    pub fn last_commit_id(&self) -> MerkleHash {
        match self {
            FileNode::V0_25_0(file_node) => file_node.last_commit_id,
        }
    }

    pub fn set_last_commit_id(&mut self, last_commit_id: MerkleHash) {
        match self {
            FileNode::V0_25_0(file_node) => file_node.last_commit_id = last_commit_id,
        }
    }

    pub fn last_modified_seconds(&self) -> i64 {
        match self {
            FileNode::V0_25_0(file_node) => file_node.last_modified_seconds,
        }
    }

    pub fn last_modified_nanoseconds(&self) -> u32 {
        match self {
            FileNode::V0_25_0(file_node) => file_node.last_modified_nanoseconds,
        }
    }

    pub fn data_type(&self) -> EntryDataType {
        match self {
            FileNode::V0_25_0(file_node) => file_node.data_type.clone(),
        }
    }

    pub fn metadata(&self) -> Option<GenericMetadata> {
        match self {
            FileNode::V0_25_0(file_node) => file_node.metadata.clone(),
        }
    }

    pub fn get_mut_metadata(&mut self) -> &mut Option<GenericMetadata> {
        match self {
            FileNode::V0_25_0(file_node) => &mut file_node.metadata,
        }
    }

    pub fn set_metadata(&mut self, metadata: Option<GenericMetadata>) {
        match self {
            FileNode::V0_25_0(file_node) => file_node.metadata = metadata,
        }
    }

    pub fn mime_type(&self) -> &str {
        match self {
            FileNode::V0_25_0(file_node) => &file_node.mime_type,
        }
    }

    pub fn extension(&self) -> &str {
        match self {
            FileNode::V0_25_0(file_node) => &file_node.extension,
        }
    }

    pub fn chunk_hashes(&self) -> Vec<u128> {
        match self {
            FileNode::V0_25_0(file_node) => file_node.chunk_hashes.clone(),
        }
    }

    pub fn set_chunk_hashes(&mut self, chunk_hashes: Vec<u128>) {
        match self {
            FileNode::V0_25_0(file_node) => file_node.chunk_hashes = chunk_hashes,
        }
    }

    pub fn chunk_type(&self) -> FileChunkType {
        match self {
            FileNode::V0_25_0(file_node) => file_node.chunk_type.clone(),
        }
    }

    pub fn storage_backend(&self) -> FileStorageType {
        match self {
            FileNode::V0_25_0(file_node) => file_node.storage_backend.clone(),
        }
    }
}

impl Default for FileNode {
    fn default() -> Self {
        FileNode::V0_25_0(FileNodeDataV0_25_0 {
            node_type: MerkleTreeNodeType::File,
            name: "".to_string(),
            hash: MerkleHash::new(0),
            combined_hash: MerkleHash::new(0),
            metadata_hash: None,
            num_bytes: 0,
            last_commit_id: MerkleHash::new(0),
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type: EntryDataType::Binary,
            metadata: None,
            mime_type: "".to_string(),
            extension: "".to_string(),
            chunk_hashes: vec![],
            chunk_type: FileChunkType::SingleFile,
            storage_backend: FileStorageType::Disk,
        })
    }
}

impl MerkleTreeNodeIdType for FileNode {
    fn node_type(&self) -> MerkleTreeNodeType {
        self.node_type()
    }

    fn hash(&self) -> MerkleHash {
        self.hash()
    }
}

impl Hash for FileNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.num_bytes().hash(state);
        self.last_modified_seconds().hash(state);
        self.last_modified_nanoseconds().hash(state);
        self.hash().hash(state);
    }
}

impl TMerkleTreeNode for FileNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for FileNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "FileNode")?;
        writeln!(f, "\thash: {}", self.hash())?;
        writeln!(f, "\tname: {}", self.name())?;
        writeln!(
            f,
            "\tnum_bytes: {}",
            bytesize::ByteSize::b(self.num_bytes())
        )?;
        writeln!(f, "\tdata_type: {:?}", self.data_type())?;
        writeln!(f, "\tmetadata: {:?}", self.metadata())?;
        writeln!(f, "\tmime_type: {}", self.mime_type())?;
        writeln!(f, "\textension: {}", self.extension())?;
        writeln!(f, "\tchunk_hashes: {:?}", self.chunk_hashes())?;
        writeln!(f, "\tchunk_type: {:?}", self.chunk_type())?;
        writeln!(f, "\tstorage_backend: {:?}", self.storage_backend())?;
        writeln!(f, "\tlast_commit_id: {}", self.last_commit_id())?;
        writeln!(
            f,
            "\tlast_modified_seconds: {}",
            self.last_modified_seconds()
        )?;
        writeln!(
            f,
            "\tlast_modified_nanoseconds: {}",
            self.last_modified_nanoseconds()
        )?;
        writeln!(f, "\tmetadata: {:?}", self.metadata())?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for FileNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\"{}\" ({}) {} [{}] (commit {})",
            self.name(),
            self.mime_type(),
            bytesize::ByteSize::b(self.num_bytes()),
            self.hash().to_short_str(),
            self.last_commit_id().to_short_str()
        )?;
        if let Some(metadata) = self.metadata() {
            write!(f, " {}", metadata)?;
        }
        Ok(())
    }
}

impl PartialEq for FileNode {
    fn eq(&self, other: &Self) -> bool {
        self.hash() == other.hash()
    }
}

impl Eq for FileNode {}
