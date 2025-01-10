//! Wrapper around the DirNodeData struct to support old versions of the dir node

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use crate::core::v_latest::model::merkle_tree::node::dir_node::DirNodeData as DirNodeDataV0_25_0;
use crate::error::OxenError;
use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};
use crate::view::DataTypeCount;

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum DirNode {
    V0_25_0(DirNodeDataV0_25_0),
}

impl DirNode {
    pub fn num_files(&self) -> u64 {
        // sum up the data type counts
        self.data_type_counts().values().sum()
    }

    pub fn data_types(&self) -> Vec<DataTypeCount> {
        self.data_type_counts()
            .iter()
            .map(|(k, v)| DataTypeCount {
                data_type: k.clone(),
                count: *v as usize,
            })
            .collect()
    }

    pub fn deserialize(data: &[u8]) -> Result<DirNode, OxenError> {
        // In order to support versions that didn't have the enum,
        // if it fails we will fall back to the old struct, then populate the enum
        let dir_node: DirNode = match rmp_serde::from_slice(data) {
            Ok(dir_node) => dir_node,
            Err(_) => {
                // This is a fallback for old versions of the dir node
                let dir_node: DirNodeDataV0_25_0 = rmp_serde::from_slice(data)?;
                DirNode::V0_25_0(dir_node)
            }
        };
        Ok(dir_node)
    }

    pub fn node_type(&self) -> MerkleTreeNodeType {
        match self {
            DirNode::V0_25_0(data) => data.node_type,
        }
    }

    pub fn hash(&self) -> MerkleHash {
        match self {
            DirNode::V0_25_0(data) => data.hash,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            DirNode::V0_25_0(data) => &data.name,
        }
    }

    pub fn set_name(&mut self, name: impl AsRef<str>) {
        match self {
            DirNode::V0_25_0(data) => data.name = name.as_ref().to_string(),
        }
    }

    pub fn num_bytes(&self) -> u64 {
        match self {
            DirNode::V0_25_0(data) => data.num_bytes,
        }
    }

    pub fn last_commit_id(&self) -> MerkleHash {
        match self {
            DirNode::V0_25_0(data) => data.last_commit_id,
        }
    }

    pub fn set_last_commit_id(&mut self, last_commit_id: MerkleHash) {
        match self {
            DirNode::V0_25_0(data) => data.last_commit_id = last_commit_id,
        }
    }

    pub fn last_modified_seconds(&self) -> i64 {
        match self {
            DirNode::V0_25_0(data) => data.last_modified_seconds,
        }
    }

    pub fn last_modified_nanoseconds(&self) -> u32 {
        match self {
            DirNode::V0_25_0(data) => data.last_modified_nanoseconds,
        }
    }

    pub fn data_type_counts(&self) -> &HashMap<String, u64> {
        match self {
            DirNode::V0_25_0(data) => &data.data_type_counts,
        }
    }

    pub fn data_type_sizes(&self) -> &HashMap<String, u64> {
        match self {
            DirNode::V0_25_0(data) => &data.data_type_sizes,
        }
    }
}

impl Default for DirNode {
    fn default() -> Self {
        DirNode::V0_25_0(DirNodeDataV0_25_0 {
            node_type: MerkleTreeNodeType::Dir,
            name: "".to_string(),
            hash: MerkleHash::new(0),
            num_bytes: 0,
            last_commit_id: MerkleHash::new(0),
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type_counts: HashMap::new(),
            data_type_sizes: HashMap::new(),
        })
    }
}

impl MerkleTreeNodeIdType for DirNode {
    fn node_type(&self) -> MerkleTreeNodeType {
        self.node_type()
    }

    fn hash(&self) -> MerkleHash {
        self.hash()
    }
}

impl TMerkleTreeNode for DirNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for DirNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DirNode")?;
        writeln!(f, "\thash: {}", self.hash())?;
        writeln!(f, "\tname: {}", self.name())?;
        writeln!(
            f,
            "\tnum_bytes: {}",
            bytesize::ByteSize::b(self.num_bytes())
        )?;
        writeln!(f, "\tdata_type_counts: {:?}", self.data_type_counts())?;
        writeln!(f, "\tdata_type_sizes: {:?}", self.data_type_sizes())?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for DirNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\"{}\" ({}) ({} files) (commit {}) ",
            self.name(),
            bytesize::ByteSize::b(self.num_bytes()),
            self.num_files(),
            self.last_commit_id().to_short_str()
        )
    }
}
