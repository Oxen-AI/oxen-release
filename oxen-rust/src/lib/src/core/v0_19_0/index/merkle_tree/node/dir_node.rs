//! This is a compact representation of a directory merkle tree node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use crate::view::DataTypeCount;

use super::{MerkleTreeNode, MerkleTreeNodeIdType, MerkleTreeNodeType};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct DirNode {
    // The type of the node
    pub dtype: MerkleTreeNodeType,

    // The name of the directory
    pub name: String,

    // Hash of all the children
    pub hash: u128,
    // Number of bytes in the file
    pub num_bytes: u64,
    // Last commit id that modified the file
    pub last_commit_id: u128,
    // Last modified timestamp
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    // Total number of files in the directory
    pub data_type_counts: HashMap<String, usize>,
}

impl DirNode {
    pub fn num_files(&self) -> usize {
        // sum up the data type counts
        self.data_type_counts.values().sum()
    }

    pub fn data_types(&self) -> Vec<DataTypeCount> {
        self.data_type_counts
            .iter()
            .map(|(k, v)| DataTypeCount {
                data_type: k.clone(),
                count: *v,
            })
            .collect()
    }
}

impl Default for DirNode {
    fn default() -> Self {
        DirNode {
            dtype: MerkleTreeNodeType::Dir,
            name: "".to_string(),
            hash: 0,
            num_bytes: 0,
            last_commit_id: 0,
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type_counts: HashMap::new(),
        }
    }
}

impl MerkleTreeNodeIdType for DirNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> u128 {
        self.hash
    }
}

impl MerkleTreeNode for DirNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for DirNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DirNode")?;
        writeln!(f, "\thash: {:x}", self.hash)?;
        writeln!(f, "\tname: {}", self.name)?;
        writeln!(f, "\tnum_bytes: {}", bytesize::ByteSize::b(self.num_bytes))?;
        writeln!(f, "\tdata_type_counts: {:?}", self.data_type_counts)?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for DirNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\"{}/\" ({}) ({} files) (commit {:x}) ",
            self.name,
            bytesize::ByteSize::b(self.num_bytes),
            self.num_files(),
            self.last_commit_id
        )
    }
}
