//! This is a compact representation of a directory merkle tree node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use crate::view::DataTypeCount;

use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct DirNode {
    // The type of the node
    pub dtype: MerkleTreeNodeType,

    // The name of the directory
    pub name: String,

    // Hash of all the children
    pub hash: MerkleHash,
    // Recursive size of the directory
    pub num_bytes: u64,
    // Last commit id that modified the file
    pub last_commit_id: MerkleHash,
    // Last modified timestamp
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    // Recursive file counts in the directory
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
            hash: MerkleHash::new(0),
            num_bytes: 0,
            last_commit_id: MerkleHash::new(0),
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

    fn hash(&self) -> MerkleHash {
        self.hash
    }
}

impl TMerkleTreeNode for DirNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for DirNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DirNode")?;
        writeln!(f, "\thash: {}", self.hash)?;
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
            "\"{}/\" ({}) ({} files) (commit {}) ",
            self.name,
            bytesize::ByteSize::b(self.num_bytes),
            self.num_files(),
            self.last_commit_id
        )
    }
}
