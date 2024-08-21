//! This is a compact representation of a merkle tree schema node
//! that is stored in on disk
//!

use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct SchemaNode {
    // The name of the file the schema references
    pub name: String,
    // TODO: add schema metadata here
    // * width
    // * height
    // * fields
    //   * name
    //   * type
    pub dtype: MerkleTreeNodeType,
    pub hash: MerkleHash,
}

impl Default for SchemaNode {
    fn default() -> Self {
        SchemaNode {
            dtype: MerkleTreeNodeType::Schema,
            name: "".to_string(),
            hash: MerkleHash::new(0),
        }
    }
}

impl MerkleTreeNodeIdType for SchemaNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> MerkleHash {
        self.hash
    }
}

impl TMerkleTreeNode for SchemaNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for SchemaNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SchemaNode({})", self.hash.to_string())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for SchemaNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SchemaNode({})", self.hash.to_string())
    }
}
