//! This is a compact representation of a merkle tree schema node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};

use super::{MerkleTreeNode, MerkleTreeNodeType};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
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
    pub hash: u128,
}

impl Default for SchemaNode {
    fn default() -> Self {
        SchemaNode {
            dtype: MerkleTreeNodeType::Schema,
            name: "".to_string(),
            hash: 0,
        }
    }
}

impl MerkleTreeNode for SchemaNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> u128 {
        self.hash
    }
}
