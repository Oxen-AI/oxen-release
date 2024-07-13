//! This is the type of node that we are storing in the merkle tree
//!
//! There are only 5 node types as of now, so can store in a u8, and would
//! need a migration to change anyways.
//!
//! This value is stored at the top of a merkle tree db file
//! to know how to deserialize the node type
//!

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum MerkleTreeNodeType {
    File,
    Dir,
    VNode,
    Schema,
    FileChunk,
    // TODO: Add FileChunk

    //       Fun realization - this fixes our push protocol of having to
    //       "chunk" large files before sending and reconstruct on the other side
    //       because we already chunked, and we can write them directly to the merkle tree
    //       on the other side
}

impl MerkleTreeNodeType {
    pub fn to_u8(&self) -> u8 {
        match self {
            MerkleTreeNodeType::Dir => 0u8,
            MerkleTreeNodeType::VNode => 1u8,
            MerkleTreeNodeType::File => 2u8,
            MerkleTreeNodeType::Schema => 3u8,
            MerkleTreeNodeType::FileChunk => 4u8,
        }
    }

    pub fn from_u8(val: u8) -> MerkleTreeNodeType {
        match val {
            0u8 => MerkleTreeNodeType::Dir,
            1u8 => MerkleTreeNodeType::VNode,
            2u8 => MerkleTreeNodeType::File,
            3u8 => MerkleTreeNodeType::Schema,
            4u8 => MerkleTreeNodeType::FileChunk,
            _ => panic!("Invalid MerkleTreeNodeType"),
        }
    }
}
