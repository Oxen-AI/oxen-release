use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::model::{MerkleHash, MerkleTreeNodeType};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct CommitNodeData {
    pub hash: MerkleHash,
    pub node_type: MerkleTreeNodeType,
    pub parent_ids: Vec<MerkleHash>,
    pub message: String,
    pub author: String,
    pub email: String,
    pub timestamp: OffsetDateTime,
}
