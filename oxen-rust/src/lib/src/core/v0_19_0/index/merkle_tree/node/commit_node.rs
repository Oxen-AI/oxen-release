use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::{MerkleTreeNode, MerkleTreeNodeType};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct CommitNode {
    pub id: u128,
    pub dtype: MerkleTreeNodeType,
    pub parent_ids: Vec<u128>,
    pub message: String,
    pub author: String,
    pub email: String,
    pub timestamp: OffsetDateTime,
}

impl Default for CommitNode {
    fn default() -> Self {
        CommitNode {
            id: 0,
            dtype: MerkleTreeNodeType::Commit,
            parent_ids: vec![],
            message: "".to_string(),
            author: "".to_string(),
            email: "".to_string(),
            timestamp: OffsetDateTime::now_utc(),
        }
    }
}

impl MerkleTreeNode for CommitNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> u128 {
        self.id
    }
}