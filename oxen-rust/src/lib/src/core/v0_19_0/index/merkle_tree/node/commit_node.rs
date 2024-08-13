use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use time::OffsetDateTime;

use super::{MerkleTreeNode, MerkleTreeNodeType};
use crate::model::Commit;

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

impl CommitNode {
    pub fn to_commit(&self) -> Commit {
        Commit {
            id: format!("{:x}", self.id),
            parent_ids: self
                .parent_ids
                .iter()
                .map(|id| format!("{:x}", id))
                .collect(),
            email: self.email.to_owned(),
            author: self.author.to_owned(),
            message: self.message.to_owned(),
            timestamp: self.timestamp.to_owned(),
            root_hash: None,
        }
    }
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

impl Display for CommitNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CommitNode({:x}, {})", self.id, self.message,)
    }
}
