use serde::{Deserialize, Serialize};
use std::fmt;
use time::OffsetDateTime;

use super::{MerkleTreeNode, MerkleTreeNodeIdType, MerkleTreeNodeType};
use crate::model::Commit;

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
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

impl MerkleTreeNodeIdType for CommitNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> u128 {
        self.id
    }
}

impl MerkleTreeNode for CommitNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for CommitNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "CommitNode")?;
        writeln!(f, "\tmessage: {}", self.message)?;
        writeln!(f, "\tparent_ids: {:?}", self.parent_ids)?;
        writeln!(f, "\tauthor: {}", self.author)?;
        writeln!(f, "\temail: {}", self.email)?;
        writeln!(f, "\ttimestamp: {}", self.timestamp)?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for CommitNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let parent_ids = self
            .parent_ids
            .iter()
            .map(|x| format!("{:x}", x))
            .collect::<Vec<String>>()
            .join(",");
        write!(
            f,
            "\"{}\" -> {} {} parent_ids {:?}",
            self.message, self.author, self.email, parent_ids
        )
    }
}
