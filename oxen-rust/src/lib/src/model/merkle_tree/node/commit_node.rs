use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use time::OffsetDateTime;

use crate::core::v_latest::model::merkle_tree::node::commit_node::CommitNodeData as CommitNodeDataV0_25_0;
use crate::error::OxenError;
use crate::model::Commit;
use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum CommitNode {
    V0_25_0(CommitNodeDataV0_25_0),
}

impl CommitNode {
    pub fn new(
        hash: MerkleHash,
        parent_ids: Vec<MerkleHash>,
        email: String,
        author: String,
        message: String,
        timestamp: OffsetDateTime,
    ) -> CommitNode {
        CommitNode::V0_25_0(CommitNodeDataV0_25_0 {
            hash,
            parent_ids,
            email,
            author,
            message,
            timestamp,
            node_type: MerkleTreeNodeType::Commit,
        })
    }

    pub fn from_commit(commit: Commit) -> CommitNode {
        CommitNode::V0_25_0(CommitNodeDataV0_25_0 {
            hash: MerkleHash::from_str(&commit.id).unwrap(),
            parent_ids: commit
                .parent_ids
                .iter()
                .map(|id| MerkleHash::from_str(id).unwrap())
                .collect(),
            email: commit.email.clone(),
            author: commit.author.clone(),
            message: commit.message.clone(),
            timestamp: commit.timestamp,
            node_type: MerkleTreeNodeType::Commit,
        })
    }

    pub fn to_commit(&self) -> Commit {
        Commit {
            id: self.hash().to_string(),
            parent_ids: self.parent_ids().iter().map(|id| id.to_string()).collect(),
            email: self.email().to_owned(),
            author: self.author().to_owned(),
            message: self.message().to_owned(),
            timestamp: self.timestamp().to_owned(),
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<CommitNode, OxenError> {
        // In order to support versions that didn't have the enum,
        // if it fails we will fall back to the old struct, then populate the enum
        let commit: CommitNode = match rmp_serde::from_slice(data) {
            Ok(commit) => commit,
            Err(e) => {
                log::debug!(
                    "Failed to deserialize CommitNode, falling back to CommitNodeV0_25_0: {}",
                    e
                );
                let commit: CommitNodeDataV0_25_0 = rmp_serde::from_slice(data)?;
                CommitNode::V0_25_0(commit)
            }
        };
        Ok(commit)
    }

    pub fn hash(&self) -> MerkleHash {
        match self {
            CommitNode::V0_25_0(commit) => commit.hash,
        }
    }

    pub fn parent_ids(&self) -> Vec<MerkleHash> {
        match self {
            CommitNode::V0_25_0(commit) => commit.parent_ids.clone(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            CommitNode::V0_25_0(commit) => &commit.message,
        }
    }

    pub fn author(&self) -> &str {
        match self {
            CommitNode::V0_25_0(commit) => &commit.author,
        }
    }

    pub fn email(&self) -> &str {
        match self {
            CommitNode::V0_25_0(commit) => &commit.email,
        }
    }

    pub fn timestamp(&self) -> OffsetDateTime {
        match self {
            CommitNode::V0_25_0(commit) => commit.timestamp,
        }
    }
}

impl Default for CommitNode {
    fn default() -> Self {
        CommitNode::V0_25_0(CommitNodeDataV0_25_0 {
            hash: MerkleHash::new(0),
            node_type: MerkleTreeNodeType::Commit,
            parent_ids: vec![],
            message: "".to_string(),
            author: "".to_string(),
            email: "".to_string(),
            timestamp: OffsetDateTime::now_utc(),
        })
    }
}

impl MerkleTreeNodeIdType for CommitNode {
    fn node_type(&self) -> MerkleTreeNodeType {
        match self {
            CommitNode::V0_25_0(commit) => commit.node_type,
        }
    }

    fn hash(&self) -> MerkleHash {
        match self {
            CommitNode::V0_25_0(commit) => commit.hash,
        }
    }
}

impl TMerkleTreeNode for CommitNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for CommitNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "CommitNode")?;
        writeln!(f, "\tmessage: {}", self.message())?;
        writeln!(f, "\tparent_ids: {:?}", self.parent_ids())?;
        writeln!(f, "\tauthor: {}", self.author())?;
        writeln!(f, "\temail: {}", self.email())?;
        writeln!(f, "\ttimestamp: {}", self.timestamp())?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for CommitNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let parent_ids = self
            .parent_ids()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(",");
        write!(
            f,
            "\"{}\" -> {} {} parent_ids {:?}",
            self.message(),
            self.author(),
            self.email(),
            parent_ids
        )
    }
}
