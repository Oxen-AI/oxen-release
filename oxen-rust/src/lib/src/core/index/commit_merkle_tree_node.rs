use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MerkleTreeNodeType {
    File,
    Dir,
    VNode,
    Schema,
    FileChunk,
}

#[derive(Debug, Clone, Eq)]
pub struct CommitMerkleTreeNode {
    pub path: PathBuf,
    pub hash: String,
    pub dtype: MerkleTreeNodeType,
    pub children: HashSet<CommitMerkleTreeNode>,
}

impl CommitMerkleTreeNode {
    /// Constant time lookup by hash
    pub fn get_by_hash(&self, hash: &str) -> Option<&CommitMerkleTreeNode> {
        let lookup_node = CommitMerkleTreeNode {
            path: PathBuf::new(), // Dummy value
            hash: hash.to_string(),
            dtype: MerkleTreeNodeType::File, // Dummy value
            children: HashSet::new(), // Dummy value
        };
        self.children.get(&lookup_node)
    }

    /// Linear time lookup by path
    pub fn get_by_path(&self, path: impl AsRef<Path>) -> Option<&CommitMerkleTreeNode> {
        self.children.iter().find(|&child| child.path == path.as_ref())
    }

    /// Check if the node is a leaf node (i.e. it has no children)
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }
}

impl PartialEq for CommitMerkleTreeNode {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Hash for CommitMerkleTreeNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}