use std::path::PathBuf;

use crate::model::CommitEntry;
use serde::{Deserialize, Serialize};

use super::merkle_tree::node::FileNode;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MergeConflict {
    pub lca_entry: CommitEntry,   // Least Common Ancestor Entry
    pub base_entry: CommitEntry,  // Entry that existed in the base commit
    pub merge_entry: CommitEntry, // Entry we are trying to merge in
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NodeMergeConflict {
    pub lca_entry: (FileNode, PathBuf),  // Least Common Ancestor Entry
    pub base_entry: (FileNode, PathBuf), // Entry that existed in the base commit
    pub merge_entry: (FileNode, PathBuf), // Entry we are trying to merge in
}
