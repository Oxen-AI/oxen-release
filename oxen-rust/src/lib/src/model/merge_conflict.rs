use crate::model::CommitEntry;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MergeConflict {
    pub lca_entry: CommitEntry,   // Least Common Ancestor Entry
    pub base_entry: CommitEntry,  // Entry that existed in the base commit
    pub merge_entry: CommitEntry, // Entry we are trying to merge in
}
