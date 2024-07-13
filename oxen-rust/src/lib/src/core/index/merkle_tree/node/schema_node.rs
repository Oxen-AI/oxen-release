//! This is a compact representation of a merkle tree schema node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct SchemaNode {
    pub path: String,
    // TODO: Put the raw schema here?
}
