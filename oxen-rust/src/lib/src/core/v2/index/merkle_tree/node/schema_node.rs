//! This is a compact representation of a merkle tree schema node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct SchemaNode {
    // The name of the file the schema references
    pub name: String,
    // TODO: add schema metadata here
    // * width
    // * height
    // * fields
    //   * name
    //   * type
}
