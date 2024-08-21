use serde::{Deserialize, Serialize};

use crate::core::v0_19_0::index::merkle_tree::node::FileNode;
use crate::view::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct FileNodeResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub node: FileNode,
}
