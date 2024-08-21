use serde::{Deserialize, Serialize};

use crate::core::v0_19_0::index::merkle_tree::node::DirNode;
use crate::view::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct DirNodeResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub node: DirNode,
}
