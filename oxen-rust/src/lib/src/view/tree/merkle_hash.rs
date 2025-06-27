use serde::{Deserialize, Serialize};

use crate::model::merkle_tree::merkle_hash::MerkleHashAsString;
use crate::model::MerkleHash;
use crate::view::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct MerkleHashResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(with = "MerkleHashAsString")]
    pub hash: MerkleHash,
}
