use serde::{Deserialize, Serialize};

use crate::model::MerkleHash;
use crate::view::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct MerkleHashResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub hash: MerkleHash,
}
