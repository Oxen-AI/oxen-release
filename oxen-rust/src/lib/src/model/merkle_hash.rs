use crate::error::OxenError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MerkleHash(u128);

impl MerkleHash {
    pub fn new(hash: u128) -> Self {
        Self(hash)
    }

    pub fn from_str(s: &str) -> Result<Self, OxenError> {
        let hash = u128::from_str_radix(s, 16)?;
        Ok(Self(hash))
    }

    pub fn to_string(&self) -> String {
        format!("{:x}", self.0)
    }

    pub fn to_le_bytes(&self) -> [u8; 16] {
        self.0.to_le_bytes()
    }

    pub fn to_u128(&self) -> u128 {
        self.0
    }
}

impl fmt::Display for MerkleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl fmt::Debug for MerkleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MerkleHash({})", self.to_string())
    }
}

impl Hash for MerkleHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
