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

    pub fn to_le_bytes(&self) -> [u8; 16] {
        self.0.to_le_bytes()
    }

    pub fn to_u128(&self) -> u128 {
        self.0
    }
}

impl std::str::FromStr for MerkleHash {
    type Err = OxenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hash = u128::from_str_radix(s, 16)?;
        Ok(Self(hash))
    }
}

impl fmt::Display for MerkleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

impl fmt::Debug for MerkleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MerkleHash({})", self)
    }
}

impl Hash for MerkleHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
