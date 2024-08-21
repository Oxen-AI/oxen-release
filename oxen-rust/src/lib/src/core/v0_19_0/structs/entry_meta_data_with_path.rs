use crate::model::{EntryDataType, MerkleHash, StagedEntryStatus};
use std::fmt;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

#[derive(Clone, Eq, Debug)]
pub struct EntryMetaDataWithPath {
    pub path: PathBuf,
    pub hash: MerkleHash,
    pub num_bytes: u64,
    pub data_type: EntryDataType,
    pub status: StagedEntryStatus,
    pub last_commit_id: MerkleHash,
}

impl PartialEq for EntryMetaDataWithPath {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Hash for EntryMetaDataWithPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl Display for EntryMetaDataWithPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EntryMetaDataWithPath {{ path: {:?}, hash: {}, num_bytes: {}, data_type: {:?}, status: {:?} }}",
            self.path, self.hash, self.num_bytes, self.data_type, self.status
        )
    }
}
