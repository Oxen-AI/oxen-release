use crate::model::{EntryDataType, StagedEntryStatus};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

#[derive(Clone, Eq)]
pub struct EntryMetaDataWithPath {
    pub path: PathBuf,
    pub hash: u128,
    pub num_bytes: u64,
    pub data_type: EntryDataType,
    pub status: StagedEntryStatus,
    pub last_commit_id: u128,
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
