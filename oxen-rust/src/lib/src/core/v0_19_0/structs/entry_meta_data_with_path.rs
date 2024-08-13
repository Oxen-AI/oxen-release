use crate::model::EntryDataType;
use std::path::PathBuf;

#[derive(Clone)]
pub struct EntryMetaDataWithPath {
    pub path: PathBuf,
    pub hash: u128,
    pub num_bytes: u64,
    pub data_type: EntryDataType,
}
