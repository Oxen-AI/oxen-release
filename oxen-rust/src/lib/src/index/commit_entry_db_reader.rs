use crate::error::OxenError;
use crate::model::CommitEntry;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::Path;
use std::str;

pub struct CommitEntryDBReader {}

impl CommitEntryDBReader {
    pub fn has_file(db: &DBWithThreadMode<MultiThreaded>, path: &Path) -> bool {
        match CommitEntryDBReader::get_entry(db, path) {
            Ok(Some(_val)) => true,
            Ok(None) => false,
            Err(_err) => false,
        }
    }

    pub fn get_entry(
        db: &DBWithThreadMode<MultiThreaded>,
        path: &Path,
    ) -> Result<Option<CommitEntry>, OxenError> {
        let key = path.to_str().unwrap();
        let bytes = key.as_bytes();
        match db.get(bytes) {
            Ok(Some(value)) => match str::from_utf8(&*value) {
                Ok(value) => {
                    let entry: CommitEntry = serde_json::from_str(value)?;
                    Ok(Some(entry))
                }
                Err(_) => Err(OxenError::basic_str(
                    "CommitEntryDBReader::get_entry invalid entry",
                )),
            },
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!("CommitEntryDBReader::get_entry Error reading db\nErr: {}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }
}
