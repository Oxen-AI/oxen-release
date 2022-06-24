use crate::constants::MERGE_DIR;
use crate::db;
use crate::error::OxenError;
use crate::model::{MergeConflict, LocalRepository};
use crate::util;

use std::path::Path;
use rocksdb::{IteratorMode, DB};
use std::str;

pub struct MergeConflictReader {
    merge_db: DB,
}

impl MergeConflictReader {
    pub fn new(repo: &LocalRepository) -> Result<MergeConflictReader, OxenError> {
        let db_path = util::fs::oxen_hidden_dir(&repo.path).join(Path::new(MERGE_DIR));
        log::debug!("MergeConflictReader::new() DB {:?}", db_path);

        let opts = db::opts::default();
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
            // open it then lose scope to close it
            let _db = DB::open(&opts, &db_path)?;
        }

        Ok(MergeConflictReader {
            merge_db: DB::open_for_read_only(&opts, &db_path, false)?,
        })
    }

    pub fn has_conflicts(&self) -> Result<bool, OxenError> {
        Ok(self.merge_db.iterator(IteratorMode::Start).count() > 0)
    }

    pub fn list_conflicts(&self) -> Result<Vec<MergeConflict>, OxenError> {
        let mut conflicts: Vec<MergeConflict> = vec![];
        let iter = self.merge_db.iterator(IteratorMode::Start);
        for (_key, value) in iter {
            let entry: MergeConflict = serde_json::from_str(str::from_utf8(&*value)?)?;
            conflicts.push(entry);
        }
        Ok(conflicts)
    }
}