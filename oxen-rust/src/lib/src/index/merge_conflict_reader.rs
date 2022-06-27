use crate::constants::MERGE_DIR;
use crate::db;
use crate::error::OxenError;
use crate::index::MergeConflictDBReader;
use crate::model::{LocalRepository, MergeConflict};
use crate::util;

use rocksdb::DB;
use std::path::Path;

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
        MergeConflictDBReader::has_conflicts(&self.merge_db)
    }

    pub fn list_conflicts(&self) -> Result<Vec<MergeConflict>, OxenError> {
        MergeConflictDBReader::list_conflicts(&self.merge_db)
    }

    pub fn has_file(&self, path: &Path) -> Result<bool, OxenError> {
        MergeConflictDBReader::has_file(&self.merge_db, path)
    }
}
