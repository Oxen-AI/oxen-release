use crate::constants::{HISTORY_DIR, SCHEMAS_DIR};
use crate::db;
use crate::error::OxenError;
use crate::index::kv_json_db;
use crate::model::Schema;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::PathBuf;
use std::str;

use crate::model::LocalRepository;

pub struct SchemaReader {
    db: DBWithThreadMode<MultiThreaded>,
}

impl SchemaReader {
    pub fn db_dir(repo: &LocalRepository, commit_id: &str) -> PathBuf {
        // .oxen/history/COMMIT_ID/schemas/path/to/dir
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(SCHEMAS_DIR)
    }

    /// Create a new reader that can find commits, list history, etc
    pub fn new(repository: &LocalRepository, commit_id: &str) -> Result<SchemaReader, OxenError> {
        let db_path = SchemaReader::db_dir(repository, commit_id);
        let opts = db::opts::default();
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open(&opts, &db_path)?;
        }

        Ok(SchemaReader {
            db: DBWithThreadMode::open_for_read_only(&opts, &db_path, false)?,
        })
    }

    /// See if a commit id exists
    pub fn hash_exists(&self, hash: &str) -> bool {
        kv_json_db::has_key(&self.db, hash)
    }

    /// Get a commit object from an ID
    pub fn get_by_hash<S: AsRef<str>>(&self, hash: S) -> Result<Option<Schema>, OxenError> {
        kv_json_db::get(&self.db, hash)
    }

    pub fn list(&self) -> Result<Vec<Schema>, OxenError> {
        kv_json_db::list_vals(&self.db)
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::SchemaReader;
    use crate::test;

    #[test]
    fn test_list_empty_schemas() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let history = command::log(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;
            let schemas = schema_reader.list()?;

            assert_eq!(schemas.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_list_committed_schemas() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;
            let schemas = schema_reader.list()?;

            assert_eq!(schemas.len(), 1);

            Ok(())
        })
    }
}
