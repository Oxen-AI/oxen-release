use crate::constants::{FILES_DIR, HISTORY_DIR, SCHEMAS_DIR};
use crate::db;
use crate::db::{str_json_db, str_val_db};
use crate::error::OxenError;
use crate::model::Schema;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};
use std::str;

use crate::model::LocalRepository;

pub struct SchemaReader {
    schema_db: DBWithThreadMode<MultiThreaded>,
    schema_files_db: DBWithThreadMode<MultiThreaded>,
}

impl SchemaReader {
    pub fn schemas_db_dir(repo: &LocalRepository, commit_id: &str) -> PathBuf {
        // .oxen/history/COMMIT_ID/schemas/schemas
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(SCHEMAS_DIR) // double schemas/schemas is intentional because we have multiple dirs at this level
            .join(SCHEMAS_DIR)
    }

    pub fn schema_files_db_dir(repo: &LocalRepository, commit_id: &str) -> PathBuf {
        // .oxen/history/COMMIT_ID/schemas/files
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(SCHEMAS_DIR)
            .join(FILES_DIR)
    }

    pub fn new(repository: &LocalRepository, commit_id: &str) -> Result<SchemaReader, OxenError> {
        let schema_db_path = SchemaReader::schemas_db_dir(repository, commit_id);
        log::debug!("SchemaReader db {:?}", schema_db_path);
        let schema_files_db_path = SchemaReader::schema_files_db_dir(repository, commit_id);
        log::debug!("SchemaReader files db {:?}", schema_files_db_path);
        let opts = db::opts::default();
        if !schema_db_path.exists() {
            std::fs::create_dir_all(&schema_db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&schema_db_path))?;
        }

        if !schema_files_db_path.exists() {
            std::fs::create_dir_all(&schema_files_db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&schema_files_db_path))?;
        }

        Ok(SchemaReader {
            schema_db: DBWithThreadMode::open_for_read_only(&opts, &schema_db_path, false)?,
            schema_files_db: DBWithThreadMode::open_for_read_only(
                &opts,
                &schema_files_db_path,
                false,
            )?,
        })
    }

    /// See if a commit id exists
    pub fn schema_hash_exists(&self, hash: &str) -> bool {
        str_json_db::has_key(&self.schema_db, hash)
    }

    /// Get a commit object from an ID
    pub fn get_schema_by_hash<S: AsRef<str>>(&self, hash: S) -> Result<Option<Schema>, OxenError> {
        str_json_db::get(&self.schema_db, hash)
    }

    pub fn get_schema_hash_for_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Option<String>, OxenError> {
        str_val_db::get(&self.schema_files_db, path.as_ref().to_str().unwrap())
    }

    pub fn get_schema_for_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Option<Schema>, OxenError> {
        if let Some(hash) = self.get_schema_hash_for_file(path)? {
            return self.get_schema_by_hash(hash);
        }
        Ok(None)
    }

    pub fn list_schemas(&self) -> Result<Vec<Schema>, OxenError> {
        str_json_db::list_vals(&self.schema_db)
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
            let schemas = schema_reader.list_schemas()?;

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
            let schemas = schema_reader.list_schemas()?;

            assert_eq!(schemas.len(), 3);

            Ok(())
        })
    }
}
