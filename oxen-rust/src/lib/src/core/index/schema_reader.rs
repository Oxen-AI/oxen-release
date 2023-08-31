use crate::constants::{FILES_DIR, HISTORY_DIR, SCHEMAS_DIR};
use crate::core::db;
use crate::core::db::{str_json_db, str_val_db};
use crate::error::OxenError;
use crate::model::Schema;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
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

    pub fn list_schemas(&self) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        // This is a map of paths to schema hashes
        let paths_to_hashes: HashMap<String, String> = str_val_db::hash_map(&self.schema_files_db)?;

        // This is a map of hashes to schemas
        let hash_to_schemas: HashMap<String, Schema> = str_json_db::hash_map(&self.schema_db)?;

        // For each path, get the schema
        let path_vals: HashMap<PathBuf, Schema> = paths_to_hashes
            .iter()
            .map(|(k, v)| (PathBuf::from(k), hash_to_schemas.get(v).unwrap().clone()))
            .collect();
        Ok(path_vals)
    }

    pub fn list_schemas_for_ref(
        &self,
        schema_ref: impl AsRef<str>,
    ) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        let schema_ref = schema_ref.as_ref();
        // This is a map of paths to schema hashes
        let paths_to_hashes: HashMap<String, String> = str_val_db::hash_map(&self.schema_files_db)?;

        // This is a map of hashes to schemas
        let hash_to_schemas: HashMap<String, Schema> = str_json_db::hash_map(&self.schema_db)?;

        // For each path, get the schema
        let path_vals: HashMap<PathBuf, Schema> = paths_to_hashes
            .iter()
            .map(|(k, v)| (PathBuf::from(k), hash_to_schemas.get(v).unwrap().clone()))
            .filter(|(k, v)| {
                k.to_string_lossy() == schema_ref
                    || v.hash == schema_ref
                    || v.name == Some(schema_ref.to_string())
            })
            .collect();
        Ok(path_vals)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::api;
    use crate::core::index::SchemaReader;
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_schema_reader_list_empty_schemas() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let history = api::local::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;
            let schemas = schema_reader.list_schemas()?;

            assert_eq!(schemas.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_schema_reader_list_committed_schemas() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = api::local::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;
            let schemas = schema_reader.list_schemas()?;

            for (k, v) in schemas.iter() {
                println!("{}: {}", k.to_string_lossy(), v.hash);
            }

            assert_eq!(schemas.len(), 7);
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/bounding_box.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/one_shot.csv")));
            assert!(
                schemas.contains_key(&PathBuf::from("nlp/classification/annotations/train.tsv"))
            );
            assert!(schemas.contains_key(&PathBuf::from("large_files/test.csv")));
            assert!(schemas.contains_key(&PathBuf::from("nlp/classification/annotations/test.tsv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/two_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/test/annotations.csv")));

            Ok(())
        })
    }

    #[test]
    fn test_schema_reader_get_schema_ref_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = api::local::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;

            let schema_ref = "annotations/train/bounding_box.csv";
            let schemas = schema_reader.list_schemas_for_ref(schema_ref)?;

            assert_eq!(schemas.len(), 1);
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/bounding_box.csv")));

            Ok(())
        })
    }

    #[test]
    fn test_schema_reader_get_schema_ref_by_name() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = api::local::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;

            let schema_ref = "bounding_box";
            let schemas = schema_reader.list_schemas_for_ref(schema_ref)?;

            assert_eq!(schemas.len(), 4);
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/bounding_box.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/one_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/two_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/test/annotations.csv")));

            Ok(())
        })
    }

    #[test]
    fn test_schema_reader_get_schema_ref_by_hash() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = api::local::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;

            let schema_ref = "b821946753334c083124fd563377d795";
            let schemas = schema_reader.list_schemas_for_ref(schema_ref)?;

            for (k, v) in schemas.iter() {
                println!("{}: {}", k.to_string_lossy(), v);
            }

            assert_eq!(schemas.len(), 4);
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/bounding_box.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/one_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/two_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/test/annotations.csv")));

            Ok(())
        })
    }
}
