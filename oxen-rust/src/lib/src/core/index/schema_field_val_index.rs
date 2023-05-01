use crate::constants::{FIELDS_DIR, HISTORY_DIR, INDICES_DIR};
use crate::core::db;
use crate::error::OxenError;
use crate::model::{schema::Field, Commit, Schema};
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::PathBuf;
use std::str;

use crate::model::LocalRepository;

pub struct SchemaFieldValIndex {
    field_indices_db: DBWithThreadMode<MultiThreaded>,
}

impl SchemaFieldValIndex {
    pub fn db_dir(
        repo: &LocalRepository,
        commit: &Commit,
        schema: &Schema,
        field: &Field,
    ) -> PathBuf {
        let key_hash = util::hasher::hash_str(&field.name);
        // .oxen/history/COMMIT_ID/indices/SCHEMA_HASH/fields/FIELD_NAME_HASH
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(&commit.id)
            .join(INDICES_DIR)
            .join(&schema.hash)
            .join(FIELDS_DIR)
            .join(key_hash)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
        schema: &Schema,
        field: &Field,
    ) -> Result<SchemaFieldValIndex, OxenError> {
        let db_path = SchemaFieldValIndex::db_dir(repository, commit, schema, field);
        log::debug!("SchemaFieldValIndex db {:?}", db_path);
        let opts = db::opts::default();
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }

        Ok(SchemaFieldValIndex {
            field_indices_db: DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?,
        })
    }

    pub fn insert_index<S: AsRef<str>>(&self, key: S, indices: Vec<u32>) -> Result<(), OxenError> {
        db::index_db::insert_indices(&self.field_indices_db, key, indices)
    }

    pub fn get_indices<S: AsRef<str>>(&self, key: S) -> Result<Option<Vec<u32>>, OxenError> {
        db::index_db::get_indices(&self.field_indices_db, key)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::index::SchemaFieldValIndex;
    use crate::error::OxenError;
    use crate::model::schema;
    use crate::test;
    use crate::{api, command};

    #[test]
    fn test_list_empty_indices() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = api::local::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schemas = command::schemas::list(&repo, Some(&last_commit.id))?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();

            // As if we aggregated up the "label" field and "dog" was a value
            let field = schema::Field {
                name: String::from("label"),
                dtype: String::from("str"),
            };

            let reader = SchemaFieldValIndex::new(&repo, last_commit, schema, &field)?;
            let indices = reader.get_indices("cat")?;

            assert!(indices.is_none());

            Ok(())
        })
    }

    #[test]
    fn test_insert_index() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = api::local::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schemas = command::schemas::list(&repo, Some(&last_commit.id))?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();

            // As if we aggregated up the "label" field and "dog" was a value
            let field = schema::Field {
                name: String::from("label"),
                dtype: String::from("str"),
            };

            let index = SchemaFieldValIndex::new(&repo, last_commit, schema, &field)?;

            let indices: Vec<u32> = vec![4, 3, 2, 5, 1];
            index.insert_index("cat", indices)?;

            let indices = index.get_indices("cat")?.unwrap();
            assert_eq!(indices.len(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_insert_large_index() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = api::local::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schemas = command::schemas::list(&repo, Some(&last_commit.id))?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();

            // As if we aggregated up the "label" field and "dog" was a value
            let field = schema::Field {
                name: String::from("label"),
                dtype: String::from("str"),
            };

            let index = SchemaFieldValIndex::new(&repo, last_commit, schema, &field)?;

            // Large insert failed when working with the bytevec lib, so had to write some custom code
            let num_indices: u32 = 1000000;
            let mut indices: Vec<u32> = vec![];
            for i in 0..num_indices {
                indices.push(i);
            }
            index.insert_index("cat", indices)?;

            let indices = index.get_indices("cat")?.unwrap();
            assert_eq!(indices.len(), num_indices as usize);

            Ok(())
        })
    }
}
