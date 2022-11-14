use crate::constants::{FIELDS_DIR, HISTORY_DIR, INDICES_DIR};
use crate::db;
use crate::error::OxenError;
use crate::model::{schema::Field, Commit, Schema};
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::mem;
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
            field_indices_db: DBWithThreadMode::open(&opts, &db_path)?,
        })
    }

    pub fn insert_index<S: AsRef<str>>(
        &self,
        key: S,
        mut indices: Vec<u32>,
    ) -> Result<(), OxenError> {
        let key = key.as_ref();

        // Could not use the bytevec library here when inserting a larger set of indices
        let byte_indices = unsafe {
            let ratio = mem::size_of::<u32>() / mem::size_of::<u8>();

            let length = indices.len() * ratio;
            let capacity = indices.capacity() * ratio;
            let ptr = indices.as_mut_ptr() as *mut u8;

            // Don't run the destructor for vec32
            mem::forget(indices);

            // Construct new Vec
            Vec::from_raw_parts(ptr, length, capacity)
        };

        self.field_indices_db.put(key, byte_indices)?;
        Ok(())
    }

    fn from_u8(v: Vec<u8>) -> Vec<u32> {
        let data = v.as_ptr();
        let len = v.len();
        let capacity = v.capacity();
        let element_size = mem::size_of::<u32>();

        // Make sure we have a proper amount of capacity (may be overkill)
        assert_eq!(capacity % element_size, 0);
        // Make sure we are going to read a full chunk of stuff
        assert_eq!(len % element_size, 0);

        unsafe {
            // Don't allow the current vector to be dropped
            // (which would invalidate the memory)
            mem::forget(v);

            Vec::from_raw_parts(
                data as *mut u32,
                len / element_size,
                capacity / element_size,
            )
        }
    }

    pub fn list_indices<S: AsRef<str>>(&self, key: S) -> Result<Option<Vec<u32>>, OxenError> {
        let key = key.as_ref();
        let bytes = key.as_bytes();
        match self.field_indices_db.get(bytes) {
            Ok(Some(raw_indices)) => {
                // found it
                let decoded = SchemaFieldValIndex::from_u8(raw_indices);

                Ok(Some(decoded))
            }
            Ok(None) => {
                // did not get val
                Ok(None)
            }
            Err(err) => {
                // error from the DB
                let err = format!("Err could not fetch value {:?} from db: {:?}", key, err,);
                Err(OxenError::basic_str(err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::SchemaFieldValIndex;
    use crate::model::schema;
    use crate::test;

    #[test]
    fn test_list_empty_indices() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let last_commit = history.first().unwrap();
            let schemas = command::schema_list(&repo, Some(&last_commit.id))?;
            let schema = schemas.first().unwrap();

            // As if we aggregated up the "label" field and "dog" was a value
            let field = schema::Field {
                name: String::from("label"),
                dtype: String::from("str"),
            };

            let reader = SchemaFieldValIndex::new(&repo, last_commit, schema, &field)?;
            let indices = reader.list_indices("cat")?;

            assert!(indices.is_none());

            Ok(())
        })
    }

    #[test]
    fn test_insert_index() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let last_commit = history.first().unwrap();
            let schemas = command::schema_list(&repo, Some(&last_commit.id))?;
            let schema = schemas.first().unwrap();

            // As if we aggregated up the "label" field and "dog" was a value
            let field = schema::Field {
                name: String::from("label"),
                dtype: String::from("str"),
            };

            let index = SchemaFieldValIndex::new(&repo, last_commit, schema, &field)?;

            let indices: Vec<u32> = vec![4, 3, 2, 5, 1];
            index.insert_index("cat", indices)?;

            let indices = index.list_indices("cat")?.unwrap();
            assert_eq!(indices.len(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_insert_large_index() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let last_commit = history.first().unwrap();
            let schemas = command::schema_list(&repo, Some(&last_commit.id))?;
            let schema = schemas.first().unwrap();

            // As if we aggregated up the "label" field and "dog" was a value
            let field = schema::Field {
                name: String::from("label"),
                dtype: String::from("str"),
            };

            let index = SchemaFieldValIndex::new(&repo, last_commit, schema, &field)?;

            let num_indices: u32 = 1000000;
            let mut indices: Vec<u32> = vec![];
            for i in 0..num_indices {
                indices.push(i);
            }
            index.insert_index("cat", indices)?;

            let indices = index.list_indices("cat")?.unwrap();
            assert_eq!(indices.len(), num_indices as usize);

            Ok(())
        })
    }
}
