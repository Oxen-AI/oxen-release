use crate::constants::{FIELDS_DIR, HISTORY_DIR, INDICES_DIR};
use crate::db;
use crate::db::str_val_db;
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
        val_hash: &str,
    ) -> PathBuf {
        let key_hash = util::hasher::hash_str(&field.name);
        // .oxen/history/COMMIT_ID/indices/SCHEMA_HASH/fields/FIELD_NAME_HASH/FIELD_VAL_HASH
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(&commit.id)
            .join(INDICES_DIR)
            .join(&schema.hash)
            .join(FIELDS_DIR)
            .join(key_hash)
            .join(val_hash)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
        schema: &Schema,
        field: &Field,
        val_hash: &str,
    ) -> Result<SchemaFieldValIndex, OxenError> {
        let db_path = SchemaFieldValIndex::db_dir(repository, commit, schema, field, val_hash);
        log::debug!("SchemaFieldValIndex db {:?}", db_path);
        let opts = db::opts::default();
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }

        Ok(SchemaFieldValIndex {
            field_indices_db: DBWithThreadMode::open(&opts, &db_path)?,
        })
    }

    pub fn insert_index<S: AsRef<str>>(&self, row_hash: S, index: u32) -> Result<(), OxenError> {
        str_val_db::put(&self.field_indices_db, row_hash, &index)
    }

    pub fn list_indices(&self) -> Result<Vec<u32>, OxenError> {
        str_val_db::list_vals(&self.field_indices_db)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::index::SchemaFieldValIndex;
    use crate::model::schema;
    use crate::test;
    use crate::{command, util};

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
            let val = "dog";
            // technically don't have to do this for test...but it's what we do irl
            let val_hash = util::hasher::hash_str(val);

            let reader = SchemaFieldValIndex::new(&repo, last_commit, schema, &field, &val_hash)?;
            let indices = reader.list_indices()?;

            assert_eq!(indices.len(), 0);

            Ok(())
        })
    }
}
