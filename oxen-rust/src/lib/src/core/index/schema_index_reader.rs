use crate::constants::{HISTORY_DIR, INDICES_DIR};
use crate::core::db;
use crate::core::db::str_json_db;
use crate::error::OxenError;
use crate::model::{schema, Commit, Schema};
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::PathBuf;
use std::str;

use crate::model::LocalRepository;

pub struct SchemaIndexReader {
    field_indices_db: DBWithThreadMode<MultiThreaded>,
}

impl SchemaIndexReader {
    pub fn field_indices_db_dir(
        repo: &LocalRepository,
        commit: &Commit,
        schema: &Schema,
    ) -> PathBuf {
        // .oxen/history/COMMIT_ID/indices/SCHEMA_HASH/indices
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(&commit.id)
            .join(INDICES_DIR)
            .join(&schema.hash)
            .join(INDICES_DIR)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
        schema: &Schema,
    ) -> Result<SchemaIndexReader, OxenError> {
        let field_indices_db_path =
            SchemaIndexReader::field_indices_db_dir(repository, commit, schema);
        log::debug!("SchemaIndexReader db {:?}", field_indices_db_path);
        let opts = db::opts::default();
        if !field_indices_db_path.exists() {
            std::fs::create_dir_all(&field_indices_db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&field_indices_db_path))?;
        }

        Ok(SchemaIndexReader {
            field_indices_db: DBWithThreadMode::open_for_read_only(
                &opts,
                &field_indices_db_path,
                false,
            )?,
        })
    }

    /// See if a field is indexed
    pub fn field_index_exists<S: AsRef<str>>(&self, name: S) -> bool {
        let hashed_name = util::hasher::hash_str(name);
        str_json_db::has_key(&self.field_indices_db, hashed_name)
    }

    /// Get a field index by id
    pub fn get_field_index_by_name<S: AsRef<str>>(
        &self,
        name: S,
    ) -> Result<Option<schema::Field>, OxenError> {
        let hashed_name = util::hasher::hash_str(name);
        str_json_db::get(&self.field_indices_db, hashed_name)
    }

    /// List all the fields that are indexed
    pub fn list_field_indices(&self) -> Result<Vec<schema::Field>, OxenError> {
        str_json_db::list_vals(&self.field_indices_db)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::core::index::SchemaIndexReader;
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

            let index_reader = SchemaIndexReader::new(&repo, last_commit, schema)?;
            let indices = index_reader.list_field_indices()?;

            assert_eq!(indices.len(), 0);

            Ok(())
        })
    }
}
