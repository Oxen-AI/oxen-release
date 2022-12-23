use crate::db::{self, str_json_db};
use crate::error::OxenError;
use crate::model::schema::Field;
use crate::model::{Commit, Schema};
use crate::util;

use crate::model::LocalRepository;
use rocksdb::{DBWithThreadMode, MultiThreaded};

use super::SchemaIndexReader;

pub struct SchemaIndexWriter {
    field_indices_db: DBWithThreadMode<MultiThreaded>,
}

impl SchemaIndexWriter {
    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
        schema: &Schema,
    ) -> Result<SchemaIndexWriter, OxenError> {
        let db_path = SchemaIndexReader::field_indices_db_dir(repository, commit, schema);

        let opts = db::opts::default();
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }

        Ok(SchemaIndexWriter {
            field_indices_db: DBWithThreadMode::open(&opts, &db_path)?,
        })
    }

    pub fn create_field_index(&self, field: &Field) -> Result<(), OxenError> {
        let hashed_name = util::hasher::hash_str(&field.name);
        str_json_db::put(&self.field_indices_db, hashed_name, field)
    }

    pub fn delete_field_index(&self, field: &Field) -> Result<(), OxenError> {
        let hashed_name = util::hasher::hash_str(&field.name);
        str_json_db::delete(&self.field_indices_db, hashed_name)
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::{SchemaIndexReader, SchemaIndexWriter};
    use crate::model::schema;
    use crate::test;

    #[test]
    fn test_schema_index_writer_create_index() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let last_commit = history.first().unwrap();
            let schemas = command::schema_list(&repo, Some(&last_commit.id))?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();

            let field = schema::Field {
                name: String::from("label"),
                dtype: String::from("str"),
            };
            {
                let writer = SchemaIndexWriter::new(&repo, last_commit, schema)?;
                writer.create_field_index(&field)?;
            }

            let reader = SchemaIndexReader::new(&repo, last_commit, schema)?;
            let schemas = reader.list_field_indices()?;
            assert_eq!(schemas.len(), 1);

            let result = reader.get_field_index_by_name(&field.name)?.unwrap();
            assert_eq!(result.name, field.name);

            Ok(())
        })
    }
}
