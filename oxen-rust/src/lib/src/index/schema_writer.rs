use crate::db::str_json_db;
use crate::db::{self, str_val_db};
use crate::error::OxenError;
use crate::model::Schema;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::Path;
use std::str;

use crate::index::SchemaReader;
use crate::model::LocalRepository;

pub struct SchemaWriter {
    db: DBWithThreadMode<MultiThreaded>,
    files_db: DBWithThreadMode<MultiThreaded>,
}

impl SchemaWriter {
    pub fn new(repository: &LocalRepository, commit_id: &str) -> Result<SchemaWriter, OxenError> {
        let db_path = SchemaReader::schemas_db_dir(repository, commit_id);
        let opts = db::opts::default();
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open(&opts, &db_path)?;
        }

        let schema_files_db_path = SchemaReader::schema_files_db_dir(repository, commit_id);
        if !schema_files_db_path.exists() {
            std::fs::create_dir_all(&schema_files_db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, &schema_files_db_path)?;
        }

        Ok(SchemaWriter {
            db: DBWithThreadMode::open(&opts, &db_path)?,
            files_db: DBWithThreadMode::open(&opts, &schema_files_db_path)?,
        })
    }

    pub fn put_schema_for_file(&self, path: &Path, schema: &Schema) -> Result<(), OxenError> {
        str_val_db::put(&self.files_db, path.to_str().unwrap(), &schema.hash)
    }

    pub fn put_schema(&self, schema: &Schema) -> Result<(), OxenError> {
        str_json_db::put(&self.db, &schema.hash, schema)
    }

    pub fn update_schema(&self, schema: &Schema) -> Result<Schema, OxenError> {
        str_json_db::put(&self.db, &schema.hash, schema)?;
        Ok(str_json_db::get(&self.db, &schema.hash)?.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::SchemaReader;
    use crate::index::SchemaWriter;
    use crate::model::schema;
    use crate::model::Schema;
    use crate::test;

    #[test]
    fn test_put_schema() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let history = command::log(&repo)?;
            let last_commit = history.first().unwrap();

            {
                let schema_writer = SchemaWriter::new(&repo, &last_commit.id)?;

                let schema = Schema::from_fields(vec![
                    schema::Field {
                        name: String::from("label"),
                        dtype: String::from("str"),
                    },
                    schema::Field {
                        name: String::from("min_x"),
                        dtype: String::from("int"),
                    },
                    schema::Field {
                        name: String::from("min_y"),
                        dtype: String::from("int"),
                    },
                ]);

                schema_writer.put_schema(&schema)?;
            }

            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;
            let schemas = schema_reader.list_schemas()?;
            assert_eq!(schemas.len(), 1);

            Ok(())
        })
    }
}
