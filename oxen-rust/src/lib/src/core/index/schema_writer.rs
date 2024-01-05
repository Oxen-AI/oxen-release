use crate::core::db::str_json_db;
use crate::core::db::{self, str_val_db};
use crate::error::OxenError;
use crate::model::Schema;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::Path;
use std::str;

use crate::core::index::SchemaReader;
use crate::model::LocalRepository;

use super::versioner;

pub struct SchemaWriter {
    db: DBWithThreadMode<MultiThreaded>,
    files_db: DBWithThreadMode<MultiThreaded>,
    repository: LocalRepository,
}

impl SchemaWriter {
    pub fn new(repository: &LocalRepository, commit_id: &str) -> Result<SchemaWriter, OxenError> {
        let db_path = SchemaReader::schemas_db_dir(repository, commit_id);
        let opts = db::opts::default();
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
        }

        let schema_files_db_path = SchemaReader::schema_files_db_dir(repository, commit_id);
        if !schema_files_db_path.exists() {
            std::fs::create_dir_all(&schema_files_db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&schema_files_db_path))?;
        }

        Ok(SchemaWriter {
            db: DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?,
            files_db: DBWithThreadMode::open(&opts, dunce::simplified(&schema_files_db_path))?,
            repository: repository.clone(),
        })
    }

    // pub fn put_schema_for_file(&self, path: &Path, schema: &Schema) -> Result<(), OxenError> {
    //     str_val_db::put(&self.files_db, path.to_string_lossy(), &schema.hash)?;
    //     str_json_db::put(&self.db, &schema.hash, schema)
    // }

    pub fn put_schema_for_file(&self, _path: &Path, schema: &Schema) -> Result<(), OxenError> {
        // All we want to do is make sure the schema is inserted into the versions directory by hash.
        versioner::backup_schema(&self.repository, schema)?;
        Ok(())
    }

    // pub fn delete_schema_for_file(&self, path: &Path, schema: &Schema) -> Result<(), OxenError> {
    //     str_val_db::delete(&self.files_db, path.to_string_lossy())?;
    //     str_json_db::delete(&self.db, &schema.hash)
    // }

    pub fn delete_schema_for_file(&self, _path: &Path, schema: &Schema) -> Result<(), OxenError> {
        // We don't need this at all

        Ok(())
    }

    // TODONOW:: ??
    pub fn has_schema(&self, schema: &Schema) -> bool {
        str_json_db::has_key(&self.db, &schema.hash)
    }

    // pub fn put_schema(&self, schema: &Schema) -> Result<(), OxenError> {
    //     str_json_db::put(&self.db, &schema.hash, schema)
    // }

    pub fn put_schema(&self, schema: &Schema) -> Result<(), OxenError> {
        // All we want to do is make sure the schema is inserted into the versions directory by hash.
        versioner::backup_schema(&self.repository, schema)?;
        Ok(())
    }

    // pub fn update_schema(&self, schema: &Schema) -> Result<Schema, OxenError> {
    //     str_json_db::put(&self.db, &schema.hash, schema)?;
    //     Ok(str_json_db::get(&self.db, &schema.hash)?.unwrap())
    // }

    pub fn update_schema(&self, schema: &Schema) -> Result<Schema, OxenError> {
        // All we want to do is make sure the schema is inserted into the versions directory by hash.
        versioner::backup_schema(&self.repository, schema)?;
        Ok(schema.clone())
    }

    // pub fn delete_schema(&self, schema: &Schema) -> Result<(), OxenError> {
    //     str_json_db::delete(&self.db, &schema.hash)?;
    //     Ok(())
    // }

    pub fn delete_schema(&self, schema: &Schema) -> Result<(), OxenError> {
        // We don't need this at all

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::core::index::SchemaReader;
    use crate::core::index::SchemaWriter;
    use crate::error::OxenError;
    use crate::model::schema;
    use crate::model::Schema;
    use crate::test;

    #[test]
    fn test_put_schema_for_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let history = api::local::commits::list(&repo)?;
            let last_commit = history.first().unwrap();

            {
                let schema_writer = SchemaWriter::new(&repo, &last_commit.id)?;

                let schema = Schema::from_fields(vec![
                    schema::Field::new("label", "str"),
                    schema::Field::new("min_x", "int"),
                    schema::Field::new("min_y", "int"),
                ]);

                schema_writer.put_schema_for_file(Path::new("test.csv"), &schema)?;
            }

            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;
            let schemas = schema_reader.list_schemas()?;
            assert_eq!(schemas.len(), 1);

            Ok(())
        })
    }
}
