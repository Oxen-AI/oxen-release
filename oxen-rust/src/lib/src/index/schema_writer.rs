use crate::db;
use crate::error::OxenError;
use crate::index::kv_json_db;
use crate::model::Schema;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::str;

use crate::index::SchemaReader;
use crate::model::LocalRepository;

pub struct SchemaWriter {
    db: DBWithThreadMode<MultiThreaded>,
}

impl SchemaWriter {
    /// Create a new reader that can find commits, list history, etc
    pub fn new(repository: &LocalRepository, commit_id: &str) -> Result<SchemaWriter, OxenError> {
        let db_path = SchemaReader::db_dir(repository, commit_id);
        let opts = db::opts::default();
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open(&opts, &db_path)?;
        }

        Ok(SchemaWriter {
            db: DBWithThreadMode::open(&opts, &db_path)?,
        })
    }

    pub fn add_schema(&self, schema: &Schema) -> Result<(), OxenError> {
        kv_json_db::put(&self.db, &schema.hash, schema)
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
    fn test_add_schema() -> Result<(), OxenError> {
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

                schema_writer.add_schema(&schema)?;
            }

            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;
            let schemas = schema_reader.list()?;
            assert_eq!(schemas.len(), 1);

            Ok(())
        })
    }
}
