use polars::prelude::{col, AnyValue, DataFrame};

use crate::{
    constants::{ROW_HASH_COL_NAME, ROW_NUM_COL_NAME},
    df::tabular,
    error::OxenError,
    index::SchemaFieldValIndex,
    model::{schema, Commit, LocalRepository, Schema},
    util,
};

use super::SchemaIndexWriter;

pub struct SchemaIndexer {
    repository: LocalRepository,
    commit: Commit,
    schema: Schema,
}

impl SchemaIndexer {
    pub fn new(repo: &LocalRepository, commit: &Commit, schema: &Schema) -> SchemaIndexer {
        SchemaIndexer {
            repository: repo.clone(),
            commit: commit.clone(),
            schema: schema.clone(),
        }
    }

    /// Create an index on a field
    pub fn create_index(&self, field: &schema::Field) -> Result<(), OxenError> {
        // Get the data frame of the schema
        let df_path = util::fs::schema_df_path(&self.repository, &self.schema);
        let content_df = tabular::scan_df(&df_path)?;

        // Check to make sure the field exists in the schema
        let df_schema = Schema::from_polars(&content_df.schema().unwrap());
        if !df_schema.has_field(field) {
            return Err(OxenError::schema_does_not_have_field(&field.name));
        }

        // Aggregate up all values on that field
        let agg_results = content_df
            .groupby([col(&field.name)])
            .agg([col(ROW_NUM_COL_NAME), col(ROW_HASH_COL_NAME)])
            .collect()
            .unwrap();

        log::debug!("Got aggregation: {}", agg_results);
        // agg_values are the grouped value
        // ex) group_by(label)
        //       0: dog
        //       1: cat
        //       2: person
        let agg_values = agg_results.column(&field.name).unwrap();

        // agg_row_indices are lists of the grouped row indices
        // ex) group_by(label)
        //       0: [0, 2, 4]
        //       1: [1, 2]
        //       2: [5, 6]
        let agg_row_indices = agg_results.column(ROW_NUM_COL_NAME).unwrap();

        // agg_row_hashes are lists of the grouped row hashes, each is same len as the row_indices
        // ex) group_by(label)
        //       0: [87ceb166ad0313730525609380942fd1, a72d61871a0647761adfff20b24ee97b, 7ed97d42afb074edf98d459d8d618606]
        //       1: [306a4bff4f48082376bd17b411b7f667, ed60d2c75cf8945f079bb3856d22dcdf]
        //       2: [7297adf68c254b0345307a83d18370ef, 47ceb166ad0313730525609380942fd1]
        let agg_row_hashes = agg_results.column(ROW_HASH_COL_NAME).unwrap();

        for (i, val) in agg_values.iter().enumerate() {
            let buffer = tabular::any_val_to_bytes(&val);
            let val_hash = util::hasher::hash_buffer(&buffer);

            // One index per value that was aggregated
            // ie: dog, cat, person
            let val_index = SchemaFieldValIndex::new(
                &self.repository,
                &self.commit,
                &self.schema,
                field,
                &val_hash,
            )?;

            // Loop over each set of indices and hashes in Series and add them to index
            match (agg_row_hashes.get(i), agg_row_indices.get(i)) {
                (AnyValue::List(hashes), AnyValue::List(indices)) => {
                    let hashes = hashes.utf8().unwrap();
                    let indices = indices.u32().unwrap();

                    let _result: Vec<Result<(), OxenError>> = hashes
                        .into_iter()
                        .zip(indices.into_iter())
                        .map(|(opt_hash, opt_index)| match (opt_hash, opt_index) {
                            (Some(hash), Some(index)) => {
                                // Add to index
                                val_index.insert_index(hash, index)
                            }
                            _ => {
                                panic!("Invalid types zipped...");
                            }
                        })
                        .collect();
                }
                _ => {
                    panic!("Aggregation must be list...");
                }
            }
        }

        // Keep track of the field that is being indexed
        let writer = SchemaIndexWriter::new(&self.repository, &self.commit, &self.schema)?;
        writer.create_field_index(field)?;

        Ok(())
    }

    /// Query the index on a field and value
    pub fn query<S: AsRef<str>>(
        &self,
        field: &schema::Field,
        query: S,
    ) -> Result<DataFrame, OxenError> {
        // Get the CADF
        let df_path = util::fs::schema_df_path(&self.repository, &self.schema);
        let content_df = tabular::scan_df(&df_path)?;

        let query_hash = util::hasher::hash_str(query);

        let val_index = SchemaFieldValIndex::new(
            &self.repository,
            &self.commit,
            &self.schema,
            field,
            &query_hash,
        )?;

        let indices = val_index.list_indices()?;
        let df = tabular::take(content_df, indices)?;

        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::SchemaIndexer;
    use crate::test;

    #[test]
    fn test_schema_indexer_create_index_query_results() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let last_commit = history.first().unwrap();
            let schemas = command::schema_list(&repo, Some(&last_commit.id))?;
            let schema = schemas.first().unwrap();

            let label_field = schema.fields.iter().find(|f| f.name == "label").unwrap();

            let indexer = SchemaIndexer::new(&repo, last_commit, schema);
            indexer.create_index(label_field)?;

            let results = indexer.query(label_field, "cat")?;
            println!("Got index query results: {}", results);
            assert_eq!(results.width(), 8);
            assert_eq!(results.height(), 2);

            Ok(())
        })
    }
}
