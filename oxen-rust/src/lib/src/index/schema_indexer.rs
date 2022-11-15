use indicatif::ProgressBar;
use polars::prelude::{col, AnyValue, DataFrame, Expr};
use rayon::prelude::*;

use crate::index::SchemaIndexWriter;
use crate::{
    constants::ROW_NUM_COL_NAME,
    df::tabular,
    error::OxenError,
    index::SchemaFieldValIndex,
    model::{schema, Commit, LocalRepository, Schema},
    util,
};

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

        println!("Indexing field: {}", field.name);
        // Aggregate up all _row_num indices on that field
        let agg_results = content_df
            .groupby([col(&field.name)])
            .agg([col(ROW_NUM_COL_NAME)])
            .collect()
            .unwrap();

        // println!("Got aggregation: {}", agg_results);
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

        // One index per value that was aggregated
        // ie: dog, cat, person
        let val_index =
            SchemaFieldValIndex::new(&self.repository, &self.commit, &self.schema, field)?;

        let bar = ProgressBar::new(agg_values.len() as u64);

        agg_values
            .iter()
            .zip(agg_row_indices.iter())
            .par_bridge()
            .for_each(|(val, indices)| {
                bar.inc(1);
                // Convert val to a string we can insert into db
                let val = match val {
                    AnyValue::Utf8(val) => val.to_string(),
                    val => {
                        format!("{}", val)
                    }
                };

                // Loop over each set of indices and hashes in Series and add them to index
                match indices {
                    AnyValue::List(indices) => {
                        let indices = indices.u32().unwrap();
                        let result: Vec<u32> = indices
                            .into_iter()
                            .map(|opt_index| match opt_index {
                                Some(index) => index,
                                _ => {
                                    panic!("Invalid value zipped...");
                                }
                            })
                            .collect();
                        val_index.insert_index(val, result).unwrap();
                    }
                    _ => {
                        panic!("Aggregation must be list...");
                    }
                }
            });
        bar.finish();

        // Keep track of the field that is being indexed
        let writer = SchemaIndexWriter::new(&self.repository, &self.commit, &self.schema)?;
        writer.create_field_index(field)?;
        println!("Done.");

        Ok(())
    }

    /// Query the index on a field and value
    pub fn query<S: AsRef<str>>(
        &self,
        field: &schema::Field,
        query: S,
    ) -> Result<Option<DataFrame>, OxenError> {
        // Get the CADF
        let df_path = util::fs::schema_df_path(&self.repository, &self.schema);
        let content_df = tabular::scan_df(&df_path)?;

        let mut cols: Vec<Expr> = vec![];
        for f in &self.schema.fields {
            cols.push(col(&f.name));
        }
        let content_df = content_df.select(cols);

        // let query_hash = util::hasher::hash_str(query);

        let val_index =
            SchemaFieldValIndex::new(&self.repository, &self.commit, &self.schema, field)?;

        if let Some(indices) = val_index.list_indices(&query)? {
            let df = tabular::take(content_df, indices)?;
            Ok(Some(df))
        } else {
            Ok(None)
        }
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

            let results = indexer.query(label_field, "cat")?.unwrap();
            println!("Got index query results: {}", results);
            assert_eq!(results.width(), 6);
            assert_eq!(results.height(), 3);

            Ok(())
        })
    }
}
