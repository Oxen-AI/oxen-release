use indicatif::ProgressBar;
use polars::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};

use crate::constants::{self, HISTORY_DIR, INDICES_DIR, ROW_HASH_COL_NAME, ROW_NUM_COL_NAME};
use crate::db;
use crate::db::str_val_db;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, Schema};
use crate::util;

pub struct CommitSchemaTableIndex {
    db: DBWithThreadMode<MultiThreaded>,
}

impl CommitSchemaTableIndex {
    pub fn db_path(path: &Path, commit: &Commit, schema: &Schema, name: &str) -> PathBuf {
        // .oxen/history/COMMIT_ID/indices/SCHEMA_HASH/name
        util::fs::oxen_hidden_dir(path)
            .join(HISTORY_DIR)
            .join(&commit.id)
            .join(INDICES_DIR)
            .join(&schema.hash)
            .join(name)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
        schema: &Schema,
        name: &str,
    ) -> Result<CommitSchemaTableIndex, OxenError> {
        let db_path = CommitSchemaTableIndex::db_path(&repository.path, commit, schema, name);
        log::debug!("CommitSchemaTableIndex new dir db_path {:?}", db_path);
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }
        let opts = db::opts::default();
        Ok(CommitSchemaTableIndex {
            db: DBWithThreadMode::open(&opts, &db_path)?,
        })
    }

    pub fn hash_key<S: AsRef<str>>(&self, hash: S) -> bool {
        str_val_db::has_key(&self.db, hash)
    }

    pub fn put<S: AsRef<str>>(&self, hash: S, index: u32) -> Result<(), OxenError> {
        str_val_db::put(&self.db, hash, &index)
    }

    pub fn list(&self) -> Result<Vec<(String, u64)>, OxenError> {
        str_val_db::list(&self.db)
    }

    pub fn index_hash_row_nums(
        repository: LocalRepository,
        commit: Commit,
        schema: Schema,
        name: String,
        df: DataFrame,
    ) -> Result<DataFrame, OxenError> {
        let num_rows = df.width() as i64;

        // Save off hash->row_idx to db
        let df = df
            .lazy()
            .select([
                col(ROW_HASH_COL_NAME),
                col(ROW_NUM_COL_NAME),
                as_struct(&[col(ROW_HASH_COL_NAME), col(ROW_NUM_COL_NAME)])
                    .apply(
                        move |s| {
                            // log::debug!("s: {:?}", s);

                            let indexer =
                                CommitSchemaTableIndex::new(&repository, &commit, &schema, &name)
                                    .unwrap();
                            let pb = ProgressBar::new(num_rows as u64);
                            // downcast to struct

                            let ca = s.struct_()?;
                            // get the fields as Series
                            let s_a = &ca.fields()[0];
                            let s_b = &ca.fields()[1];

                            // downcast the `Series` to their known type
                            let ca_a = s_a.utf8()?;
                            let ca_b = s_b.u32()?;

                            // iterate both `ChunkedArrays`
                            let out: Utf8Chunked = ca_a
                                .into_iter()
                                .zip(ca_b)
                                .map(|(opt_a, opt_b)| match (opt_a, opt_b) {
                                    (Some(row_hash), Some(row_num)) => {
                                        pb.inc(1);
                                        indexer.put(row_hash, row_num).unwrap();
                                        Some(row_hash)
                                    }
                                    _ => None,
                                })
                                .collect();
                            Ok(out.into_series())
                        },
                        GetOutput::from_type(DataType::Utf8),
                    )
                    .alias("_result"),
            ])
            .select([col("_result")])
            .collect()
            .unwrap();
        println!("{}", df);
        Ok(df)
    }

    pub fn compute_new_rows(
        repository: LocalRepository,
        commit: Commit,
        schema: Schema,
        name: String,
        df: DataFrame,
    ) -> Result<DataFrame, OxenError> {
        let num_rows = df.width() as i64;

        let mut col_names = vec![];
        for field in df.schema().iter_fields() {
            if field.name() != constants::ROW_NUM_COL_NAME {
                col_names.push(col(field.name()));
            }
        }
        log::debug!("FILTER DOWN TO {:?}", col_names);

        // Save off hash->row_idx to db
        let df = df
            .lazy()
            .select([
                all(),
                as_struct(&[col(ROW_HASH_COL_NAME)])
                    .apply(
                        move |s| {
                            log::debug!("s: {:?}", s);

                            let indexer =
                                CommitSchemaTableIndex::new(&repository, &commit, &schema, &name)
                                    .unwrap();
                            let pb = ProgressBar::new(num_rows as u64);
                            // downcast to struct
                            let ca = s.struct_()?;
                            // get the fields as Series
                            let s_a = &ca.fields()[0];

                            // downcast the `Series` to their known type
                            let ca_a = s_a.utf8()?;

                            // iterate both `ChunkedArrays`
                            let out: BooleanChunked = ca_a
                                .into_iter()
                                .map(|opt_a| match opt_a {
                                    Some(row_hash) => {
                                        log::debug!("Checking if we have hash: {}", row_hash);
                                        pb.inc(1);
                                        if indexer.hash_key(row_hash) {
                                            log::debug!("GOT IT: {}", row_hash);
                                            Some(false)
                                        } else {
                                            Some(true)
                                        }
                                    }
                                    _ => None,
                                })
                                .collect();
                            log::debug!("Got series: {:?}", out);
                            Ok(out.into_series())
                        },
                        GetOutput::from_type(DataType::Boolean),
                    )
                    .alias("_is_new"),
            ])
            .filter(col("_is_new").eq(true))
            .select(&col_names)
            .collect()
            .unwrap();
        // println!("NEW ROWS: {}", df);
        Ok(df)
    }
}
