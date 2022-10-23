use indicatif::ProgressBar;
use polars::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};

use crate::constants::{
    self, FILES_DIR, HISTORY_DIR, INDICES_DIR, ROWS_DIR, ROW_HASH_COL_NAME, ROW_NUM_COL_NAME,
};
use crate::db;
use crate::db::str_val_db;
use crate::error::OxenError;
use crate::media::tabular;
use crate::model::{Commit, CommitEntry, LocalRepository, Schema};
use crate::util;

/// indices is a tuple that represents (row_num_og,row_num_versioned)
/// 1) row_num_og = row number in the original file, so that we can restore properly
/// 2) row_num_versioned = global row number in the row content hashed arrow file for the schema
pub type RowIndexPair = (u32, u32);

pub struct CommitSchemaRowIndex {
    row_db: DBWithThreadMode<MultiThreaded>,  // global row hashes
    file_db: DBWithThreadMode<MultiThreaded>, // file level row hashes
}

impl CommitSchemaRowIndex {
    pub fn row_db_path(repo: &LocalRepository, schema: &Schema) -> PathBuf {
        // .oxen/versions/schemas/SCHEMA_HASH/rows
        util::fs::schema_version_dir(repo, schema).join(ROWS_DIR)
    }

    pub fn file_db_path(
        path: &Path,
        commit: &Commit,
        schema: &Schema,
        entry: &CommitEntry,
    ) -> PathBuf {
        // .oxen/history/COMMIT_ID/indices/SCHEMA_HASH/files/FILE_NAME_HASH
        let file_name_hash = util::hasher::hash_buffer(entry.path.to_str().unwrap().as_bytes());
        util::fs::oxen_hidden_dir(path)
            .join(HISTORY_DIR)
            .join(&commit.id)
            .join(INDICES_DIR)
            .join(&schema.hash)
            .join(FILES_DIR)
            .join(file_name_hash)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
        schema: &Schema,
        entry: &CommitEntry,
    ) -> Result<CommitSchemaRowIndex, OxenError> {
        let row_db_path = CommitSchemaRowIndex::row_db_path(repository, schema);
        let file_db_path =
            CommitSchemaRowIndex::file_db_path(&repository.path, commit, schema, entry);
        log::debug!("CommitSchemaRowIndex new dir row_db_path {:?}", row_db_path);
        if !row_db_path.exists() {
            std::fs::create_dir_all(&row_db_path)?;
        }
        let opts = db::opts::default();
        Ok(CommitSchemaRowIndex {
            row_db: DBWithThreadMode::open(&opts, &row_db_path)?,
            file_db: DBWithThreadMode::open(&opts, &file_db_path)?,
        })
    }

    pub fn has_global_key<S: AsRef<str>>(&self, hash: S) -> bool {
        str_val_db::has_key(&self.row_db, hash)
    }

    pub fn has_file_key<S: AsRef<str>>(&self, hash: S) -> bool {
        str_val_db::has_key(&self.file_db, hash)
    }

    /// Write just the global index to the row db
    pub fn put_row_index<S: AsRef<str>>(&self, hash: S, index: u32) -> Result<(), OxenError> {
        str_val_db::put(&self.row_db, &hash, &index)
    }

    
    pub fn put_file_index<S: AsRef<str>>(
        &self,
        hash: S,
        indices: RowIndexPair,
    ) -> Result<(), OxenError> {
        // Write file level info to the file db
        str_val_db::put(&self.file_db, hash, &indices)
    }

    pub fn list_global_indices(&self) -> Result<Vec<(String, u32)>, OxenError> {
        str_val_db::list(&self.row_db)
    }

    pub fn list_file_indices(&self) -> Result<Vec<(String, RowIndexPair)>, OxenError> {
        str_val_db::list(&self.file_db)
    }

    pub fn diff(
        repository: LocalRepository,
        commit_1: Commit,
        commit_2: Commit,
        schema: Schema,
        entry: &CommitEntry,
    ) -> Result<DataFrame, OxenError> {
        let _index_1 = CommitSchemaRowIndex::new(&repository, &commit_1, &schema, entry);
        let _index_2 = CommitSchemaRowIndex::new(&repository, &commit_2, &schema, entry);

        let schema_df_path = util::fs::schema_df_path(&repository, &schema);
        let df = tabular::scan_df(&schema_df_path)?;

        let indices: Vec<u32> = vec![0, 1];
        let df = tabular::take(df, indices)?;

        Ok(df)
    }

    pub fn index_hash_row_nums(
        repository: LocalRepository,
        commit: Commit,
        schema: Schema,
        entry: CommitEntry,
        df: DataFrame,
    ) -> Result<DataFrame, OxenError> {
        let num_rows = df.height() as i64;

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
                                CommitSchemaRowIndex::new(&repository, &commit, &schema, &entry)
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

                                        log::debug!("Saving row hash: {} -> {}", row_hash, row_num);
                                        indexer.put_row_index(row_hash, row_num).unwrap();

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
        log::debug!("index_hash_row_nums {}", df);
        Ok(df)
    }

    // This function is nasty, I know, but it works and is pretty efficient
    pub fn compute_new_rows(
        repository: LocalRepository,
        commit: Commit,
        schema: Schema,
        entry: CommitEntry,
        new_df: DataFrame,
        old_df: &DataFrame,
    ) -> Result<DataFrame, OxenError> {
        let num_rows = new_df.height() as i64;
        let old_num_rows = old_df.height() as u32;

        let mut col_names = vec![];
        for field in new_df.schema().iter_fields() {
            if field.name() != constants::ROW_NUM_COL_NAME {
                col_names.push(col(field.name()));
            }
        }
        log::debug!("FILTER DOWN TO {:?}", col_names);

        // Save off hash->row_idx to db
        let df = new_df
            .lazy()
            .select([
                all(),
                as_struct(&[col(ROW_HASH_COL_NAME), col(ROW_NUM_COL_NAME)])
                    .apply(
                        move |s| {
                            log::debug!("s: {:?}", s);

                            let indexer =
                                CommitSchemaRowIndex::new(&repository, &commit, &schema, &entry)
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

                            let mut num_new = 0;
                            // iterate both `ChunkedArrays`
                            let out: BooleanChunked = ca_a
                                .into_iter()
                                .zip(ca_b)
                                .map(|(opt_a, opt_b)| match (opt_a, opt_b) {
                                    (Some(row_hash), Some(row_num)) => {
                                        log::debug!("Checking if we have hash: {}", row_hash);
                                        pb.inc(1);
                                        if indexer.has_global_key(row_hash) {
                                            log::debug!("GOT IT: {}", row_hash);
                                            indexer
                                                .put_file_index(row_hash, (row_num, row_num))
                                                .unwrap();
                                            Some(false)
                                        } else {
                                            indexer
                                                .put_file_index(
                                                    row_hash,
                                                    (row_num, old_num_rows + num_new),
                                                )
                                                .unwrap();
                                            num_new += 1;
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

#[cfg(test)]
mod tests {
    use polars::prelude::IntoLazy;

    use crate::command;
    use crate::error::OxenError;
    use crate::index::CommitDirReader;
    use crate::index::CommitSchemaRowIndex;
    use crate::index::commit_schema_row_index::RowIndexPair;
    use crate::media::tabular;
    use crate::media::DFOpts;
    use crate::test;
    use crate::util;

    use std::path::Path;

    #[test]
    fn test_commit_tabular_data_add_data_different_file_can_fetch_file_content(
    ) -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Create a new data file with some annotations
            let og_bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let og_bbox_path = repo.path.join(&og_bbox_file);
            let my_bbox_file = Path::new("annotations")
                .join("train")
                .join("my_bounding_box.csv");
            let my_bbox_path = repo.path.join(&my_bbox_file);
            // The first two rows are duplicate, the third is new data, but should be able
            // To get back to a dataframe that has the same content
            test::write_txt_file_to_path(
                &my_bbox_path,
                r#"
file,min_x,min_y,width,height
train/dog_1.jpg,101.5,32.0,385,330
train/dog_2.jpg,7.0,29.5,246,247
train/new.jpg,1.0,1.5,100,20
"#,
            )?;

            let my_df = tabular::read_df(&my_bbox_path, DFOpts::empty())?;
            let og_df = tabular::read_df(&og_bbox_path, DFOpts::empty())?;
            command::add(&repo, &my_bbox_path)?;
            let commit =
                command::commit(&repo, "Committing my bbox data, to append onto og data")?.unwrap();

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas.first().unwrap();

            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(og_df.height() + 1, version_df.height());

            // TODO write a function that does all this to restore a file
            // THEN project the original file row nums on in a column
            // THEN sort by the original file row num
            // THEN filter down to the original columns
            // THEN write the restored version
            let entry_reader = CommitDirReader::new(&repo, &commit)?;
            let entry = entry_reader.get_entry(&my_bbox_file)?.unwrap();

            let row_index_reader = CommitSchemaRowIndex::new(&repo, &commit, schema, &entry)?;

            let file_indices: Vec<RowIndexPair> = row_index_reader
                .list_file_indices()?
                .into_iter()
                .map(|(_hash, indices)| indices)
                .collect();

            println!("My df {}", my_df);
            println!("Version df {}", version_df);
            println!("file_indices {:?}", file_indices);

            let file_indices: Vec<u32> = file_indices
                .into_iter()
                .map(|(_local_idx, global_idx)| global_idx)
                .collect();

            println!("file_indices global {:?}", file_indices);

            let rows = tabular::take(version_df.lazy(), file_indices)?;
            println!("Reconstructed {}", rows);

            assert_eq!(my_df.height(), rows.height());

            Ok(())
        })
    }
}
