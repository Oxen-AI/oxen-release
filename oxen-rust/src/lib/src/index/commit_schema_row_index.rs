use indicatif::ProgressBar;
use polars::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::constants::{
    self, FILES_DIR, HISTORY_DIR, INDICES_DIR, ROWS_DIR, ROW_HASH_COL_NAME, ROW_NUM_COL_NAME,
};
use crate::db;
use crate::db::str_val_db;
use crate::error::OxenError;
use crate::media::{tabular, DFOpts};
use crate::model::{Commit, CommitEntry, LocalRepository, Schema, DataFrameDiff};
use crate::util;

/// indices is a tuple that represents (row_num_og,row_num_versioned)
/// 1) row_num_og = row number in the original file, so that we can restore properly
/// 2) row_num_versioned = global row number in the row content hashed arrow file for the schema
pub type RowIndexPair = (u32, u32);

pub struct CommitSchemaRowIndex {
    row_db: DBWithThreadMode<MultiThreaded>,  // global row hashes
    file_db: DBWithThreadMode<MultiThreaded>, // file level row hashes
    schema: Schema,
    repository: LocalRepository,
}

// TODO: Split this into two different classes, one for the global row index, and one for the
// Commit entry row index
impl CommitSchemaRowIndex {
    pub fn row_db_path(repo: &LocalRepository, schema: &Schema) -> PathBuf {
        // .oxen/versions/schemas/SCHEMA_HASH/rows
        util::fs::schema_version_dir(repo, schema).join(ROWS_DIR)
    }

    pub fn file_db_path(
        repo: &LocalRepository,
        commit: &Commit,
        schema: &Schema,
        path: &Path,
    ) -> PathBuf {
        // .oxen/history/COMMIT_ID/indices/SCHEMA_HASH/files/FILE_NAME_HASH
        let file_name_hash = util::hasher::hash_buffer(path.to_str().unwrap().as_bytes());
        util::fs::oxen_hidden_dir(&repo.path)
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
        path: &Path,
    ) -> Result<CommitSchemaRowIndex, OxenError> {
        let row_db_path = CommitSchemaRowIndex::row_db_path(repository, schema);
        let file_db_path =
            CommitSchemaRowIndex::file_db_path(repository, commit, schema, path);
        log::debug!("CommitSchemaRowIndex new dir row_db_path {:?}", row_db_path);
        if !row_db_path.exists() {
            std::fs::create_dir_all(&row_db_path)?;
        }
        let opts = db::opts::default();
        Ok(CommitSchemaRowIndex {
            row_db: DBWithThreadMode::open(&opts, &row_db_path)?,
            file_db: DBWithThreadMode::open(&opts, &file_db_path)?,
            schema: schema.to_owned(),
            repository: repository.to_owned(),
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

    pub fn list_file_indices_hash_map(&self) -> Result<HashMap<String, RowIndexPair>, OxenError> {
        str_val_db::hash_map(&self.file_db)
    }

    pub fn index_hash_row_nums(
        repository: LocalRepository,
        commit: Commit,
        schema: Schema,
        path: PathBuf,
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
                                CommitSchemaRowIndex::new(&repository, &commit, &schema, &path)
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
                                CommitSchemaRowIndex::new(&repository, &commit, &schema, &entry.path)
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

    pub fn diff_current(
        repo: &LocalRepository,
        schema: &Schema,
        commit: &Commit,
        path: &Path
    ) -> Result<DataFrameDiff, OxenError> {
        let other = CommitSchemaRowIndex::new(repo, commit, schema, path)?;

        let path = repo.path.join(&path);
        let df = tabular::read_df(&path, DFOpts::empty())?;
        let df = tabular::df_hash_rows(df)?;

        let current_hash_indices: HashMap<String, u32> = df.column(constants::ROW_HASH_COL_NAME).unwrap()
            .utf8()
            .unwrap()
            .into_iter()
            .enumerate()
            .map(|(i, v)| 
                (v.unwrap().to_string(), i as u32)
            )
            .collect();

        let other_hash_indices = other.list_file_indices_hash_map()?;

        // Added is all the row hashes that are in current that are not in other
        let added_indices: Vec<u32> = current_hash_indices.iter().filter(|(hash, _indices)| {
            !other_hash_indices.contains_key(*hash)
        })
        .map(|(_hash, index_pair)| index_pair.clone())
        .collect();

        // Removed is all the row hashes that are in other that are not in current
        let removed_indices: Vec<u32> = other_hash_indices
            .iter()
            .filter(|(hash, _indices)| {!current_hash_indices.contains_key(*hash)})
            .map(|(_hash, index_pair)| index_pair.1)
            .collect();
        
        let content_df = tabular::scan_df(path)?;
        let added_df = tabular::take(content_df, added_indices)?;

        let content_df = tabular::scan_df(path)?;
        let removed_df = tabular::take(content_df, removed_indices)?;

        Ok(DataFrameDiff {
            added: added_df,
            removed: removed_df
        })
    }
    
    pub fn diff_commits(
        repo: &LocalRepository,
        schema: &Schema,
        current_commit: &Commit,
        other_commit: &Commit,
        path: &Path
    ) -> Result<DataFrameDiff, OxenError> {
        let current = CommitSchemaRowIndex::new(repo, current_commit, schema, path)?;
        let other = CommitSchemaRowIndex::new(repo, other_commit, schema, path)?;

        let current_hash_indices = current.list_file_indices_hash_map()?;
        let other_hash_indices = other.list_file_indices_hash_map()?;

        // Added is all the row hashes that are in current that are not in other
        let added_indices: Vec<u32> = current_hash_indices.iter().filter(|(hash, _indices)| {
            !other_hash_indices.contains_key(*hash)
        })
        .map(|(_hash, index_pair)| index_pair.1)
        .collect();

        // Removed is all the row hashes that are in other that are not in current
        let removed_indices: Vec<u32> = other_hash_indices
            .iter()
            .filter(|(hash, _indices)| {!current_hash_indices.contains_key(*hash)})
            .map(|(_hash, index_pair)| index_pair.1)
            .collect();
        
        let content_df = tabular::scan_df(path)?;
        let added_df = tabular::take(content_df, added_indices)?;

        let content_df = tabular::scan_df(path)?;
        let removed_df = tabular::take(content_df, removed_indices)?;

        Ok(DataFrameDiff {
            added: added_df,
            removed: removed_df
        })
    }

    pub fn entry_df(&self) -> Result<DataFrame, OxenError> {
        // Get large arrow file
        let path = util::fs::schema_df_path(&self.repository, &self.schema);
        let version_df = tabular::scan_df(path)?.collect().unwrap();
        println!("VERSION DF {:?}", version_df);

        let file_indices: Vec<RowIndexPair> = self
            .list_file_indices()?
            .into_iter()
            .map(|(_hash, indices)| indices)
            .collect();

        println!("file_indices {:?}", file_indices);

        let global_indices: Vec<u32> = file_indices
            .clone()
            .into_iter()
            .map(|(_local_idx, global_idx)| global_idx)
            .collect();

        let local_indices: Vec<u32> = file_indices
            .into_iter()
            .map(|(local_idx, _global_idx)| local_idx)
            .collect();

        println!("file_indices global {:?}", global_indices);
        println!("file_indices local {:?}", local_indices);

        // Project the original file row nums on in a column
        let mut subset = tabular::take(version_df.lazy(), global_indices)?;
        let file_column_name = "_file_row_num";
        let column = polars::prelude::Series::new(file_column_name, local_indices);
        let with_og_row_nums = subset
            .with_column(column)
            .expect("Could not project row num cols");

        // Sort by the original file row num
        let sorted = with_og_row_nums
            .sort([file_column_name], false)
            .expect("Could sort df");
        // Filter down to the original columns
        let opts = DFOpts::from_filter_schema(&self.schema);
        tabular::transform_df(sorted.lazy(), opts)
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::CommitDirReader;
    use crate::index::CommitSchemaRowIndex;
    use crate::media::tabular;
    use crate::media::DFOpts;
    use crate::test;
    use crate::util;

    use std::path::Path;

    #[test]
    fn test_commit_tabular_data_first_commit_can_fetch_content() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let commit = history.first().unwrap();

            // Create a new data file with some annotations
            let og_bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let og_bbox_path = repo.path.join(&og_bbox_file);
            let og_df = tabular::read_df(&og_bbox_path, DFOpts::empty())?;

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas.first().unwrap();

            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(og_df.height(), version_df.height());

            let entry_reader = CommitDirReader::new(&repo, commit)?;

            let row_index_reader = CommitSchemaRowIndex::new(&repo, commit, schema, &og_bbox_file)?;
            let rows = row_index_reader.entry_df()?;
            println!("Reconstructed {}", rows);

            assert_eq!(og_df.height(), rows.height());

            Ok(())
        })
    }

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

            let entry_reader = CommitDirReader::new(&repo, &commit)?;

            let row_index_reader = CommitSchemaRowIndex::new(&repo, &commit, schema, &my_bbox_file)?;
            let rows = row_index_reader.entry_df()?;
            println!("Reconstructed {}", rows);

            assert_eq!(my_df.height(), rows.height());

            Ok(())
        })
    }

    #[test]
    async fn test_tabular_diff_added() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let last_commit = commits.first().unwrap();
            let commit_entry_reader = CommitDirReader::new(&repo, last_commit)?;

            let bbox_file = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file =
                test::append_line_txt_file(bbox_file, "train/cat_3.jpg,41.0,31.5,410,427")?;

            let relative = util::fs::path_relative_to_dir(&bbox_file, &repo.path)?;
            let diff = CommitSchemaRowIndex::diff(&repo, &schema, current_commit, other_commit, bbox_file)?;


            let results = r"
╭─────────────────┬───────┬───────┬───────┬────────╮
│ file            ┆ min_x ┆ min_y ┆ width ┆ height │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/cat_3.jpg ┆ 41    ┆ 31.5  ┆ 410   ┆ 427    │
╰─────────────────┴───────┴───────┴───────┴────────╯
 1 Rows x 5 Columns";

            assert_eq!(results, tabular_datafusion::df_to_str(&diff.added).await?);
            Ok(())
        })
        .await
    }
}
