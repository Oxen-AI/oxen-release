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
use crate::df::{tabular, DFOpts};
use crate::error::OxenError;
use crate::index::{SchemaFieldValIndex, SchemaIndexReader};
use crate::model::{schema, Commit, CommitEntry, DataFrameDiff, LocalRepository, Schema};
use crate::util;

use super::SchemaReader;

/// TODO: Rename this to ContentAddressableDataFrame or something
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
        commit_id: &str,
        schema: &Schema,
        path: &Path,
    ) -> PathBuf {
        // .oxen/history/COMMIT_ID/indices/SCHEMA_HASH/files/FILE_NAME_HASH
        let filename = path.to_str().unwrap();
        log::debug!("CommitSchemaRowIndex hashing filename {}", filename);
        let file_name_hash = util::hasher::hash_buffer(filename.as_bytes());
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(INDICES_DIR)
            .join(&schema.hash)
            .join(FILES_DIR)
            .join(file_name_hash)
    }

    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
        schema: &Schema,
        path: &Path,
    ) -> Result<CommitSchemaRowIndex, OxenError> {
        let row_db_path = CommitSchemaRowIndex::row_db_path(repository, schema);
        let file_db_path = CommitSchemaRowIndex::file_db_path(repository, commit_id, schema, path);
        log::debug!("CommitSchemaRowIndex new dir row_db_path {:?}", row_db_path);
        if !row_db_path.exists() {
            std::fs::create_dir_all(&row_db_path)?;
        }

        log::debug!(
            "CommitSchemaRowIndex new dir file_db_path {:?}",
            file_db_path
        );
        if !file_db_path.exists() {
            std::fs::create_dir_all(&file_db_path)?;
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

    pub fn get_global_idx<S: AsRef<str>>(&self, hash: S) -> Result<Option<u32>, OxenError> {
        str_val_db::get(&self.row_db, hash)
    }

    pub fn has_file_key<S: AsRef<str>>(&self, hash: S) -> bool {
        str_val_db::has_key(&self.file_db, hash)
    }

    pub fn get_file_indices<S: AsRef<str>>(&self, hash: S) -> Result<Option<Vec<u32>>, OxenError> {
        db::index_db::get_indices(&self.file_db, hash)
    }

    /// Write just the global index to the row db
    pub fn put_row_index<S: AsRef<str>>(&self, hash: S, index: u32) -> Result<(), OxenError> {
        str_val_db::put(&self.row_db, &hash, &index)
    }

    /// When inserting vec<u32>
    /// global index = first index in vec
    /// local indices = remaining indices
    pub fn put_file_index<S: AsRef<str>>(
        &self,
        hash: S,
        indices: Vec<u32>,
    ) -> Result<(), OxenError> {
        let key = hash.as_ref();
        // log::debug!("put_file_index [{:?}] -> {:?}", key, indices);
        let encoded = db::index_db::u32_to_u8(indices);
        self.file_db.put(key, encoded)?;
        Ok(())
    }

    pub fn list_global_indices(&self) -> Result<Vec<(String, u32)>, OxenError> {
        str_val_db::list(&self.row_db)
    }

    pub fn list_file_indices(&self) -> Result<Vec<(String, Vec<u32>)>, OxenError> {
        db::index_db::list_indices(&self.file_db)
    }

    pub fn list_file_indices_hash_map(&self) -> Result<HashMap<String, Vec<u32>>, OxenError> {
        db::index_db::hash_map_indices(&self.file_db)
    }

    fn select_cols(fields: &[schema::Field]) -> Vec<Expr> {
        // Start with _row_hash and _row_num columns
        let mut cols = vec![col(ROW_HASH_COL_NAME), col(ROW_NUM_COL_NAME)];
        for field in fields.iter() {
            cols.push(col(&field.name));
        }
        cols
    }

    fn select_apply(
        repository: LocalRepository,
        commit: Commit,
        schema: Schema,
        path: PathBuf,
        num_rows: i64,
        fields: &[schema::Field],
    ) -> Expr {
        let indices: Vec<SchemaFieldValIndex> = fields
            .iter()
            .map(|field| SchemaFieldValIndex::new(&repository, &commit, &schema, field).unwrap())
            .collect();
        let cols = CommitSchemaRowIndex::select_cols(fields);

        as_struct(&cols)
            .apply(
                move |s| {
                    log::debug!("select_apply: series {:?}", s);

                    let indexer =
                        CommitSchemaRowIndex::new(&repository, &commit.id, &schema, &path).unwrap();
                    let pb = ProgressBar::new(num_rows as u64);
                    // downcast to struct

                    let ca = s.struct_()?;
                    // the first two fields from select_cols are _row_hash and _row_num
                    let s_row_hash = &ca.fields()[0];
                    let s_row_num = &ca.fields()[1];

                    // downcast the `Series` to their known type
                    let ca_row_hash = s_row_hash.utf8()?;
                    let ca_row_num = s_row_num.u32()?;

                    // TODO: This seems inefficient..can we iterate just once over all cols?
                    if ca.fields().len() > 2 {
                        for i in 2..ca.fields().len() {
                            let s_val = &ca.fields()[2].utf8()?;

                            let mut agg: HashMap<&str, Vec<u32>> = HashMap::new();
                            let _out: UInt32Chunked = s_val
                                .into_iter()
                                .zip(ca_row_num)
                                .map(|(opt_val, opt_row_num)| match (opt_val, opt_row_num) {
                                    (Some(row_val), Some(row_num)) => {
                                        // log::debug!("Saving index [{}] -> {}", row_num, row_val);

                                        agg.entry(row_val).or_default().push(row_num);

                                        Some(row_num)
                                    }
                                    _ => None,
                                })
                                .collect();

                            // log::debug!("Got agg {:?}", agg);
                            for (key, vals) in agg {
                                indices[i - 2].insert_index(key, vals).unwrap();
                            }
                        }
                    }

                    // iterate both `ChunkedArrays`
                    let out: Utf8Chunked = ca_row_hash
                        .into_iter()
                        .zip(ca_row_num)
                        .map(
                            |(opt_row_hash, opt_row_num)| match (opt_row_hash, opt_row_num) {
                                (Some(row_hash), Some(row_num)) => {
                                    pb.inc(1);

                                    // log::debug!("Saving row hash {} -> {}", row_hash, row_num);
                                    indexer.put_row_index(row_hash, row_num).unwrap();

                                    Some(row_hash)
                                }
                                _ => None,
                            },
                        )
                        .collect();
                    Ok(out.into_series())
                },
                GetOutput::from_type(DataType::Utf8),
            )
            .alias("_result")
    }

    fn select_opts(
        repository: LocalRepository,
        commit: Commit,
        schema: Schema,
        path: PathBuf,
        num_rows: i64,
        fields: &[schema::Field],
    ) -> Vec<Expr> {
        let mut cols = CommitSchemaRowIndex::select_cols(fields);
        cols.push(CommitSchemaRowIndex::select_apply(
            repository, commit, schema, path, num_rows, fields,
        ));
        cols
    }

    pub fn index_hash_row_nums(
        repository: LocalRepository,
        commit: Commit,
        schema: Schema,
        path: PathBuf,
        df: DataFrame,
    ) -> Result<DataFrame, OxenError> {
        let num_rows = df.height() as i64;

        let index_reader = SchemaIndexReader::new(&repository, &commit, &schema)?;
        let index_fields = index_reader.list_field_indices()?;

        // Save off hash->row_idx to db
        let df = df
            .lazy()
            .select(CommitSchemaRowIndex::select_opts(
                repository,
                commit,
                schema,
                path,
                num_rows,
                &index_fields,
            ))
            .select([col("_result")])
            .collect()
            .unwrap();
        log::debug!("index_hash_row_nums {}", df);
        Ok(df)
    }

    // This function is nasty, I know
    // TODO: Cleanup and refactor to be more efficient
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
        log::debug!("Compute new rows for file {:?}", entry.path);
        log::debug!("FILTER DOWN TO {:?}", col_names);

        // Save off hash->row_idx to db
        let df = new_df
            .lazy()
            .select([
                all(),
                as_struct(&[col(ROW_HASH_COL_NAME), col(ROW_NUM_COL_NAME)])
                    .apply(
                        move |s| {
                            // log::debug!("s: {:?}", s);

                            let indexer = CommitSchemaRowIndex::new(
                                &repository,
                                &commit.id,
                                &schema,
                                &entry.path,
                            )
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
                                        // log::debug!("compute_new_rows Checking if we have row {} hash: {}", row_num, row_hash);
                                        pb.inc(1);

                                        // Check if the row hash is in the global CADF
                                        match indexer.get_global_idx(row_hash) {
                                            Ok(Some(global_idx)) => {
                                                // log::debug!("GOT IT: {}, {}", row_hash, global_idx);
                                                // If it is in the global CADF, check if it is in the local file
                                                match indexer.get_file_indices(row_hash) {
                                                    Ok(Some(mut indices)) => {
                                                        // If local file has row_hash, and we don't already have row_num tracked, append this new local row_num
                                                        let mut has_row_num = false;
                                                        for i in indices.iter().skip(1) {
                                                            if i == &row_num {
                                                                has_row_num = true;
                                                            }
                                                        }
                                                        if !has_row_num {
                                                            indices.push(row_num);
                                                        }
                                                        // log::debug!("compute_new_rows APPENDING file global {} indices {:?}", row_num, indices);
                                                        indexer
                                                            .put_file_index(row_hash, indices)
                                                            .unwrap();
                                                    }
                                                    Ok(None) => {
                                                        // If local file does not have it, start with [global_idx, local_idx]
                                                        let indices = vec![global_idx, row_num];
                                                        // log::debug!("compute_new_rows FIRST file {} indices {:?}", row_num, indices);
                                                        indexer
                                                            .put_file_index(row_hash, indices)
                                                            .unwrap();
                                                    }
                                                    Err(err) => {
                                                        panic!(
                                                            "Error computing new rows... {}",
                                                            err
                                                        );
                                                    }
                                                }
                                                Some(false)
                                            }
                                            Ok(None) => {
                                                // It is not in the global CADF, so it will not be in local either
                                                // Compute global idx based off of old size and current row idx
                                                let global_idx = old_num_rows + num_new;
                                                let indices = vec![global_idx, row_num];
                                                // log::debug!("compute_new_rows first CADF {} indices {:?}", row_num, indices);
                                                indexer
                                                    .put_file_index(row_hash, indices)
                                                    .unwrap();
                                                num_new += 1;
                                                Some(true)
                                            }
                                            Err(err) => {
                                                panic!("Error computing new rows... {}", err);
                                            }
                                        }
                                    }
                                    _ => None,
                                })
                                .collect();
                            // log::debug!("Got series: {:?}", out);
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
        path: &Path,
    ) -> Result<DataFrameDiff, OxenError> {
        log::debug!(
            "diff_current look at other commit {:?} for path {:?}",
            commit,
            path
        );
        let other = CommitSchemaRowIndex::new(repo, &commit.id, schema, path)?;

        // Read the current data frame
        log::debug!("diff_current going to read path {:?}", path);
        let current_path = repo.path.join(path);
        let current_df = tabular::read_df(&current_path, DFOpts::empty())?;

        // Path to CADF
        let content_addressable_df_path = util::fs::schema_df_path(repo, schema);

        // Check to make sure the schemas match, ie column changes
        let schema_current = Schema::from_polars(&current_df.schema());
        if schema.hash != schema_current.hash {
            let added_fields = schema_current.added_fields(schema);
            let removed_fields = schema_current.removed_fields(schema);

            let added_cols = if !added_fields.is_empty() {
                let opts = DFOpts::from_filter_fields(added_fields);
                let df_added = tabular::read_df(&current_path, opts)?;
                log::debug!("Got added col df: {}", df_added);
                if df_added.width() > 0 {
                    Some(df_added)
                } else {
                    None
                }
            } else {
                None
            };

            let removed_cols = if !removed_fields.is_empty() {
                let opts = DFOpts::from_filter_fields(removed_fields);
                // Read CDAF
                let df_removed = tabular::read_df(&content_addressable_df_path, opts)?;
                log::debug!("Got removed col df: {}", df_removed);
                if df_removed.width() > 0 {
                    Some(df_removed)
                } else {
                    None
                }
            } else {
                None
            };

            return Ok(DataFrameDiff {
                added_rows: None,
                removed_rows: None,
                added_cols,
                removed_cols,
            });
        }

        // Read the saved off CADF (Content Addressable Data Frame) for this schema
        let content_df = tabular::scan_df(&content_addressable_df_path)?;

        // Hash the current df rows
        let current_df = tabular::df_hash_rows(current_df)?;

        // log::debug!("diff_current got current hashes {}", current_df);

        let current_hash_indices: HashMap<String, u32> = current_df
            .column(constants::ROW_HASH_COL_NAME)
            .unwrap()
            .utf8()
            .unwrap()
            .into_iter()
            .enumerate()
            .map(|(i, v)| (v.unwrap().to_string(), i as u32))
            .collect();
        // log::debug!("diff_current current indices {:?}", current_hash_indices);

        let other_hash_indices = other.list_file_indices_hash_map()?;
        // log::debug!("diff_current other indices {:?}", other_hash_indices);

        // Added is all the row hashes that are in current that are not in other
        let added_indices: Vec<u32> = current_hash_indices
            .iter()
            .filter(|(hash, _indices)| !other_hash_indices.contains_key(*hash))
            .map(|(_hash, index_pair)| *index_pair)
            .collect();

        // Removed is all the row hashes that are in other that are not in current
        let removed_indices: Vec<u32> = other_hash_indices
            .iter()
            .filter(|(hash, _indices)| !current_hash_indices.contains_key(*hash))
            .map(|(_hash, idx_list)| idx_list[0])
            .collect();

        // log::debug!("diff_current added_indices {:?}", added_indices);

        // log::debug!("diff_current removed_indices {:?}", removed_indices);

        // Take added from the added df
        let opts = DFOpts::from_filter_schema(schema);
        let current_df = tabular::transform_df(current_df.lazy(), opts)?;
        let added_rows = tabular::take(current_df.lazy(), added_indices)?;

        // Take removed from CADF (Content Addressable Data Frame)
        let opts = DFOpts::from_filter_schema(schema);
        let content_df = tabular::transform_df(content_df, opts)?;
        let removed_rows = tabular::take(content_df.lazy(), removed_indices)?;

        Ok(DataFrameDiff {
            added_rows: if added_rows.height() > 0 {
                Some(added_rows)
            } else {
                None
            },
            removed_rows: if removed_rows.height() > 0 {
                Some(removed_rows)
            } else {
                None
            },
            added_cols: None,
            removed_cols: None,
        })
    }

    pub fn df_from_entry(
        repo: &LocalRepository,
        entry: &CommitEntry,
    ) -> Result<DataFrame, OxenError> {
        let schema_reader = SchemaReader::new(repo, &entry.commit_id)?;
        let schema = schema_reader.get_schema_for_file(&entry.path)?.unwrap();
        let reader = CommitSchemaRowIndex::new(repo, &entry.commit_id, &schema, &entry.path)?;
        reader.entry_df()
    }

    pub fn entry_df(&self) -> Result<DataFrame, OxenError> {
        // Sort by the original file row num
        let sorted = self.sorted_entry_df_with_row_hash()?;
        log::debug!("entry_df got sorted: {}", sorted);
        // Filter down to the original columns
        let opts = DFOpts::from_filter_schema(&self.schema);
        tabular::transform_df(sorted.lazy(), opts)
    }

    pub fn sorted_entry_df_with_row_hash(&self) -> Result<DataFrame, OxenError> {
        // Get large arrow file
        let path = util::fs::schema_df_path(&self.repository, &self.schema);
        let version_df = tabular::scan_df(path)?.collect().unwrap();
        log::debug!("sorted_entry_df_with_row_hash cadf {:?}", version_df);

        let file_indices: Vec<Vec<u32>> = self
            .list_file_indices()?
            .into_iter()
            .map(|(_hash, indices)| indices)
            .collect();

        log::debug!(
            "sorted_entry_df_with_row_hash file_indices {:?}",
            file_indices
        );

        let global_indices: Vec<u32> = file_indices
            .clone()
            .into_iter()
            .flat_map(|mut indices| {
                let global_idx = indices.remove(0);
                let mut v: Vec<u32> = vec![];
                for _ in 0..indices.len() {
                    v.push(global_idx);
                }
                v
            })
            .collect();
        log::debug!("file_indices global {:?}", global_indices);

        let local_indices: Vec<u32> = file_indices
            .into_iter()
            .flat_map(|mut indices| {
                indices.remove(0);
                indices
            })
            .collect();

        log::debug!("file_indices local {:?}", local_indices);

        // Project the original file row nums on in a column
        let mut subset = tabular::take(version_df.lazy(), global_indices)?;
        log::debug!("got subset {}", subset);

        let file_column_name = "_file_row_num";
        let column = polars::prelude::Series::new(file_column_name, local_indices);
        let with_og_row_nums = subset
            .with_column(column)
            .expect("Could not project row num cols");

        // Sort by the original file row num
        let sorted = with_og_row_nums
            .sort([file_column_name], false)
            .expect("Could sort df");
        Ok(sorted)
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::df::{tabular, DFOpts};
    use crate::error::OxenError;
    use crate::index::CommitSchemaRowIndex;
    use crate::model::schema::Field;
    use crate::test;
    use crate::util;

    use std::path::Path;

    #[test]
    fn test_commit_tabular_data_first_commit_can_fetch_content() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Create a new data file with some annotations
            let og_bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let og_bbox_path = repo.path.join(&og_bbox_file);
            let og_df = tabular::read_df(&og_bbox_path, DFOpts::empty())?;

            // Add & commit
            command::add(&repo, og_bbox_path)?;
            let commit = command::commit(&repo, "Adding bbox training file")?.unwrap();

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas.first().unwrap();

            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(og_df.height(), version_df.height());

            let row_index_reader =
                CommitSchemaRowIndex::new(&repo, &commit.id, schema, &og_bbox_file)?;
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
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();
            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();

            // CADF
            let path = util::fs::schema_df_path(&repo, schema);
            let og_df = tabular::read_df(&path, DFOpts::empty())?;

            let my_bbox_file = Path::new("annotations")
                .join("train")
                .join("my_bounding_box.csv");
            let my_bbox_path = repo.path.join(&my_bbox_file);
            // The first two rows are duplicate, the third is new data, but should be able
            // To get back to a dataframe that has the same content
            test::write_txt_file_to_path(
                &my_bbox_path,
                r#"
file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
train/dog_2.jpg,dog,7.0,29.5,246,247
train/new.jpg,new,1.0,1.5,100,20
"#,
            )?;

            let my_df = tabular::read_df(&my_bbox_path, DFOpts::empty())?;
            command::add(&repo, &my_bbox_path)?;
            let commit =
                command::commit(&repo, "Committing my bbox data, to append onto og data")?.unwrap();

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();

            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(og_df.height() + 1, version_df.height());

            let row_index_reader =
                CommitSchemaRowIndex::new(&repo, &commit.id, schema, &my_bbox_file)?;
            let rows = row_index_reader.entry_df()?;
            println!("Reconstructed {}", rows);

            assert_eq!(my_df.height(), rows.height());

            Ok(())
        })
    }

    #[test]
    fn test_tabular_diff_added_rows() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let last_commit = commits.first().unwrap();

            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);
            test::append_line_txt_file(bbox_path, "train/cat_3.jpg,cat,41.0,31.5,410,427")?;

            let schemas = command::schema_list(&repo, None)?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();
            let diff = CommitSchemaRowIndex::diff_current(&repo, schema, last_commit, &bbox_file)?;

            // Make sure there is only added rows
            assert!(diff.added_rows.is_some());
            assert!(diff.removed_rows.is_none());
            assert!(diff.added_cols.is_none());
            assert!(diff.removed_cols.is_none());
            // Make sure they are the correct shape
            let added_row = diff.added_rows.unwrap();
            assert_eq!(added_row.height(), 1);
            assert_eq!(added_row.width(), 6);

            Ok(())
        })
    }

    #[test]
    fn test_tabular_diff_added_cols() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let last_commit = commits.first().unwrap();

            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);

            // Add a column to the data
            let df = tabular::scan_df(&bbox_path)?;
            let df = tabular::add_col(df, "is_fluffy", "unknown", "str")?;
            let mut df = df.collect().unwrap();
            tabular::write_df(&mut df, &bbox_path)?;

            let schemas = command::schema_list(&repo, None)?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();
            let diff = CommitSchemaRowIndex::diff_current(&repo, schema, last_commit, &bbox_file)?;

            // Make sure there is only added columns
            assert!(diff.added_rows.is_none());
            assert!(diff.removed_rows.is_none());
            assert!(diff.added_cols.is_some());
            assert!(diff.removed_cols.is_none());
            // Make sure they are the correct shape
            let added = diff.added_cols.unwrap();
            assert_eq!(added.height(), 6);
            assert_eq!(added.width(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_tabular_diff_removed_rows() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let last_commit = commits.first().unwrap();

            let bbox_file = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = test::modify_txt_file(
                bbox_file,
                r"
file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
train/dog_2.jpg,dog,7.0,29.5,246,247
train/cat_2.jpg,cat,30.5,44.0,333,396
",
            )?;

            let relative = util::fs::path_relative_to_dir(&bbox_file, &repo.path)?;
            let schemas = command::schema_list(&repo, None)?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();
            let diff = CommitSchemaRowIndex::diff_current(&repo, schema, last_commit, &relative)?;

            // Make sure there is only removed rows
            assert!(diff.removed_rows.is_some());
            let removed_row = diff.removed_rows.unwrap();
            println!("Got removed: {}", removed_row);

            assert!(diff.added_rows.is_none());
            assert!(diff.added_cols.is_none());
            assert!(diff.removed_cols.is_none());

            // Make sure we found the multiple removed rows
            assert_eq!(removed_row.height(), 3);
            assert_eq!(removed_row.width(), 6);
            Ok(())
        })
    }

    #[test]
    fn test_tabular_diff_removed_cols() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let last_commit = commits.first().unwrap();

            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);

            // Remove columns from the data
            let fields = vec![
                Field {
                    name: String::from("file"),
                    dtype: String::from("str"),
                },
                Field {
                    name: String::from("min_x"),
                    dtype: String::from("f64"),
                },
                Field {
                    name: String::from("min_y"),
                    dtype: String::from("f64"),
                },
            ];
            let opts = DFOpts::from_filter_fields(fields);
            let mut df = tabular::read_df(&bbox_path, opts)?;
            tabular::write_df(&mut df, &bbox_path)?;

            let schemas = command::schema_list(&repo, None)?;
            let schema = schemas
                .iter()
                .find(|s| s.name.as_ref().unwrap() == "bounding_box")
                .unwrap();
            let diff = CommitSchemaRowIndex::diff_current(&repo, schema, last_commit, &bbox_file)?;

            // Make sure there is only added columns
            assert!(diff.added_rows.is_none());
            assert!(diff.removed_rows.is_none());
            assert!(diff.added_cols.is_none());
            assert!(diff.removed_cols.is_some());
            // Make sure they are the correct shape
            let removed = diff.removed_cols.unwrap();
            assert_eq!(removed.height(), 9);
            assert_eq!(removed.width(), 3);

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_data_first_commit_does_save_indexed_fields() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Create a new data file with some annotations
            let og_bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let og_bbox_path = repo.path.join(&og_bbox_file);
            let og_df = tabular::read_df(&og_bbox_path, DFOpts::empty())?;

            // Add & commit
            command::add(&repo, og_bbox_path)?;
            let commit = command::commit(&repo, "Adding bbox training file")?.unwrap();

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas.first().unwrap();

            // Create the index
            command::schema_create_index(&repo, &schema.hash, "label")?;

            // Make sure cadf is created
            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(og_df.height(), version_df.height());

            // Add & commit another file
            let new_bbox_file = Path::new("annotations")
                .join("test")
                .join("annotations.csv");
            let new_bbox_path = repo.path.join(&new_bbox_file);

            command::add(&repo, new_bbox_path)?;
            command::commit(&repo, "Adding bbox test file")?.unwrap();

            let result = command::schema_query_index(&repo, &schema.hash, "label", "unknown")?;

            // There should be one new field indexed with label=="unknown"
            assert_eq!(result.height(), 1);

            Ok(())
        })
    }
}
