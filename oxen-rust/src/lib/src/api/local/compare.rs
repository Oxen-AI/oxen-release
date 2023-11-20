use polars::datatypes::BooleanChunked;

use crate::constants::TARGETS_HASH_COL;
use crate::core::df::tabular::{self, any_val_to_bytes};
use crate::error::OxenError;
use crate::model::compare::tabular_compare::TabularCompare;
use crate::model::compare::tabular_compare_summary::TabularCompareSummary;

use crate::model::{Commit, LocalRepository, Schema, CommitEntry};
use crate::opts::DFOpts;

use crate::view::schema::SchemaWithPath;
use crate::view::JsonDataFrame;
use crate::{api, util};

use polars::prelude::ChunkCompare;
use polars::prelude::{DataFrame, DataFrameJoinOps};
use std::path::{Path, PathBuf};

pub fn compare_files(
    repo: &LocalRepository,
    entry_1: CommitEntry,
    entry_2: CommitEntry,
    keys: Vec<String>,
    targets: Vec<String>,
    randomize: bool,
    opts: DFOpts, 
) -> Result<TabularCompare, OxenError> {
    // Assert that the files exist in their respective commits and are tabular.
    let version_file_1 = api::local::diff::get_version_file_from_commit_id(repo, &entry_1.commit_id, &entry_1.path)?;
    let version_file_2 = api::local::diff::get_version_file_from_commit_id(repo, &entry_2.commit_id, &entry_2.path)?;

    if !util::fs::is_tabular(&version_file_1) || !util::fs::is_tabular(&version_file_2) {
        return Err(OxenError::invalid_file_type(format!(
            "Compare not supported for non-tabular files, found {:?} and {:?}",
            entry_1.path, 
            entry_2.path
        )));
    }

    // Read DFs and get schemas
    let df_1 = tabular::read_df(&version_file_1, DFOpts::empty())?;
    let df_2 = tabular::read_df(&version_file_2, DFOpts::empty())?;

    let schema_1 = Schema::from_polars(&df_1.schema());
    let schema_2 = Schema::from_polars(&df_2.schema());


    // Subset dataframes to "keys" and "targets"
    let required_fields = keys
        .iter()
        .chain(targets.iter())
        .map(|field| field.clone())
        .collect::<Vec<String>>();

    // Make sure both dataframes have all required fields
    if !schema_1.has_field_names(&required_fields) {
        return Err(OxenError::InvalidSchema(Box::new(schema_1)));
    }

    if !schema_2.has_field_names(&required_fields) {
        return Err(OxenError::InvalidSchema(Box::new(schema_2)));
    }

    let keys = keys.iter().map(|key| key.as_str()).collect::<Vec<&str>>();
    let targets = targets
        .iter()
        .map(|target| target.as_str())
        .collect::<Vec<&str>>();

    let compare = compute_row_comparison(df_1, df_2, &entry_1.path, &entry_2.path, keys, targets, randomize, opts)?;

    Ok(compare)
}

fn compute_row_comparison(
    df_1: DataFrame, // TODONOW: probably make these mut 
    df_2: DataFrame,
    path_1: &Path,
    path_2: &Path,
    keys: Vec<&str>,
    targets: Vec<&str>,
    randomize: bool,
    opts: DFOpts,
) -> Result<TabularCompare, OxenError> {
    const COMPARE_SLICE_SIZE: usize = 100;

    let og_schema_1 = SchemaWithPath {
        path: path_1.as_os_str().to_str().map(|s| s.to_owned()).unwrap(),
        schema: Schema::from_polars(&df_1.schema()),
    };
    
    let og_schema_2 =  SchemaWithPath {
        path: path_2.as_os_str().to_str().map(|s| s.to_owned()).unwrap(),
        schema: Schema::from_polars(&df_2.schema()),
    };


    // TODO: unsure if hash comparison or join is faster here - would guess join, could use some testing
    let joined_df = hash_and_join_dfs(df_1, df_2, keys.clone(), targets.clone())?;

    let df1_unique = joined_df.filter(
        &joined_df
            .column(format!("{}.right", targets[0]).as_str())?
            .is_null(),
    )?;

    let df2_unique = joined_df.filter(
        &joined_df
            .column(format!("{}.left", targets[0]).as_str())?
            .is_null(),
    )?;

    let diff_df = calculate_diff_df(&joined_df, targets.clone(), keys.clone())?;
    let match_df = calculate_match_df(&joined_df, targets.clone(), keys.clone())?;

    println!("different targets are {:?}", diff_df);
    println!("same targets are {:?}", match_df);
    println!("df1 unique are {:?}", df1_unique);
    println!("df2 unique are {:?}", df2_unique);

    
    let different_targets_size = diff_df.height();
    let same_targets_size = match_df.height();

    let diff_view = generate_df_view(diff_df, randomize, Some(COMPARE_SLICE_SIZE))?;
    let match_view = generate_df_view(match_df, randomize, Some(COMPARE_SLICE_SIZE))?;


    // Print different_targets with only the columns in rename_cols with .right and .left 
    let summary = TabularCompareSummary {
        num_left_only_rows: df1_unique.height(),
        num_right_only_rows: df2_unique.height(),
        num_diff_rows: different_targets_size,
        num_match_rows: same_targets_size,
    };

    // TODONOW: Paginate?
    // TODONOW: view?
    let match_rows = JsonDataFrame::from_df_opts(match_view, opts.clone());
    let diff_rows = JsonDataFrame::from_df_opts(diff_view, opts.clone());

    let tabular_compare = TabularCompare {
        summary,
        schema_left: Some(og_schema_1),
        schema_right: Some(og_schema_2),
        keys: keys
            .iter()
            .map(|key| key.to_string())
            .collect::<Vec<String>>(),
        targets: targets
            .iter()
            .map(|target| target.to_string())
            .collect::<Vec<String>>(),
        match_rows: Some(match_rows),
        diff_rows: Some(diff_rows),
    };

    Ok(tabular_compare)
}

fn hash_and_join_dfs(mut left_df: DataFrame, mut right_df: DataFrame, keys: Vec<&str>, targets: Vec<&str>) -> Result<DataFrame, OxenError> {
    const TARGETS_HASH_COL: &str = "_targets_hash";
    const KEYS_HASH_COL: &str = "_keys_hash";
    
    // Subset to only targets and keys - also checks that these are present
    let out_fields = keys
        .iter()
        .chain(targets.iter())
        .map(|&field| field)
        .collect::<Vec<&str>>();

    left_df = left_df.select(&out_fields)?;
    right_df = right_df.select(&out_fields)?;

    // Generate hash columns for target set and key set 
    left_df = tabular::df_hash_rows_on_cols(left_df, targets.clone(), TARGETS_HASH_COL)?;
    right_df = tabular::df_hash_rows_on_cols(right_df, targets.clone(), TARGETS_HASH_COL)?;

    left_df = tabular::df_hash_rows_on_cols(left_df, keys.clone(), KEYS_HASH_COL)?;
    right_df = tabular::df_hash_rows_on_cols(right_df, keys.clone(), KEYS_HASH_COL)?;

    let mut joined_df = left_df.outer_join(&right_df, keys.clone(), keys.clone())?;

    // Rename columns to .left and .right suffixes. 

    let mut cols_to_rename = targets.clone();
    cols_to_rename.push(TARGETS_HASH_COL);

    for target in cols_to_rename.iter() {
        let left_before = format!("{}", target);
        let left_after = format!("{}.left", target);
        let right_before = format!("{}_right", target);
        let right_after = format!("{}.right", target);
        joined_df.rename(&left_before, &left_after)?;
        joined_df.rename(&right_before, &right_after)?;
    }

    Ok(joined_df)
}

fn calculate_diff_df(df: &DataFrame, targets: Vec<&str>, keys: Vec<&str>) -> Result<DataFrame, OxenError> {
    let diff_mask = df
        .column(format!("{}.left", TARGETS_HASH_COL).as_str())?
        .not_equal(df.column(format!("{}.right", TARGETS_HASH_COL).as_str())?)?;
    let diff_df = df.filter(&diff_mask)?;

    let mut cols_to_keep: Vec<String> = keys
        .iter()
        .map(|&field| field.to_string())
        .collect::<Vec<String>>();

    for target in targets.iter() {
        cols_to_keep.push(format!("{}.left", target));
        cols_to_keep.push(format!("{}.right", target));
    }


    Ok(diff_df.select(&cols_to_keep)?)
}

fn calculate_match_df(df: &DataFrame, targets: Vec<&str>, keys: Vec<&str>) -> Result<DataFrame, OxenError> {
    let match_mask = df
        .column(format!("{}.left", TARGETS_HASH_COL).as_str())?
        .equal(df.column(format!("{}.right", TARGETS_HASH_COL).as_str())?)?;

    let mut match_df = df.filter(&match_mask)?;

    for target in targets.iter() {
        let left_before = format!("{}.left", target);
        let left_after = format!("{}", target);
        match_df.rename(&left_before, &left_after)?;
    }

    let cols_to_keep = keys
        .iter()
        .chain(targets.iter())
        .map(|&field| field)
        .collect::<Vec<&str>>();

    Ok(match_df.select(&cols_to_keep)?)
}

// TODONOW: Should be able to replace this with DFOpts
fn generate_df_view(df: DataFrame, random: bool, limit: Option<usize>) -> Result<DataFrame, OxenError> {
    if random {
        Ok(df.sample_n(limit.unwrap_or(100), false, true, None)?)
    } else {
        Ok(df.slice(0, limit.unwrap_or(100)))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use jwalk::WalkDir;

    use crate::api;
    use crate::command;
    use crate::command::df;
    use crate::error::OxenError;
    use crate::model::diff::diff_entry_status::DiffEntryStatus;
    use crate::opts::RmOpts;
    use crate::test;
    use crate::util;
    use crate::opts::DFOpts;

    #[test]
    fn test_compare_fails_when_not_tabular() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed(|repo| {
            let hello_file = repo.path.join("Hello.txt");
            let world_file = repo.path.join("World.txt");
            test::write_txt_file_to_path(&hello_file, "Hello")?;
            test::write_txt_file_to_path(&world_file, "World")?;

            command::add(&repo, &hello_file)?;
            command::add(&repo, &world_file)?;

            command::commit(&repo, "adding_new_files")?;

            let head_commit = api::local::commits::head_commit(&repo)?;

            let keys = vec![];
            let targets = vec![];

            let entry_left = api::local::entries::get_commit_entry(&repo, &head_commit, &PathBuf::from("Hello.txt"))?.unwrap();
            let entry_right = api::local::entries::get_commit_entry(&repo, &head_commit, &PathBuf::from("World.txt"))?.unwrap();

            let result = api::local::compare::compare_files(
                &repo,
                entry_left, 
                entry_right,
                keys,
                targets,
                false,
                DFOpts::empty(),
            );

            assert!(matches!(
                result.unwrap_err(),
                OxenError::InvalidFileType(_)
            ));

            Ok(())
        })
    }

    #[test]
    fn test_compare_file_to_itself() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed(|repo| {

            // Debug log all files in the `repo` directory 
            let path = Path::new("annotations").join("train").join("bounding_box.csv");

            let head_commit = api::local::commits::head_commit(&repo)?;

            let entry = api::local::entries::get_commit_entry(&repo, &head_commit, &path)?.unwrap();

            let keys = vec!["file".to_string()];
            let targets: Vec<String> = vec!["label", "width", "height"].iter()
                .map(|&s| String::from(s))
                .collect();

            let result = api::local::compare::compare_files(
                &repo,
                entry.clone(), 
                entry,
                keys,
                targets,
                false,
                DFOpts::empty(),
            )?;

            assert_eq!(result.summary.num_left_only_rows, 0);
            assert_eq!(result.summary.num_right_only_rows, 0);
            assert_eq!(result.summary.num_diff_rows, 0);
            assert_eq!(result.summary.num_match_rows, 6);
            Ok(())
        })
    }
}
