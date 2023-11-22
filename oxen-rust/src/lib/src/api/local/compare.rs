use polars::datatypes::BooleanChunked;

use crate::constants::{TARGETS_HASH_COL, COMPARES_DIR, CACHE_DIR, RIGHT_COMPARE_COMMIT, LEFT_COMPARE_COMMIT};
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
    compare_id: &str,
    entry_1: CommitEntry,
    entry_2: CommitEntry,
    keys: Vec<String>,
    targets: Vec<String>,
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

    let compare = compute_row_comparison(repo, compare_id, df_1, df_2, &entry_1.path, &entry_2.path, keys, targets, opts)?;

    write_compare_commit_ids(repo, compare_id, &entry_1.commit_id, &entry_2.commit_id)?;

    Ok(compare)
}

pub fn get_cached_compare(repo: &LocalRepository, compare_id: &str, left_entry: &CommitEntry, right_entry: &CommitEntry, opts: &DFOpts) -> Result<Option<TabularCompare>, OxenError> {

    // Check if commits have changed since LEFT and RIGHT files were cached
    // TODONOW: need tests for this big time 

    let (cached_left_id, cached_right_id) = get_compare_commit_ids(repo, compare_id)?;

    // If commits cache files do not exist or have changed since last hash (via branch name) then return None to recompute
    if cached_left_id.is_none() || cached_right_id.is_none() {
        return Ok(None);
    }

    if cached_left_id.unwrap() != left_entry.commit_id || cached_right_id.unwrap() != right_entry.commit_id {
        return Ok(None);
    }

    // Get schemas - TODO: after schema population migration, can get these directly from 
    // schemas dbs to avoid loading these into memory
    let left_full_df = tabular::read_df(&api::local::diff::get_version_file_from_commit_id(repo, &left_entry.commit_id, &left_entry.path)?, DFOpts::empty())?;
    let right_full_df = tabular::read_df(&api::local::diff::get_version_file_from_commit_id(repo, &right_entry.commit_id, &right_entry.path)?, DFOpts::empty())?;

    let left_schema = SchemaWithPath {
        schema: Schema::from_polars(&left_full_df.schema()),
        path: left_entry.path.to_str().map(|s| s.to_owned()).unwrap(),
    };

    let right_schema = SchemaWithPath {
        schema: Schema::from_polars(&right_full_df.schema()),
        path: right_entry.path.to_str().map(|s| s.to_owned()).unwrap(),
    };


    let match_df = tabular::read_df(&get_compare_match_path(repo, compare_id), DFOpts::empty())?;
    let diff_df = tabular::read_df(&get_compare_diff_path(repo, compare_id), DFOpts::empty())?;
    let left_df = tabular::read_df(&get_compare_left_path(repo, compare_id), DFOpts::empty())?;
    let right_df = tabular::read_df(&get_compare_right_path(repo, compare_id), DFOpts::empty())?;

    let match_height = match_df.height();
    let diff_height = diff_df.height();

    let match_rows = JsonDataFrame::from_df_opts(match_df, opts.clone());
    let diff_rows = JsonDataFrame::from_df_opts(diff_df, opts.clone());

    let compare = TabularCompare {
        summary: TabularCompareSummary {
            num_left_only_rows: left_df.height(),
            num_right_only_rows: right_df.height(),
            num_diff_rows: diff_height,
            num_match_rows: match_height,
        },
        schema_left: Some(left_schema),
        schema_right: Some(right_schema),
        keys: vec![],
        targets: vec![],
        match_rows: Some(match_rows),
        diff_rows: Some(diff_rows),
    };

    Ok(Some(compare))
}

// TODONOW: Somewhere to relocate this? 

pub fn get_compare_dir(
    repo: &LocalRepository, 
    compare_id: &str, 
) -> PathBuf {
    let compare_dir = util::fs::oxen_hidden_dir(&repo.path)
        .join(CACHE_DIR)
        .join(COMPARES_DIR)
        .join(compare_id);
    compare_dir
}

pub fn get_compare_match_path(
    repo: &LocalRepository, 
    compare_id: &str, 
) -> PathBuf {
    let compare_dir = get_compare_dir(repo, compare_id);
    compare_dir.join("match.parquet")
}

pub fn get_compare_diff_path(
    repo: &LocalRepository, 
    compare_id: &str, 
) -> PathBuf {
    let compare_dir = get_compare_dir(repo, compare_id);
    compare_dir.join("diff.parquet")
}

pub fn get_compare_left_path(
    repo: &LocalRepository, 
    compare_id: &str, 
) -> PathBuf {
    let compare_dir = get_compare_dir(repo, compare_id);
    compare_dir.join("left_only.parquet")
}

pub fn get_compare_right_path(
    repo: &LocalRepository, 
    compare_id: &str, 
) -> PathBuf {
    let compare_dir = get_compare_dir(repo, compare_id);
    compare_dir.join("right_only.parquet")
}

fn write_compare_dfs(
    repo: &LocalRepository,
    compare_id: &str,
    left_only: &mut DataFrame,
    right_only: &mut DataFrame,
    match_df: &mut DataFrame,
    diff_df: &mut DataFrame,
) -> Result<(), OxenError> {
    let compare_dir = get_compare_dir(repo, compare_id);

    if !compare_dir.exists() {
        std::fs::create_dir_all(&compare_dir)?;
    }

    let match_path = get_compare_match_path(repo, compare_id);
    let diff_path = get_compare_diff_path(repo, compare_id);
    let left_path = get_compare_left_path(repo, compare_id);
    let right_path = get_compare_right_path(repo, compare_id);

    log::debug!("writing {:?} rows to {:?}", match_df.height(), match_path);
    tabular::write_df(match_df, &match_path)?;
    log::debug!("writing {:?} rows to {:?}", diff_df.height(), diff_path);
    tabular::write_df(diff_df, &diff_path)?;
    log::debug!("writing {:?} rows to {:?}", left_only.height(), left_path);
    tabular::write_df(left_only, &left_path)?;
    log::debug!("writing {:?} rows to {:?}", right_only.height(), right_path);
    tabular::write_df(right_only, &right_path)?;

    Ok(())
}

fn write_compare_commit_ids(
    repo: &LocalRepository, 
    compare_id: &str,
    left_id: &str,
    right_id: &str,
) -> Result<(), OxenError> {
    let compare_dir = get_compare_dir(repo, compare_id);

    if !compare_dir.exists() {
        std::fs::create_dir_all(&compare_dir)?;
    }

    let left_path = compare_dir.join(LEFT_COMPARE_COMMIT);
    let right_path = compare_dir.join(RIGHT_COMPARE_COMMIT);

    std::fs::write(left_path, left_id)?;
    std::fs::write(right_path, right_id)?;

    Ok(())
}

fn get_compare_commit_ids(
    repo: &LocalRepository, 
    compare_id: &str,
) -> Result<(Option<String>, Option<String>), OxenError> {
    let compare_dir = get_compare_dir(repo, compare_id);

    if !compare_dir.exists() {
        return Ok((None, None));
    }

    let left_path = compare_dir.join(LEFT_COMPARE_COMMIT);
    let right_path = compare_dir.join(RIGHT_COMPARE_COMMIT);

    // Should exist together or not at all, but recalculate if for some reaosn one not present
    if !left_path.exists() || !right_path.exists() {
        return Ok((None, None));
    }

    let left_id = std::fs::read_to_string(left_path)?;
    let right_id = std::fs::read_to_string(right_path)?;
    
    return Ok((Some(left_id), Some(right_id)));
}



fn compute_row_comparison(
    repo: &LocalRepository,
    compare_id: &str,
    df_1: DataFrame, 
    df_2: DataFrame,
    path_1: &Path,
    path_2: &Path,
    keys: Vec<&str>,
    targets: Vec<&str>,
    opts: DFOpts,
) -> Result<TabularCompare, OxenError> {
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

    let mut left_only = joined_df.filter(
        &joined_df
            .column(format!("{}.right", targets[0]).as_str())?
            .is_null(),
    )?;

    let mut right_only = joined_df.filter(
        &joined_df
            .column(format!("{}.left", targets[0]).as_str())?
            .is_null(),
    )?;

    let mut diff_df = calculate_diff_df(&joined_df, targets.clone(), keys.clone())?;
    let mut match_df = calculate_match_df(&joined_df, targets.clone(), keys.clone())?;

    println!("different targets are {:?}", diff_df);
    println!("same targets are {:?}", match_df);
    println!("df1 unique are {:?}", left_only);
    println!("df2 unique are {:?}", right_only);


        
    let diff_size = diff_df.height();
    let match_size = match_df.height();

    write_compare_dfs(
        repo,
        compare_id,
        &mut left_only,
        &mut right_only,
        &mut match_df,
        &mut diff_df,
    )?;

    // Print different_targets with only the columns in rename_cols with .right and .left 
    let summary = TabularCompareSummary {
        num_left_only_rows: left_only.height(),
        num_right_only_rows: right_only.height(),
        num_diff_rows: diff_size,
        num_match_rows: match_size,
    };


    let match_rows = JsonDataFrame::from_df_opts(match_df, opts.clone());
    let diff_rows = JsonDataFrame::from_df_opts(diff_df, opts.clone());

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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use jwalk::WalkDir;

    use crate::api;
    use crate::command;
    use crate::command::df;
    use crate::core::df::tabular;
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
                "temp_cli_id", // TODONOW
                entry_left, 
                entry_right,
                keys,
                targets,
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
    fn test_compare_files() -> Result<(), OxenError> {
        test::run_compare_data_repo_test_fully_commited(|repo| {
            let head_commit = api::local::commits::head_commit(&repo)?;
            let compare = api::local::compare::compare_files(
                &repo,
                "temp_cli_id", // TODONOW
                api::local::entries::get_commit_entry(&repo, &head_commit, &PathBuf::from("compare_left.csv"))?.unwrap(),
                api::local::entries::get_commit_entry(&repo, &head_commit, &PathBuf::from("compare_right.csv"))?.unwrap(),
                vec!["height".to_string(), "weight".to_string(), "gender".to_string()],
                vec!["target".to_string(), "other_target".to_string()],
                DFOpts::empty(),
            )?;

            assert_eq!(compare.summary.num_left_only_rows, 2);
            assert_eq!(compare.summary.num_right_only_rows, 1);
            assert_eq!(compare.summary.num_match_rows, 6);
            assert_eq!(compare.summary.num_diff_rows, 5);

            Ok(())
        })

    }

    #[test]
    fn test_compare_cache_miss_when_branch_ref_updates() -> Result<(), OxenError> {
        test::run_compare_data_repo_test_fully_commited(|repo| {
            let old_head = api::local::commits::head_commit(&repo)?;
            let left_entry = api::local::entries::get_commit_entry(&repo, &old_head, &PathBuf::from("compare_left.csv"))?.unwrap();
            let right_entry = api::local::entries::get_commit_entry(&repo, &old_head, &PathBuf::from("compare_right.csv"))?.unwrap();
            // Create compare on this commit 
            let created_compare = api::local::compare::compare_files(
                &repo, "a_compare_id", left_entry.clone(), right_entry.clone(), vec![String::from("height"), String::from("weight"), String::from("gender")], 
                vec![String::from("target"), String::from("other_target")], DFOpts::empty())?;

            log::debug!("Here is the original compare {:?}", created_compare);


            // Check getting via cache
            let compare = api::local::compare::get_cached_compare(&repo, "a_compare_id", &left_entry, &right_entry, &DFOpts::empty())?.unwrap();

            log::debug!("here is the cached compare {:?}", compare);
            assert_eq!(compare.summary.num_left_only_rows, 2);
            assert_eq!(compare.summary.num_right_only_rows, 1);
            assert_eq!(compare.summary.num_match_rows, 6);
            assert_eq!(compare.summary.num_diff_rows, 5);

            // Update one of the files
            let path = Path::new("compare_left.csv");
            let file_path = repo.path.join(path);
            let mut df = tabular::read_df(&file_path, DFOpts::empty())?;
            df = df.slice(0, 6);
            tabular::write_df(&mut df, &file_path)?;

            let status = command::status(&repo)?;

            // Commit the new modification
            command::add(&repo, &repo.path)?;
            let status = command::status(&repo)?;
            log::debug!("Here's our status after adding the file {:?}", status);
            command::commit(&repo, "updating compare_left.csv")?;

            // Get new entries and check the cached compare 
            let new_head = api::local::commits::head_commit(&repo)?;
            let new_left_entry = api::local::entries::get_commit_entry(&repo, &new_head, &PathBuf::from("compare_left.csv"))?.unwrap();
            let new_right_entry = api::local::entries::get_commit_entry(&repo, &new_head, &PathBuf::from("compare_right.csv"))?.unwrap();

            let maybe_compare = api::local::compare::get_cached_compare(&repo, "no_id", &new_left_entry, &new_right_entry, &DFOpts::empty())?;
            assert!(maybe_compare.is_none());

            // Create the compare and add to the cache to ensure proper update
            let new_compare = api::local::compare::compare_files(
                &repo, "a_compare_id", new_left_entry, new_right_entry, vec![String::from("height"), String::from("weight"), String::from("gender")], 
                vec![String::from("target"), String::from("other_target")], DFOpts::empty())?;

            // Should be updated values
            assert_eq!(new_compare.summary.num_left_only_rows, 0);
            assert_eq!(new_compare.summary.num_right_only_rows, 6);
            assert_eq!(new_compare.summary.num_match_rows, 6);
            assert_eq!(new_compare.summary.num_diff_rows, 0);

            Ok(())
        })
    }
}
