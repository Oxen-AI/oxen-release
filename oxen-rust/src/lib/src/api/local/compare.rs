use polars::datatypes::BooleanChunked;
use polars::frame::hash_join::JoinType;
use serde::{Deserialize, Serialize};

use crate::core::df::tabular;
use crate::core::index::CommitDirEntryReader;
use crate::error::OxenError;
use crate::model::compare::tabular_compare::TabularCompare;
use crate::model::compare::tabular_compare_summary::TabularCompareSummary;
use crate::model::diff::diff_entry_status::DiffEntryStatus;
use crate::model::diff::generic_diff::GenericDiff;
use crate::model::schema::Field;
use crate::model::{Commit, CommitEntry, DataFrameDiff, DiffEntry, LocalRepository, Schema};
use crate::opts::DFOpts;
use crate::view::compare::AddRemoveModifyCounts;
use crate::view::schema::SchemaWithPath;
use crate::view::{JsonDataFrame, JsonDataFrameView, Pagination};
use crate::{api, constants, util};

use crate::core::index::CommitEntryReader;
use colored::Colorize;
use difference::{Changeset, Difference};
use polars::export::ahash::HashMap;
use polars::prelude::ChunkCompare;
use polars::prelude::IntoLazy;
use polars::prelude::{DataFrame, DataFrameJoinOps};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub fn compare_files(
    // TODONOW split this function up!!!
    repo: &LocalRepository,
    file_1: PathBuf, // TODONOW: make these resources? Option<CommitEntry> or CommitENtry
    commit_1: Commit,
    file_2: PathBuf,
    commit_2: Commit,
    keys: Vec<String>,
    targets: Vec<String>,
    opts: DFOpts, // TODONOW: custom return type
) -> Result<TabularCompare, OxenError> {
    // Assert that the files exist in their respective commits and are tabular.
    let version_file_1 = api::local::diff::get_version_file_from_commit(repo, &commit_1, &file_1)?;
    let version_file_2 = api::local::diff::get_version_file_from_commit(repo, &commit_2, &file_2)?;

    if !util::fs::is_tabular(&version_file_1) || !util::fs::is_tabular(&version_file_2) {
        return Err(OxenError::invalid_file_type(format!(
            "Compare not supported for non-tabular files, found {file_1:?} and {file_2:?}",
        )));
    }

    if !version_file_1.exists() {
        return Err(OxenError::entry_does_not_exist(version_file_1));
    }

    if !version_file_2.exists() {
        return Err(OxenError::entry_does_not_exist(version_file_2));
    }

    // Read DFs and get schemas
    let df_1 = tabular::read_df(&version_file_1, DFOpts::empty())?;
    let df_2 = tabular::read_df(&version_file_2, DFOpts::empty())?;

    let schema_1 = Schema::from_polars(&df_1.schema());
    let schema_2 = Schema::from_polars(&df_2.schema());

    // Get the diff between the two schemas - // todonow: separate function probably starts here.

    // Subset dataframes to "keys" and "targets"
    let required_fields = keys
        .iter()
        .chain(targets.iter())
        .map(|field| field.clone())
        .collect::<Vec<String>>();

    println!("required fields are {:?}", required_fields);
    // Make sure both dataframes have all required fields
    // TODONOW: different error type that will print a descriptive message
    if !schema_1.has_field_names(&required_fields) {
        return Err(OxenError::InvalidSchema(Box::new(schema_1)));
    }

    if !schema_2.has_field_names(&required_fields) {
        return Err(OxenError::InvalidSchema(Box::new(schema_2)));
    }

    // Subset the dataframes to only the required fields
    let df_1 = df_1.select(&required_fields)?;
    let df_2 = df_2.select(&required_fields)?;

    // TODONOW type management of these slices
    let keys = keys.iter().map(|key| key.as_str()).collect::<Vec<&str>>();
    let targets = targets
        .iter()
        .map(|target| target.as_str())
        .collect::<Vec<&str>>();

    let compare = compute_row_comparison(&df_1, &df_2, &file_1, &file_2, keys, targets, opts)?;

    Ok(compare)
}

fn compute_row_comparison(
    df_1: &DataFrame,
    df_2: &DataFrame,
    path_1: &Path,
    path_2: &Path,
    keys: Vec<&str>,
    targets: Vec<&str>,
    opts: DFOpts,
) -> Result<TabularCompare, OxenError> {
    // TODONOW: dfs should be subset before the join
    // TODONOW: write a test for when the dfs have completely unique keys
    // to make sure a new target column is created.
    // Hash the rows on the keys

    // TODONOW: shouldn't be cloning keys all over the place
    // let df_1 = tabular::df_hash_rows_on_cols(df_1.clone(), keys.clone())?;
    // let df_2 = tabular::df_hash_rows_on_cols(df_2.clone(), keys.clone())?;

    // TODONOW: should we hash the cols? or just join?

    // Outer join on the keys
    // TODONOW: this could cause a combinatorial explosion to m*n rows
    // if keys are not unique within individual dfs. If they ARE, (which we should enforce before this step)
    // then worst-case is m+n

    // TODONOW, maybe we don't need the independent hashing and can let polars handle

    let mut joined_df = df_1.outer_join(&df_2, keys.clone(), keys.clone())?;

    // Rename every target col to be {name}.left, and every target_right column to be {name}.right

    // TODONOW: ew
    for target in targets.iter() {
        let left_before = format!("{}", target);
        let left_after = format!("{}.left", target);
        let right_before = format!("{}_right", target);
        let right_after = format!("{}.right", target);
        joined_df.rename(&left_before, &left_after)?;
        joined_df.rename(&right_before, &right_after)?;
    }

    println!(
        "columns of df after join are {:?}",
        joined_df.get_column_names()
    );
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

    // Collect the boolean conditions into a vector
    let different_conditions = targets
        .iter()
        .map(|target| {
            let left = format!("{}.left", target);
            let right = format!("{}.right", target);
            let left_col = joined_df.column(&left)?;
            let right_col = joined_df.column(&right)?;
            Ok(left_col.not_equal(right_col)?)
        })
        .collect::<Result<Vec<BooleanChunked>, OxenError>>()?;

    // Combine the conditions into a single boolean mask
    let different_mask = different_conditions
        .into_iter()
        .reduce(|acc, mask| acc | mask)
        .unwrap(); // TODONOW

    // Use the mask to filter the dataframe
    let different_targets = joined_df.filter(&different_mask)?;

    let same_conditions = targets
        .iter()
        .map(|target| {
            let left = format!("{}.left", target);
            let right = format!("{}.right", target);
            let left_col = joined_df.column(&left)?;
            let right_col = joined_df.column(&right)?;
            Ok(left_col.equal(right_col)?)
        })
        .collect::<Result<Vec<BooleanChunked>, OxenError>>()?;

    // Combine the conditions into a single boolean mask
    let same_mask = same_conditions
        .into_iter()
        .reduce(|acc, mask| acc & mask)
        .unwrap(); // TODONOW

    // Use the mask to filter the dataframe
    let mut same_targets = joined_df.filter(&same_mask)?;

    // TODONOW: VERY UGLY AND BADx
    // For every target, drop .right and rename .left to just target
    for target in targets.iter() {
        let left = format!("{}.left", target);
        let right = format!("{}.right", target);
        same_targets = same_targets.drop(&right)?;
        same_targets.rename(&left, target)?;
    }

    println!("different targets are {:?}", different_targets);
    println!("same targets are {:?}", same_targets);
    println!("df1 unique are {:?}", df1_unique);
    println!("df2 unique are {:?}", df2_unique);

    let summary = TabularCompareSummary {
        num_left_only_rows: df1_unique.height(),
        num_right_only_rows: df2_unique.height(),
        num_diff_rows: different_targets.height(),
        num_match_rows: same_targets.height(),
    };

    // TODONOW: Paginate?
    // TODONOW: view?
    let match_rows = JsonDataFrame::from_df_opts(same_targets, opts.clone());
    let diff_rows = JsonDataFrame::from_df_opts(different_targets, opts.clone());

    let schema_left = SchemaWithPath {
        path: path_1.as_os_str().to_str().map(|s| s.to_owned()).unwrap(),
        schema: Schema::from_polars(&df_1.schema()),
    };

    // TODONOW: clean up this path
    // TODONOW unwrapping, lossy, whatevs
    let schema_right = SchemaWithPath {
        path: path_2.as_os_str().to_str().map(|s| s.to_owned()).unwrap(),
        schema: Schema::from_polars(&df_2.schema()),
    };

    let tabular_compare = TabularCompare {
        summary,
        schema_left: Some(schema_left),
        schema_right: Some(schema_right),
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

// #[cfg(test)]
// mod tests {
//     use std::path::Path;
//     use std::path::PathBuf;

//     use crate::api;
//     use crate::command;
//     use crate::error::OxenError;
//     use crate::model::diff::diff_entry_status::DiffEntryStatus;
//     use crate::opts::RmOpts;
//     use crate::test;
//     use crate::util;

//     #[test]
//     fn test_compare_fails_when_not_tabular() -> Result<(), OxenError> {
//         test::run_bounding_box_csv_repo_test_fully_committed(|repo| {
//             let hello_file = repo.path.join("Hello.txt");
//             let world_file = repo.path.join("World.txt");
//             test::write_txt_file_to_path(&hello_file, "Hello")?;
//             test::write_txt_file_to_path(&world_file, "World")?;

//             command::add(&repo, &hello_file)?;
//             command::add(&repo, &world_file)?;

//             command::commit(&repo, "adding_new_files")?;

//             let head_commit = api::local::commits::head_commit(&repo)?;

//             let keys = vec![];
//             let targets = vec![];

//             let result = api::local::compare::compare_files(
//                 &repo,
//                 hello_file,
//                 head_commit.clone(),
//                 world_file,
//                 head_commit,
//                 keys,
//                 targets,
//             );

//             log::debug!("{:?}", result);
//             assert!(matches!(
//                 result.unwrap_err(),
//                 OxenError::InvalidFileType(_)
//             ));

//             Ok(())
//         })
//     }
// }
