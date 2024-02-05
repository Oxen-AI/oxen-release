use std::collections::HashSet;

use crate::error::OxenError;
use crate::view::compare::{CompareDupes, CompareTabularRaw};

use polars::chunked_array::ChunkedArray;
use polars::prelude::{ChunkCompare, NamedFrom};
use polars::prelude::{DataFrame, DataFrameJoinOps};
use polars::series::Series;

use super::SchemaDiff;

const TARGETS_HASH_COL: &str = "_targets_hash";
const KEYS_HASH_COL: &str = "_keys_hash";
const DIFF_STATUS_COL: &str = ".oxen.diff.status";

const DIFF_STATUS_ADDED: &str = "added";
const DIFF_STATUS_REMOVED: &str = "removed";
const DIFF_STATUS_MODIFIED: &str = "modified";
const DIFF_STATUS_UNCHANGED: &str = "unchanged";

pub fn compare(
    df_1: &DataFrame,
    df_2: &DataFrame,
    schema_diff: SchemaDiff,
    targets: Vec<&str>,
    keys: Vec<&str>,
) -> Result<CompareTabularRaw, OxenError> {
    if !targets.is_empty() && keys.is_empty() {
        return Err(OxenError::basic_str(
            "Must specifiy at least one key column if specifying target columns.",
        ));
    }

    let output_columns = get_output_columns(keys.clone(), targets.clone(), schema_diff.clone());
    log::debug!("out columns are {:?}", output_columns);

    let joined_df = join_hashed_dfs(
        df_1,
        df_2,
        keys.clone(),
        targets.clone(),
        schema_diff.unchanged_cols.clone(),
    )?;

    log::debug!("joined df: {:#?}", joined_df);

    log::debug!("getting diff");
    let diff_df = calculate_diff_df(&joined_df, targets.clone(), keys.clone(), &output_columns)?;
    log::debug!("getting match");
    let match_df = calculate_match_df(&joined_df, targets.clone(), keys.clone(), &output_columns)?;
    log::debug!("getting left only");
    let left_only_df =
        calculate_left_df(&joined_df, targets.clone(), keys.clone(), &output_columns)?;
    log::debug!("getting right only");
    let right_only_df =
        calculate_right_df(&joined_df, targets.clone(), keys.clone(), &output_columns)?;

    log::debug!("diff_df: {:#?}", diff_df);
    log::debug!("match_df: {:#?}", match_df);
    log::debug!("left_only_df: {:#?}", left_only_df);
    log::debug!("right_only_df: {:#?}", right_only_df);

    // Stack these together and then sort by the keys
    let mut stacked_df = diff_df.vstack(&match_df)?;
    stacked_df = stacked_df.vstack(&left_only_df)?;
    stacked_df = stacked_df.vstack(&right_only_df)?;

    Ok(CompareTabularRaw {
        added_cols_df: DataFrame::default(),
        removed_cols_df: DataFrame::default(),
        diff_df: stacked_df,
        dupes: CompareDupes { left: 0, right: 0 },
    })
}

fn get_output_columns(keys: Vec<&str>, targets: Vec<&str>, schema_diff: SchemaDiff) -> Vec<String> {
    // TODONOW: this is messy. look what polars has done to us.
    let mut out_columns = vec![];
    // All targets, renamed
    for target in targets.iter() {
        out_columns.push(format!("{}.left", target));
        out_columns.push(format!("{}.right", target));
    }

    for key in keys.iter() {
        out_columns.push(key.to_string());
    }

    // Columns in both dfs are renamed
    for col in schema_diff.unchanged_cols.iter() {
        if !targets.contains(&col.as_str()) && !keys.contains(&col.as_str()) {
            out_columns.push(format!("{}.left", col));
            out_columns.push(format!("{}.right", col));
        }
    }

    // Columns in one df are just once
    for col in schema_diff.added_cols.iter() {
        if !targets.contains(&col.as_str()) && !keys.contains(&col.as_str()) {
            out_columns.push(col.to_string());
        }
    }

    for col in schema_diff.removed_cols.iter() {
        if !targets.contains(&col.as_str()) && !keys.contains(&col.as_str()) {
            out_columns.push(col.to_string());
        }
    }

    out_columns.push(DIFF_STATUS_COL.to_string());
    out_columns
}

fn join_hashed_dfs(
    left_df: &DataFrame,
    right_df: &DataFrame,
    keys: Vec<&str>,
    targets: Vec<&str>,
    unchanged_cols: Vec<String>,
) -> Result<DataFrame, OxenError> {
    let mut joined_df = left_df.outer_join(right_df, [KEYS_HASH_COL], [KEYS_HASH_COL])?;

    let mut cols_to_rename = targets.clone();
    for key in keys.iter() {
        cols_to_rename.push(key);
    }
    // TODONOW: maybe set logic?
    for col in unchanged_cols.iter() {
        if !cols_to_rename.contains(&col.as_str()) {
            cols_to_rename.push(col);
        }
    }
    if !targets.is_empty() {
        cols_to_rename.push(TARGETS_HASH_COL);
    }

    for target in cols_to_rename.iter() {
        log::debug!("trying to rename col: {}", target);
        let left_before = target.to_string();
        let left_after = format!("{}.left", target);
        let right_before = format!("{}_right", target);
        let right_after = format!("{}.right", target);
        joined_df.rename(&left_before, &left_after)?;
        joined_df.rename(&right_before, &right_after)?;
    }

    Ok(joined_df)
}

fn calculate_diff_df(
    df: &DataFrame,
    targets: Vec<&str>,
    keys: Vec<&str>,
    output_columns: &Vec<String>,
) -> Result<DataFrame, OxenError> {
    // If no targets, this is undefined - return schema-matched empty df.

    let diff_mask = if targets.is_empty() {
        ChunkedArray::new("false", vec![false; df.height()])
    } else {
        df.column(format!("{}.left", TARGETS_HASH_COL).as_str())?
            .not_equal(df.column(format!("{}.right", TARGETS_HASH_COL).as_str())?)?
    };

    let mut diff_df = df.filter(&diff_mask)?;
    log::debug!("diff columns: {:?}", diff_df.get_column_names());
    for key in keys.iter() {
        diff_df.rename(&format!("{}.left", key), key)?;
    }

    let mut diff_df = diff_df.with_column(Series::new(
        DIFF_STATUS_COL,
        vec![DIFF_STATUS_MODIFIED; diff_df.height()],
    ))?;

    // Add on the diff status

    Ok(diff_df.select(output_columns)?)
}

fn calculate_match_df(
    df: &DataFrame,
    targets: Vec<&str>,
    keys: Vec<&str>,
    output_columns: &Vec<String>,
) -> Result<DataFrame, OxenError> {
    // Mask behavior: if targets defined, return match on targets hash. Else, match on keys hash.
    log::debug!("columns for match are {:?}", df.get_column_names());
    // keys[0] is guaranteed to exist - if not specified, we've populated it with all columns earlier
    let match_mask = if targets.is_empty() {
        df.column(format!("{}.left", keys[0]).as_str())?
            .equal(df.column(format!("{}.right", keys[0]).as_str())?)?
    } else {
        df.column(format!("{}.left", TARGETS_HASH_COL).as_str())?
            .equal(df.column(format!("{}.right", TARGETS_HASH_COL).as_str())?)?
    };

    let mut match_df = df.filter(&match_mask)?;

    for key in keys.iter() {
        match_df.rename(&format!("{}.left", key), key)?;
    }

    let mut match_df = match_df.with_column(Series::new(
        DIFF_STATUS_COL,
        vec![DIFF_STATUS_UNCHANGED; match_df.height()],
    ))?;

    Ok(match_df.select(output_columns)?)
}

fn calculate_left_df(
    df: &DataFrame,
    targets: Vec<&str>,
    keys: Vec<&str>,
    output_columns: &Vec<String>,
) -> Result<DataFrame, OxenError> {
    // let keys_and_targets = keys
    //     .iter()
    //     .chain(targets.iter())
    //     .copied()
    //     .collect::<Vec<&str>>();

    // Using keys hash col is correct regardless of whether or not there are targets
    let mut left_only = df.filter(&df.column(format!("{}.right", keys[0]).as_str())?.is_null())?;
    for key in keys.iter() {
        left_only.rename(&format!("{}.left", key), key)?;
    }

    // for target in targets.iter() {
    //     let left_before = format!("{}.left", target);
    //     let left_after = target.to_string();
    //     left_only.rename(&left_before, &left_after)?;
    // }

    let mut left_only = left_only.with_column(Series::new(
        DIFF_STATUS_COL,
        vec![DIFF_STATUS_REMOVED; left_only.height()],
    ))?;

    Ok(left_only.select(output_columns)?)
}

fn calculate_right_df(
    df: &DataFrame,
    targets: Vec<&str>,
    keys: Vec<&str>,
    output_columns: &Vec<String>,
) -> Result<DataFrame, OxenError> {
    // Using keys hash col is correct regardless of whether or not there are targets
    // let keys_and_targets = keys
    //     .iter()
    //     .chain(targets.iter())
    //     .copied()
    //     .collect::<Vec<&str>>();

    let mut right_only = df.filter(&df.column(format!("{}.left", keys[0]).as_str())?.is_null())?;
    for key in keys.iter() {
        right_only.rename(&format!("{}.right", key), key)?;
    }

    let mut right_only = right_only.with_column(Series::new(
        DIFF_STATUS_COL,
        vec![DIFF_STATUS_ADDED; right_only.height()],
    ))?;

    // for target in targets.iter() {
    //     let right_before = format!("{}.right", target);
    //     let right_after = target.to_string();
    //     right_only.rename(&right_before, &right_after)?;
    // }

    Ok(right_only.select(output_columns)?)
}
