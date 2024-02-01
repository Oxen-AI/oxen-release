use crate::error::OxenError;
use crate::view::compare::{CompareDupes, CompareTabularRaw};

use polars::prelude::ChunkCompare;
use polars::prelude::{DataFrame, DataFrameJoinOps};

const TARGETS_HASH_COL: &str = "_targets_hash";
const KEYS_HASH_COL: &str = "_keys_hash";

pub fn compare(
    df_1: &DataFrame,
    df_2: &DataFrame,
    targets: Vec<&str>,
    keys: Vec<&str>,
) -> Result<CompareTabularRaw, OxenError> {
    let joined_df = join_hashed_dfs(df_1, df_2, targets.clone())?;

    let diff_df = calculate_diff_df(&joined_df, targets.clone(), keys.clone())?;
    let match_df = calculate_match_df(&joined_df, targets.clone(), keys.clone())?;
    let left_only_df = calculate_left_df(&joined_df, targets.clone(), keys.clone())?;
    let right_only_df = calculate_right_df(&joined_df, targets.clone(), keys.clone())?;

    // Stack these together and then sort by the keys

    log::debug!("diff columns: {:?}", diff_df.get_column_names());
    log::debug!("match columns: {:?}", match_df.get_column_names());
    log::debug!("left columns: {:?}", left_only_df.get_column_names());
    log::debug!("right columns: {:?}", right_only_df.get_column_names());

    Ok(CompareTabularRaw {
        added_cols_df: DataFrame::default(),
        removed_cols_df: DataFrame::default(),
        diff_df,
        dupes: CompareDupes { left: 0, right: 0 },
    })
}

fn join_hashed_dfs(
    left_df: &DataFrame,
    right_df: &DataFrame,
    targets: Vec<&str>,
) -> Result<DataFrame, OxenError> {
    let mut joined_df = left_df.outer_join(right_df, [KEYS_HASH_COL], [KEYS_HASH_COL])?;

    let mut cols_to_rename = targets.clone();
    cols_to_rename.push(TARGETS_HASH_COL);

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
) -> Result<DataFrame, OxenError> {
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

fn calculate_match_df(
    df: &DataFrame,
    targets: Vec<&str>,
    keys: Vec<&str>,
) -> Result<DataFrame, OxenError> {
    let match_mask = df
        .column(format!("{}.left", TARGETS_HASH_COL).as_str())?
        .equal(df.column(format!("{}.right", TARGETS_HASH_COL).as_str())?)?;

    let mut match_df = df.filter(&match_mask)?;

    for target in targets.iter() {
        let left_before = format!("{}.left", target);
        let left_after = target.to_string();
        match_df.rename(&left_before, &left_after)?;
    }

    let cols_to_keep = keys.iter().chain(targets.iter()).copied();

    Ok(match_df.select(cols_to_keep)?)
}

fn calculate_left_df(
    df: &DataFrame,
    targets: Vec<&str>,
    keys: Vec<&str>,
) -> Result<DataFrame, OxenError> {
    let keys_and_targets = keys
        .iter()
        .chain(targets.iter())
        .copied()
        .collect::<Vec<&str>>();

    let mut left_only = df.filter(
        &df.column(format!("{}.right", targets[0]).as_str())?
            .is_null(),
    )?;

    for target in targets.iter() {
        let left_before = format!("{}.left", target);
        let left_after = target.to_string();
        left_only.rename(&left_before, &left_after)?;
    }

    Ok(left_only.select(keys_and_targets.clone())?)
}

fn calculate_right_df(
    df: &DataFrame,
    targets: Vec<&str>,
    keys: Vec<&str>,
) -> Result<DataFrame, OxenError> {
    let keys_and_targets = keys
        .iter()
        .chain(targets.iter())
        .copied()
        .collect::<Vec<&str>>();

    let mut right_only = df.filter(
        &df.column(format!("{}.left", targets[0]).as_str())?
            .is_null(),
    )?;

    for target in targets.iter() {
        let right_before = format!("{}.right", target);
        let right_after = target.to_string();
        right_only.rename(&right_before, &right_after)?;
    }

    Ok(right_only.select(keys_and_targets.clone())?)
}
