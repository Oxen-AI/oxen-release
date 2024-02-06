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

pub fn compare(
    df_1: &DataFrame,
    df_2: &DataFrame,
    schema_diff: SchemaDiff,
    targets: Vec<&str>,
    keys: Vec<&str>,
    display: Vec<&str>,
) -> Result<CompareTabularRaw, OxenError> {
    if !targets.is_empty() && keys.is_empty() {
        return Err(OxenError::basic_str(
            "Must specifiy at least one key column if specifying target columns.",
        ));
    }

    let output_columns = get_output_columns(
        keys.clone(),
        targets.clone(),
        display.clone(),
        schema_diff.clone(),
    );
    log::debug!("out columns are {:?}", output_columns);

    let joined_df = join_hashed_dfs(
        df_1,
        df_2,
        keys.clone(),
        targets.clone(),
        schema_diff.clone(),
    )?;

    log::debug!("joined df: {:#?}", joined_df);

    let modified_df =
        calculate_modified_df(&joined_df, targets.clone(), keys.clone(), &output_columns)?;
    let removed_df = calculate_removed_df(&joined_df, keys.clone(), &output_columns)?;
    let added_df = calculate_added_df(&joined_df, keys.clone(), &output_columns)?;

    // Stack these together and then sort by the keys
    let mut stacked_df = modified_df.vstack(&removed_df)?;
    stacked_df = stacked_df.vstack(&added_df)?;

    let descending = keys.iter().map(|_| false).collect::<Vec<bool>>();
    let sorted_df = stacked_df.sort(&keys, descending, false)?;

    Ok(CompareTabularRaw {
        added_cols_df: DataFrame::default(),
        removed_cols_df: DataFrame::default(),
        diff_df: sorted_df,
        dupes: CompareDupes { left: 0, right: 0 },
    })
}

fn get_output_columns(
    keys: Vec<&str>,
    targets: Vec<&str>,
    display: Vec<&str>,
    schema_diff: SchemaDiff,
) -> Vec<String> {
    // Ordering for now: keys, then targets, then removed cols, then added
    let mut out_columns = vec![];
    // All targets, renamed
    for key in keys.iter() {
        out_columns.push(key.to_string());
    }
    for target in targets.iter() {
        out_columns.push(format!("{}.left", target));
        out_columns.push(format!("{}.right", target));
    }

    // Columns in both dfs are renamed
    // for col in schema_diff.unchanged_cols.iter() {
    //     if !targets.contains(&col.as_str()) && !keys.contains(&col.as_str()) {
    //         out_columns.push(format!("{}.left", col));
    //         out_columns.push(format!("{}.right", col));
    //     }
    // }
    // for col in schema_diff.removed_cols.iter() {
    //     // If in display chcek maybe?
    //     if !targets.contains(&col.as_str()) && !keys.contains(&col.as_str()) {
    //         out_columns.push(format!("{}.left", col));
    //     }
    // }
    // for col in schema_diff.added_cols.iter() {
    //     // If in display check maybe?
    //     if !targets.contains(&col.as_str()) && !keys.contains(&col.as_str()) {
    //         out_columns.push(format!("{}.right", col));
    //     }
    // }

    for col in display.iter() {
        if col.ends_with(".left") {
            let stripped = col.trim_end_matches(".left");
            if schema_diff.removed_cols.contains(&stripped.to_string())
                || schema_diff.unchanged_cols.contains(&stripped.to_string())
            {
                out_columns.push(col.to_string());
            }
        }
        if col.ends_with(".right") {
            let stripped = col.trim_end_matches(".right");
            if schema_diff.added_cols.contains(&stripped.to_string())
                || schema_diff.unchanged_cols.contains(&stripped.to_string())
            {
                out_columns.push(col.to_string());
            }
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
    schema_diff: SchemaDiff,
) -> Result<DataFrame, OxenError> {
    let mut joined_df = left_df.outer_join(right_df, [KEYS_HASH_COL], [KEYS_HASH_COL])?;

    let mut cols_to_rename = targets.clone();
    for key in keys.iter() {
        cols_to_rename.push(key);
    }
    // TODONOW: maybe set logic?
    for col in schema_diff.unchanged_cols.iter() {
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

    for col in schema_diff.added_cols.iter() {
        joined_df.rename(col, &format!("{}.right", col))?;
    }

    for col in schema_diff.removed_cols.iter() {
        joined_df.rename(col, &format!("{}.left", col))?;
    }

    Ok(joined_df)
}

fn calculate_modified_df(
    df: &DataFrame,
    targets: Vec<&str>,
    keys: Vec<&str>,
    output_columns: &Vec<String>,
) -> Result<DataFrame, OxenError> {
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

    let diff_df = diff_df.with_column(Series::new(
        DIFF_STATUS_COL,
        vec![DIFF_STATUS_MODIFIED; diff_df.height()],
    ))?;

    // Add on the diff status
    Ok(diff_df.select(output_columns)?)
}

fn calculate_removed_df(
    df: &DataFrame,
    keys: Vec<&str>,
    output_columns: &Vec<String>,
) -> Result<DataFrame, OxenError> {
    // Using keys hash col is correct regardless of whether or not there are targets
    let mut left_only = df.filter(&df.column(format!("{}.right", keys[0]).as_str())?.is_null())?;
    for key in keys.iter() {
        left_only.rename(&format!("{}.left", key), key)?;
    }

    let left_only = left_only.with_column(Series::new(
        DIFF_STATUS_COL,
        vec![DIFF_STATUS_REMOVED; left_only.height()],
    ))?;

    Ok(left_only.select(output_columns)?)
}

fn calculate_added_df(
    df: &DataFrame,
    keys: Vec<&str>,
    output_columns: &Vec<String>,
) -> Result<DataFrame, OxenError> {
    let mut right_only = df.filter(&df.column(format!("{}.left", keys[0]).as_str())?.is_null())?;
    for key in keys.iter() {
        right_only.rename(&format!("{}.right", key), key)?;
    }

    let right_only = right_only.with_column(Series::new(
        DIFF_STATUS_COL,
        vec![DIFF_STATUS_ADDED; right_only.height()],
    ))?;

    Ok(right_only.select(output_columns)?)
}
