use std::sync::Arc;

use crate::error::OxenError;
use crate::model::Schema;
use crate::view::compare::{
    CompareDupes, CompareSchemaColumn, CompareSchemaDiff, CompareSummary, CompareTabularMods,
    CompareTabularRaw,
};

use duckdb::types::Null;
use polars::chunked_array::ChunkedArray;
use polars::datatypes::{AnyValue, StringChunked};
use polars::lazy::dsl::{all, as_struct, col, GetOutput};
use polars::lazy::dsl::{coalesce, lit};
use polars::lazy::frame::IntoLazy;
use polars::prelude::{ChunkCompare, NamedFrom};
use polars::prelude::{DataFrame, DataFrameJoinOps};
use polars::series::{IntoSeries, Series};

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

    // What columns do we actually want going into this?
    // Well, keys[0], and the targets_hash_col (but it might not exist...)

    let col_names = [
        format!("{}.left", keys[0]),
        format!("{}.right", keys[0]),
        format!("{}.left", TARGETS_HASH_COL),
        format!("{}.right", TARGETS_HASH_COL),
    ];

    let mut field_names = vec![];
    // Iterate over col_names to ensure the order is preserved
    for col_name in &col_names {
        // Check if the joined_df schema contains the column name
        if joined_df
            .schema()
            .iter_fields()
            .any(|field| field.name() == col_name)
        {
            field_names.push(col(col_name));
        }
    }

    // For pulling into the closure
    let has_targets = !targets.is_empty();
    let joined_df = joined_df
        .lazy()
        .select([
            all(),
            as_struct(field_names)
                .apply(
                    move |s| {
                        let ca = s.struct_()?;
                        let out: StringChunked = ca
                            .into_iter()
                            .map(|row| {
                                log::debug!("here's the row: {:#?}", row);
                                let key_left = row.get(0);
                                let key_right = row.get(1);
                                let target_hash_left = row.get(2);
                                let target_hash_right = row.get(3);

                                test_function(
                                    key_left,
                                    key_right,
                                    target_hash_left,
                                    target_hash_right,
                                    has_targets,
                                )
                            })
                            .collect();

                        Ok(Some(out.into_series()))
                    },
                    GetOutput::from_type(polars::prelude::DataType::String),
                )
                .alias(DIFF_STATUS_COL),
        ])
        .collect()?;

    log::debug!("finished joining");

    // log::debug!("joined df: {:#?}", joined_df.select(debug_cols));

    // let modified_df =
    //     calculate_modified_df(&joined_df, targets.clone(), keys.clone(), &output_columns)?;
    // log::debug!("getting removed df");
    // let removed_df = calculate_removed_df(&joined_df, keys.clone(), &output_columns)?;
    // log::debug!("getting added_df");
    // let added_df = calculate_added_df(&joined_df, keys.clone(), &output_columns)?;

    // let diff_status = add_diff_status_column(
    //     &mut joined_df,
    //     keys.clone(),
    //     targets.clone(),
    //     &output_columns,
    // )?;

    // let compare_summary = {
    //     let added_rows = added_df.height();
    //     let removed_rows = removed_df.height();
    //     let modified_rows = modified_df.height();
    //     let derived_schema = Schema::from_polars(&modified_df.schema());
    //     CompareSummary {
    //         modifications: CompareTabularMods {
    //             added_rows,
    //             removed_rows,
    //             modified_rows,
    //         },
    //         schema: derived_schema,
    //     }
    // };

    // // Stack these together and then sort by the keys
    // log::debug!("stacking dfs");
    // let mut stacked_df = modified_df.vstack(&removed_df)?;
    // log::debug!("again");
    // // print out all the columns of stacked_df and their dtypes
    // for col in stacked_df.get_column_names() {
    //     log::debug!("col: {}", col);
    //     log::debug!("dtype: {:?}", stacked_df.column(col).unwrap().dtype());
    // }

    // // do the same with added_df
    // for col in added_df.get_column_names() {
    //     log::debug!("col: {}", col);
    //     log::debug!("dtype: {:?}", added_df.column(col).unwrap().dtype());
    // }
    // stacked_df = stacked_df.vstack(&added_df)?;
    // log::debug!("againagain");

    // filter the joined_df to where diff_status_col != "unchanged"
    let mut joined_df = joined_df.filter(
        &joined_df
            .column(DIFF_STATUS_COL)?
            .not_equal(DIFF_STATUS_UNCHANGED)?,
    )?;

    // TODO: is converting to lazy in the loop costly?
    for key in keys.clone() {
        joined_df = joined_df
            .lazy()
            .with_columns([coalesce(&[
                col(&format!("{}.right", key)),
                col(&format!("{}.left", key)),
            ])
            .alias(key)])
            .collect()?;
    }

    // Rename all columns in schema_diff.added_cols to .right
    for c in schema_diff.added_cols.iter() {
        joined_df = joined_df
            .lazy()
            .with_column(col(&c).alias(&format!("{}.right", c)))
            .collect()?;
    }

    // Rename all columns in schema_diff.removed_cols to .left
    for c in schema_diff.removed_cols.iter() {
        joined_df = joined_df
            .lazy()
            .with_column(col(&c).alias(&format!("{}.left", c)))
            .collect()?;
    }

    let descending = keys.iter().map(|_| false).collect::<Vec<bool>>();
    let joined_df = joined_df.sort(&keys, descending, false)?;
    let schema_diff = build_compare_schema_diff(schema_diff, df_1, df_2)?;

    log::debug!(
        "joined_df with its cols and dtypes: {:#?}",
        joined_df.schema()
    );

    let output_cols = [
        "height",
        "weight",
        ".oxen.diff.status",
        // "target.left",
        // "other_target.left",
        "target.right",
        "other_target.right",
    ];
    let final_df = joined_df.select(output_cols)?;

    Ok(CompareTabularRaw {
        diff_df: final_df,
        dupes: CompareDupes { left: 0, right: 0 },
        schema_diff: Some(schema_diff),
        compare_summary: Some(
            CompareSummary {
                modifications: CompareTabularMods {
                    added_rows: 0,
                    removed_rows: 0,
                    modified_rows: 0,
                },
                schema: Schema::from_polars(&joined_df.schema()),
            }, // TODONOW return this!
        ),
    })
}

fn build_compare_schema_diff(
    schema_diff: SchemaDiff,
    df_1: &DataFrame,
    df_2: &DataFrame,
) -> Result<CompareSchemaDiff, OxenError> {
    // Refactor the above to use result instead of unwrap
    let added_cols = schema_diff
        .added_cols
        .iter()
        .map(|col| {
            let dtype = df_2.column(col)?;
            Ok(CompareSchemaColumn {
                name: col.clone(),
                key: format!("{}.right", col),
                dtype: dtype.dtype().to_string(),
            })
        })
        .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

    let removed_cols = schema_diff
        .removed_cols
        .iter()
        .map(|col| {
            let dtype = df_1.column(col)?;
            Ok(CompareSchemaColumn {
                name: col.clone(),
                key: format!("{}.left", col),
                dtype: dtype.dtype().to_string(),
            })
        })
        .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

    Ok(CompareSchemaDiff {
        added_cols,
        removed_cols,
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

    log::debug!("about to append new column");
    let diff_df = diff_df.with_column(Series::new(
        DIFF_STATUS_COL,
        vec![DIFF_STATUS_MODIFIED; diff_df.height()],
    ))?;
    log::debug!("appended new column");

    // Add on the diff status
    Ok(diff_df.select(output_columns)?)
}

// fn calculate_removed_df(
//     df: &DataFrame,
//     keys: Vec<&str>,
//     output_columns: &Vec<String>,
// ) -> Result<DataFrame, OxenError> {
//     // Using keys hash col is correct regardless of whether or not there are targets
//     let mut left_only = df.filter(&df.column(format!("{}.right", keys[0]).as_str())?.is_null())?;
//     for key in keys.iter() {
//         left_only.rename(&format!("{}.left", key), key)?;
//     }

//     let left_only = left_only.with_column(Series::new(
//         DIFF_STATUS_COL,
//         vec![DIFF_STATUS_REMOVED; left_only.height()],
//     ))?;

//     Ok(left_only.select(output_columns)?)
// }

// fn add_diff_status_column(
//     df: &mut DataFrame,
//     keys: Vec<&str>,
//     targets: Vec<&str>,
//     output_columns: &Vec<String>,
// ) -> Result<(), OxenError> {
//     // Iterate over the df
//     df.apply(|row| Ok(get_row_diff_status(row, keys.clone(), targets.clone())));

//     Ok(())
// }

// fn get_row_diff_status(row: &Series, keys: Vec<&str>, targets: Vec<&str>) -> String {
//     // If the column "keys[0].right" is null, it's a removed row
//     if row.get(&format!("{}.right", keys[0])).is_null() {
//         return DIFF_STATUS_REMOVED.to_string();
//     }

//     // If the column "keys[0].left" is null, it's an added row
//     if row.get(&format!("{}.left", keys[0])).is_null() {
//         return DIFF_STATUS_ADDED.to_string();
//     }

//     // If there are targets, check if they are different
//     // TODO: handle implicit targets!
//     if !targets.is_empty() {
//         let left_hash = row.get(&format!("{}.left", TARGETS_HASH_COL));
//         let right_hash = row.get(&format!("{}.right", TARGETS_HASH_COL));
//         if left_hash != right_hash {
//             return DIFF_STATUS_MODIFIED.to_string();
//         }
//     }

//     // If we've made it this far, it's unchanged
//     DIFF_STATUS_UNCHANGED.to_string()
// }

fn test_function(
    key_left: Option<&AnyValue>,
    key_right: Option<&AnyValue>,
    target_hash_left: Option<&AnyValue>,
    target_hash_right: Option<&AnyValue>,
    has_targets: bool,
) -> String {
    // TODONOW better error handling
    log::debug!("key left is: {:?}", key_left);
    log::debug!("key right is: {:?}", key_right);
    log::debug!("target hash left is: {:?}", target_hash_left);
    log::debug!("target hash right is: {:?}", target_hash_right);

    if let Some(key_left) = key_left {
        match key_left {
            AnyValue::Null => return DIFF_STATUS_ADDED.to_string(),
            _ => {}
        }
    }

    if let Some(key_right) = key_right {
        match key_right {
            AnyValue::Null => return DIFF_STATUS_REMOVED.to_string(),
            _ => {}
        }
    }

    if !has_targets {
        return DIFF_STATUS_UNCHANGED.to_string();
    }
    if let Some(target_hash_left) = target_hash_left {
        if let Some(target_hash_right) = target_hash_right {
            if target_hash_left != target_hash_right {
                return DIFF_STATUS_MODIFIED.to_string();
            }
        }
    }
    DIFF_STATUS_UNCHANGED.to_string()
}

// fn calculate_added_df(
//     df: &DataFrame,
//     keys: Vec<&str>,
//     output_columns: &Vec<String>,
// ) -> Result<DataFrame, OxenError> {
//     let hi = df.lazy().with_column(lit(NULL).alias("new col"))
//     .with_column()
//     );
//     let mut right_only = df.filter(&df.column(format!("{}.left", keys[0]).as_str())?.is_null())?;
//     for key in keys.iter() {
//         right_only.rename(&format!("{}.right", key), key)?;
//     }

//     let right_only = right_only.with_column(Series::new(
//         DIFF_STATUS_COL,
//         vec![DIFF_STATUS_ADDED; right_only.height()],
//     ))?;

//     Ok(right_only.select(output_columns)?)
// }

fn dummy_fn(a: String, b: String) -> String {
    format!("{}{}", a, b)
}
