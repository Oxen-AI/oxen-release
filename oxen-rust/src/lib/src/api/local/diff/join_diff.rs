use std::collections::{HashMap, HashSet};

use crate::error::OxenError;
use crate::model::compare::tabular_compare::{
    TabularCompareFieldBody, TabularCompareFields, TabularCompareTargetBody,
};
use crate::model::Schema;
use crate::view::compare::{
    CompareDupes, CompareSchemaColumn, CompareSchemaDiff, CompareSourceSchemas, CompareSummary,
    CompareTabularMods, CompareTabularWithDF,
};

use polars::datatypes::{AnyValue, StringChunked};
use polars::lazy::dsl::coalesce;
use polars::lazy::dsl::{all, as_struct, col, GetOutput};
use polars::lazy::frame::IntoLazy;
use polars::prelude::ChunkCompare;
use polars::prelude::{DataFrame, DataFrameJoinOps};
use polars::series::IntoSeries;

use super::SchemaDiff;

const TARGETS_HASH_COL: &str = "_targets_hash";
const KEYS_HASH_COL: &str = "_keys_hash";
const DIFF_STATUS_COL: &str = ".oxen.diff.status";

const DIFF_STATUS_ADDED: &str = "added";
const DIFF_STATUS_REMOVED: &str = "removed";
const DIFF_STATUS_MODIFIED: &str = "modified";
const DIFF_STATUS_UNCHANGED: &str = "unchanged";

pub fn diff(
    df_1: &DataFrame,
    df_2: &DataFrame,
    schema_diff: SchemaDiff,
    keys: &[impl AsRef<str>],
    targets: &[impl AsRef<str>],
    display: &[impl AsRef<str>],
) -> Result<CompareTabularWithDF, OxenError> {
    if !targets.is_empty() && keys.is_empty() {
        let targets = targets.iter().map(|k| k.as_ref()).collect::<Vec<&str>>();
        return Err(OxenError::basic_str(
            format!("Must specify at least one key column if specifying target columns. Targets: {targets:?}"),
        ));
    }

    let keys: Vec<&str> = keys.iter().map(|k| k.as_ref()).collect();
    let targets: Vec<&str> = targets.iter().map(|k| k.as_ref()).collect();
    let display: Vec<&str> = display.iter().map(|k| k.as_ref()).collect();

    let output_columns = get_output_columns(
        &Schema::from_polars(&df_2.schema()),
        keys.clone(),
        targets.clone(),
        display.clone(),
        schema_diff.clone(),
    );

    let joined_df = join_hashed_dfs(
        df_1,
        df_2,
        keys.clone(),
        targets.clone(),
        schema_diff.clone(),
    )?;

    let joined_df = add_diff_status_column(joined_df, keys.clone(), targets.clone())?;

    let mut joined_df = joined_df.filter(
        &joined_df
            .column(DIFF_STATUS_COL)?
            .not_equal(DIFF_STATUS_UNCHANGED)?,
    )?;

    // Once we've joined and calculated group membership based on .left and .right nullity, coalesce keys
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

    let modifications = calculate_compare_mods(&joined_df)?;

    let descending = keys.iter().map(|_| false).collect::<Vec<bool>>();
    let joined_df = joined_df.sort(&keys, descending, false)?;

    let result_fields =
        prepare_response_fields(&schema_diff, keys.clone(), targets.clone(), display);

    let schema_diff = build_compare_schema_diff(schema_diff, df_1, df_2)?;

    let source_schemas = CompareSourceSchemas {
        left: Schema::from_polars(&df_1.schema()),
        right: Schema::from_polars(&df_2.schema()),
    };

    Ok(CompareTabularWithDF {
        diff_df: joined_df.select(output_columns)?,
        dupes: CompareDupes { left: 0, right: 0 },
        schema_diff: Some(schema_diff),
        source_schemas,
        summary: Some(CompareSummary {
            modifications,
            schema: Schema::from_polars(&joined_df.schema()),
        }),
        keys: result_fields.keys,
        targets: result_fields.targets,
        display: result_fields.display,
    })
}

fn build_compare_schema_diff(
    schema_diff: SchemaDiff,
    df_1: &DataFrame,
    df_2: &DataFrame,
) -> Result<CompareSchemaDiff, OxenError> {
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
    df2_schema: &Schema, // For consistent column ordering
    keys: Vec<&str>,
    targets: Vec<&str>,
    display: Vec<&str>,
    schema_diff: SchemaDiff,
) -> Vec<String> {
    // Keys in df2 order. Targets in df2 order, then any additional. Then display in df2, then any additional.
    let df2_cols_set: HashSet<&str> = df2_schema.fields.iter().map(|f| f.name.as_str()).collect();

    // Get the column index of each column in df2schema
    let mut col_indices: HashMap<&str, usize> = HashMap::new();
    for (i, col) in df2_schema.fields.iter().enumerate() {
        col_indices.insert(col.name.as_str(), i);
    }

    let mut out_columns = vec![];

    let ordered_keys = order_columns_by_schema(keys, &df2_cols_set, &col_indices);
    let ordered_targets = order_columns_by_schema(targets, &df2_cols_set, &col_indices);
    let ordered_display = order_columns_by_schema(display, &df2_cols_set, &col_indices);

    for key in ordered_keys.iter() {
        out_columns.push(key.to_string());
    }

    for target in ordered_targets.iter() {
        if schema_diff.added_cols.contains(&target.to_string()) {
            out_columns.push(format!("{}.right", target));
        } else if schema_diff.removed_cols.contains(&target.to_string()) {
            out_columns.push(format!("{}.left", target));
        } else {
            out_columns.push(format!("{}.left", target));
            out_columns.push(format!("{}.right", target))
        };
    }

    for col in ordered_display.iter() {
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

fn order_columns_by_schema<'a>(
    columns: Vec<&'a str>,
    df2_cols_set: &HashSet<&'a str>,
    col_indices: &HashMap<&'a str, usize>,
) -> Vec<&'a str> {
    let mut ordered_columns: Vec<&'a str> = columns
        .iter()
        .filter(|col| df2_cols_set.contains(*col))
        .cloned()
        .collect();

    ordered_columns.sort_by_key(|col| *col_indices.get(col).unwrap_or(&usize::MAX));

    ordered_columns
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

    for col in schema_diff.added_cols.iter() {
        if joined_df.schema().contains(col) {
            joined_df.rename(col, &format!("{}.right", col))?;
        }
    }

    for col in schema_diff.removed_cols.iter() {
        if joined_df.schema().contains(col) {
            joined_df.rename(col, &format!("{}.left", col))?;
        }
    }

    for target in cols_to_rename.iter() {
        log::debug!("trying to rename col: {}", target);
        let left_before = target.to_string();
        let left_after = format!("{}.left", target);
        let right_before = format!("{}_right", target);
        let right_after = format!("{}.right", target);
        // Rename conditionally for asymetric targets
        if joined_df.schema().contains(&left_before) {
            joined_df.rename(&left_before, &left_after)?;
        }
        if joined_df.schema().contains(&right_before) {
            joined_df.rename(&right_before, &right_after)?;
        }
    }

    Ok(joined_df)
}

fn add_diff_status_column(
    joined_df: DataFrame,
    keys: Vec<&str>,
    targets: Vec<&str>,
) -> Result<DataFrame, OxenError> {
    // Columns required for determining group membership in the closure
    let col_names = [
        format!("{}.left", keys[0]),
        format!("{}.right", keys[0]),
        format!("{}.left", TARGETS_HASH_COL),
        format!("{}.right", TARGETS_HASH_COL),
    ];

    let mut field_names = vec![];
    for col_name in &col_names {
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
                                let key_left = row.first();
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

    Ok(joined_df)
}

fn calculate_compare_mods(joined_df: &DataFrame) -> Result<CompareTabularMods, OxenError> {
    // TODO: for reasons which are unclear to me it is ridiculously unclear how
    // to use the polars DSL to get the added, removed, and modified rows as a scalary without
    // filtering down into sub-dataframes or cloning them. This is a workaround for now.

    let mut added_rows = 0;
    let mut removed_rows = 0;
    let mut modified_rows = 0;

    for row in joined_df.column(DIFF_STATUS_COL)?.str()?.into_iter() {
        match row {
            Some("added") => added_rows += 1,
            Some("removed") => removed_rows += 1,
            Some("modified") => modified_rows += 1,
            _ => (),
        }
    }
    Ok(CompareTabularMods {
        added_rows,
        removed_rows,
        modified_rows,
    })
}

fn test_function(
    key_left: Option<&AnyValue>,
    key_right: Option<&AnyValue>,
    target_hash_left: Option<&AnyValue>,
    target_hash_right: Option<&AnyValue>,
    has_targets: bool,
) -> String {
    // TODONOW better error handling
    if let Some(AnyValue::Null) = key_left {
        return DIFF_STATUS_ADDED.to_string();
    }

    if let Some(AnyValue::Null) = key_right {
        return DIFF_STATUS_REMOVED.to_string();
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

fn prepare_response_fields(
    schema_diff: &SchemaDiff,
    keys: Vec<&str>,
    targets: Vec<&str>,
    display: Vec<&str>,
) -> TabularCompareFields {
    let res_keys = keys
        .iter()
        .map(|key| TabularCompareFieldBody {
            left: key.to_string(),
            right: key.to_string(),
            alias_as: None,
            compare_method: None,
        })
        .collect::<Vec<TabularCompareFieldBody>>();

    let mut res_targets: Vec<TabularCompareTargetBody> = vec![];

    for target in targets.iter() {
        if schema_diff.added_cols.contains(&target.to_string()) {
            res_targets.push(TabularCompareTargetBody {
                left: None,
                right: Some(target.to_string()),
                compare_method: None,
            });
        } else if schema_diff.removed_cols.contains(&target.to_string()) {
            res_targets.push(TabularCompareTargetBody {
                left: Some(target.to_string()),
                right: None,
                compare_method: None,
            });
        } else {
            res_targets.push(TabularCompareTargetBody {
                left: Some(target.to_string()),
                right: Some(target.to_string()),
                compare_method: None,
            });
        }
    }

    let mut res_display: Vec<TabularCompareTargetBody> = vec![];
    for disp in display.iter() {
        if schema_diff.added_cols.contains(&disp.to_string()) {
            res_display.push(TabularCompareTargetBody {
                left: None,
                right: Some(disp.to_string()),
                compare_method: None,
            });
        } else if schema_diff.removed_cols.contains(&disp.to_string()) {
            res_display.push(TabularCompareTargetBody {
                left: Some(disp.to_string()),
                right: None,
                compare_method: None,
            });
        } else {
            res_display.push(TabularCompareTargetBody {
                left: Some(disp.to_string()),
                right: Some(disp.to_string()),
                compare_method: None,
            });
        }
    }

    TabularCompareFields {
        keys: res_keys,
        targets: res_targets,
        display: res_display,
    }
}
