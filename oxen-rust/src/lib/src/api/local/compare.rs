use crate::constants::{CACHE_DIR, COMPARES_DIR, LEFT_COMPARE_COMMIT, RIGHT_COMPARE_COMMIT};
use crate::core::df::tabular::{self};
use crate::error::OxenError;
use crate::model::entry::commit_entry::CompareEntry;
use crate::model::{CommitEntry, LocalRepository, Schema};
use crate::opts::DFOpts;

use crate::view::compare::{
    CompareDupes, CompareResult, CompareSchemaDiff, CompareSummary, CompareTabular,
    CompareTabularWithDF,
};
use crate::{api, util};

use polars::prelude::DataFrame;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub mod join_compare;
pub mod utf8_compare;

pub enum CompareStrategy {
    Hash,
    Join,
}

#[derive(Debug, Clone)]
pub struct SchemaDiff {
    added_cols: Vec<String>,
    removed_cols: Vec<String>,
    unchanged_cols: Vec<String>,
}

const TARGETS_HASH_COL: &str = "_targets_hash";
const KEYS_HASH_COL: &str = "_keys_hash";
const DUPES_PATH: &str = "dupes.json";

#[allow(clippy::too_many_arguments)]
pub fn compare_files(
    repo: &LocalRepository,
    compare_id: Option<&str>,
    compare_entry_1: CompareEntry,
    compare_entry_2: CompareEntry,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
    output: Option<PathBuf>,
) -> Result<CompareResult, OxenError> {
    log::debug!("comparing files");
    // Assert that the files exist in their respective commits.
    let file_1 = get_version_file(repo, &compare_entry_1)?;
    let file_2 = get_version_file(repo, &compare_entry_2)?;

    if is_files_tabular(&file_1, &file_2) {
        let result = compare_tabular(
            &file_1,
            &file_2,
            compare_entry_1,
            compare_entry_2,
            repo,
            compare_id,
            keys,
            targets,
            display,
            output,
        )?;

        Ok(CompareResult::Tabular(result))
    } else if is_files_utf8(&file_1, &file_2) {
        let result = utf8_compare::compare(&file_1, &file_2)?;

        Ok(CompareResult::Text(result))
    } else {
        Err(OxenError::invalid_file_type(format!(
            "Compare not supported for files, found {:?} and {:?}",
            compare_entry_1.path, compare_entry_2.path
        )))
    }
}

#[allow(clippy::too_many_arguments)]
fn compare_tabular(
    file_1: &Path,
    file_2: &Path,
    compare_entry_1: CompareEntry,
    compare_entry_2: CompareEntry,
    repo: &LocalRepository,
    compare_id: Option<&str>,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
    output: Option<PathBuf>,
) -> Result<(CompareTabular, DataFrame), OxenError> {
    let df_1 = tabular::read_df(file_1, DFOpts::empty())?;
    let df_2 = tabular::read_df(file_2, DFOpts::empty())?;

    let schema_1 = Schema::from_polars(&df_1.schema());
    let schema_2 = Schema::from_polars(&df_2.schema());

    validate_required_fields(schema_1, schema_2, keys.clone(), targets.clone())?;

    // TODO: Clean this up
    let keys = keys.iter().map(|key| key.as_str()).collect::<Vec<&str>>();
    let targets = targets
        .iter()
        .map(|target| target.as_str())
        .collect::<Vec<&str>>();
    let display = display
        .iter()
        .map(|display| display.as_str())
        .collect::<Vec<&str>>();

    let mut compare_tabular_raw = compute_row_comparison(&df_1, &df_2, &keys, &targets, &display)?;

    let compare = CompareTabular::from_with_df(&compare_tabular_raw);
    maybe_save_compare_output(&mut compare_tabular_raw, output)?;
    maybe_write_cache(
        repo,
        compare_id,
        compare_entry_1,
        compare_entry_2,
        &mut compare_tabular_raw,
    )?;

    Ok((compare, compare_tabular_raw.diff_df))
}

pub fn get_cached_compare(
    repo: &LocalRepository,
    compare_id: &str,
    compare_entry_1: CompareEntry,
    compare_entry_2: CompareEntry,
) -> Result<Option<CompareTabular>, OxenError> {
    // Check if commits have cahnged since LEFT and RIGHT files were last cached
    let (cached_left_id, cached_right_id) = get_compare_commit_ids(repo, compare_id)?;

    // If commits cache files do not exist or have changed since last hash (via branch name) then return None to recompute
    if cached_left_id.is_none() || cached_right_id.is_none() {
        return Ok(None);
    }

    // Issue with
    if compare_entry_1.commit_entry.is_none() || compare_entry_2.commit_entry.is_none() {
        return Ok(None);
    }

    // Checked these above
    let left_commit = compare_entry_1.commit_entry.unwrap();
    let right_commit = compare_entry_2.commit_entry.unwrap();

    // TODONOW this should be cached
    let left_full_df = tabular::read_df(
        api::local::diff::get_version_file_from_commit_id(
            repo,
            &left_commit.commit_id,
            &compare_entry_1.path,
        )?,
        DFOpts::empty(),
    )?;
    let right_full_df = tabular::read_df(
        api::local::diff::get_version_file_from_commit_id(
            repo,
            &right_commit.commit_id,
            &compare_entry_2.path,
        )?,
        DFOpts::empty(),
    )?;

    let schema_diff = CompareSchemaDiff::from_schemas(
        &Schema::from_polars(&left_full_df.schema()),
        &Schema::from_polars(&right_full_df.schema()),
    )?;

    let diff_df = tabular::read_df(get_compare_diff_path(repo, compare_id), DFOpts::empty())?;

    let compare_summary = CompareSummary::from_diff_df(&diff_df)?;

    // Don't have or need server-updated keys, targets, display on cache hit
    let compare_results = CompareTabular {
        dupes: read_dupes(repo, compare_id)?,
        schema_diff: Some(schema_diff),
        summary: Some(compare_summary),
        keys: None,
        targets: None,
        display: None,
    };

    Ok(Some(compare_results))
}

pub fn get_compare_dir(repo: &LocalRepository, compare_id: &str) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(CACHE_DIR)
        .join(COMPARES_DIR)
        .join(compare_id)
}

fn get_compare_diff_path(repo: &LocalRepository, compare_id: &str) -> PathBuf {
    let compare_dir = get_compare_dir(repo, compare_id);
    compare_dir.join("diff.parquet")
}

fn maybe_write_dupes(
    repo: &LocalRepository,
    compare_id: &str,
    dupes: &CompareDupes,
) -> Result<(), OxenError> {
    let compare_dir = get_compare_dir(repo, compare_id);

    if !compare_dir.exists() {
        std::fs::create_dir_all(&compare_dir)?;
    }

    let dupes_path = compare_dir.join(DUPES_PATH);

    std::fs::write(dupes_path, serde_json::to_string(&dupes)?)?;

    Ok(())
}

fn read_dupes(repo: &LocalRepository, compare_id: &str) -> Result<CompareDupes, OxenError> {
    let compare_dir = get_compare_dir(repo, compare_id);
    let dupes_path = compare_dir.join(DUPES_PATH);

    if !dupes_path.exists() {
        return Ok(CompareDupes::empty());
    }

    let dupes: CompareDupes = serde_json::from_str(&std::fs::read_to_string(dupes_path)?)?;

    Ok(dupes)
}

fn write_compare_dfs(
    repo: &LocalRepository,
    compare_id: &str,
    compare_tabular_raw: &mut CompareTabularWithDF,
) -> Result<(), OxenError> {
    let compare_dir = get_compare_dir(repo, compare_id);

    if !compare_dir.exists() {
        std::fs::create_dir_all(&compare_dir)?;
    }

    // TODONOW expensive clone
    let mut df = compare_tabular_raw.diff_df.clone();

    // TODONOW fix path
    let diff_path = get_compare_diff_path(repo, compare_id);

    tabular::write_df(&mut df, &diff_path)?;

    Ok(())
}

fn write_compare_commit_ids(
    repo: &LocalRepository,
    compare_id: &str,
    left_entry: &Option<CommitEntry>,
    right_entry: &Option<CommitEntry>,
) -> Result<(), OxenError> {
    let compare_dir = get_compare_dir(repo, compare_id);

    if !compare_dir.exists() {
        std::fs::create_dir_all(&compare_dir)?;
    }

    let left_path = compare_dir.join(LEFT_COMPARE_COMMIT);
    let right_path = compare_dir.join(RIGHT_COMPARE_COMMIT);

    if let Some(commit_entry) = left_entry {
        let left_id = &commit_entry.commit_id;
        std::fs::write(left_path, left_id)?;
    }

    if let Some(commit_entry) = right_entry {
        let right_id = &commit_entry.commit_id;
        std::fs::write(right_path, right_id)?;
    }

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

    Ok((Some(left_id), Some(right_id)))
}

fn compute_row_comparison(
    df_1: &DataFrame,
    df_2: &DataFrame,
    keys: &[&str],
    targets: &[&str],
    display: &[&str],
) -> Result<CompareTabularWithDF, OxenError> {
    let schema_diff = get_schema_diff(df_1, df_2);

    let targets = targets.to_owned();
    let keys = keys.to_owned();
    let display = display.to_owned();

    let (keys, targets) = get_keys_targets_smart_defaults(keys, targets, &schema_diff)?;
    let display = get_display_smart_defaults(display, &schema_diff, &keys, &targets);

    let keys = keys.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
    let targets = targets.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
    let display = display.iter().map(|s| s.as_str()).collect::<Vec<&str>>();

    let (df_1, df_2) = hash_dfs(df_1.clone(), df_2.clone(), keys.clone(), targets.clone())?;

    let mut compare = join_compare::compare(&df_1, &df_2, schema_diff, targets, keys, display)?;

    compare.dupes = CompareDupes {
        left: tabular::n_duped_rows(&df_1, &[KEYS_HASH_COL])?,
        right: tabular::n_duped_rows(&df_2, &[KEYS_HASH_COL])?,
    };

    Ok(compare)
}

fn get_schema_diff(df1: &DataFrame, df2: &DataFrame) -> SchemaDiff {
    let df1_cols = df1.get_column_names();
    let df2_cols = df2.get_column_names();

    let mut df1_set = HashSet::new();
    let mut df2_set = HashSet::new();

    for col in df1_cols.iter() {
        df1_set.insert(col);
    }

    for col in df2_cols.iter() {
        df2_set.insert(col);
    }

    let added_cols: Vec<String> = df2_set
        .difference(&df1_set)
        .map(|s| (*s).to_string())
        .collect();
    let removed_cols: Vec<String> = df1_set
        .difference(&df2_set)
        .map(|s| (*s).to_string())
        .collect();
    let unchanged_cols: Vec<String> = df1_set
        .intersection(&df2_set)
        .map(|s| (*s).to_string())
        .collect();

    SchemaDiff {
        added_cols,
        removed_cols,
        unchanged_cols,
    }
}

fn hash_dfs(
    mut left_df: DataFrame,
    mut right_df: DataFrame,
    keys: Vec<&str>,
    targets: Vec<&str>,
) -> Result<(DataFrame, DataFrame), OxenError> {
    left_df = tabular::df_hash_rows_on_cols(left_df, targets.clone(), TARGETS_HASH_COL)?;
    right_df = tabular::df_hash_rows_on_cols(right_df, targets.clone(), TARGETS_HASH_COL)?;

    left_df = tabular::df_hash_rows_on_cols(left_df, keys.clone(), KEYS_HASH_COL)?;
    right_df = tabular::df_hash_rows_on_cols(right_df, keys.clone(), KEYS_HASH_COL)?;
    Ok((left_df, right_df))
}

fn get_version_file(
    repo: &LocalRepository,
    compare_entry: &CompareEntry,
) -> Result<PathBuf, OxenError> {
    if let Some(commit_entry) = &compare_entry.commit_entry {
        api::local::diff::get_version_file_from_commit_id(
            repo,
            &commit_entry.commit_id,
            &commit_entry.path,
        )
    } else {
        Ok(compare_entry.path.clone())
    }
}

fn validate_required_fields(
    schema_1: Schema,
    schema_2: Schema,
    keys: Vec<String>,
    targets: Vec<String>,
) -> Result<(), OxenError> {
    // Subset dataframes to "keys" and "targets"
    #[allow(clippy::map_clone)]
    let required_fields = keys
        .iter()
        .chain(targets.iter())
        .cloned()
        .collect::<Vec<String>>();

    // Make sure both dataframes have all required fields

    if !schema_1.has_field_names(&required_fields) {
        return Err(OxenError::incompatible_schemas(required_fields, schema_1));
    };

    if !schema_2.has_field_names(&required_fields) {
        return Err(OxenError::incompatible_schemas(required_fields, schema_2));
    };

    Ok(())
}

fn maybe_write_cache(
    repo: &LocalRepository,
    compare_id: Option<&str>,
    compare_entry_1: CompareEntry,
    compare_entry_2: CompareEntry,
    compare_tabular_raw: &mut CompareTabularWithDF,
) -> Result<(), OxenError> {
    if let Some(compare_id) = compare_id {
        write_compare_commit_ids(
            repo,
            compare_id,
            &compare_entry_1.commit_entry,
            &compare_entry_2.commit_entry,
        )?;
        write_compare_dfs(repo, compare_id, compare_tabular_raw)?;
        maybe_write_dupes(repo, compare_id, &compare_tabular_raw.dupes)?;
    }

    Ok(())
}

fn maybe_save_compare_output(
    compare_tabular_raw: &mut CompareTabularWithDF,
    output: Option<PathBuf>,
) -> Result<(), OxenError> {
    let diff_df = &mut compare_tabular_raw.diff_df;

    let (df_1, file_name_1) = (diff_df, "diff.csv");

    // // Save to disk if we have an output - i.e., if called from API
    if let Some(output) = output {
        std::fs::create_dir_all(output.clone())?;
        let file_1_path = output.join(file_name_1);
        tabular::write_df(df_1, file_1_path.clone())?;
    }

    Ok(())
}

fn is_files_tabular(file_1: &Path, file_2: &Path) -> bool {
    util::fs::is_tabular(file_1) || util::fs::is_tabular(file_2)
}
fn is_files_utf8(file_1: &Path, file_2: &Path) -> bool {
    util::fs::is_utf8(file_1) && util::fs::is_utf8(file_2)
}

fn get_keys_targets_smart_defaults(
    keys: Vec<&str>,
    targets: Vec<&str>,
    schema_diff: &SchemaDiff,
) -> Result<(Vec<String>, Vec<String>), OxenError> {
    let has_keys = !keys.is_empty();
    let has_targets = !targets.is_empty();

    // Convert to string to avoid lifetime management
    let keys = keys.iter().map(|s| s.to_string()).collect::<Vec<String>>();
    let targets = targets
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    match (has_keys, has_targets) {
        (true, true) => Ok((keys, targets)),
        (true, false) => {
            let filled_targets = schema_diff
                .unchanged_cols
                .iter()
                .filter(|c| !keys.contains(c))
                .cloned()
                .collect();
            Ok((keys, filled_targets))
        }
        (false, true) => Err(OxenError::basic_str(
            "Must specify at least one key column if specifying target columns.",
        )),
        (false, false) => {
            let filled_keys = schema_diff.unchanged_cols.to_vec();

            let filled_targets = schema_diff
                .added_cols
                .iter()
                .chain(schema_diff.removed_cols.iter())
                .cloned()
                .collect();
            Ok((filled_keys, filled_targets))
        }
    }
}

fn get_display_smart_defaults(
    display: Vec<&str>,
    schema_diff: &SchemaDiff,
    keys: &[String],
    targets: &[String],
) -> Vec<String> {
    if !display.is_empty() {
        return display
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
    }

    // All non-key non-target columns, with the appropriate suffix(es)
    let mut display_default = vec![];
    for col in &schema_diff.unchanged_cols {
        if !keys.contains(col) && !targets.contains(col) {
            display_default.push(format!("{}.left", col));
            display_default.push(format!("{}.right", col));
        }
    }
    for col in &schema_diff.removed_cols {
        if !keys.contains(col) && !targets.contains(col) {
            display_default.push(format!("{}.left", col));
        }
    }

    for col in &schema_diff.added_cols {
        if !keys.contains(col) && !targets.contains(col) {
            display_default.push(format!("{}.right", col));
        }
    }

    display_default
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use polars::lazy::dsl::col;
    use polars::lazy::dsl::lit;
    use polars::lazy::frame::IntoLazy;

    use crate::api;
    use crate::command;
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::model::entry::commit_entry::CompareEntry;
    use crate::opts::DFOpts;
    use crate::test;
    use crate::view::compare::CompareResult;

    use super::get_compare_diff_path;

    #[test]
    fn test_compare_fails_when_not_tabular() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let hello_file = "train/dog_1.jpg";
            let world_file = "train/dog_2.jpg";

            test::test_img_file_with_name(hello_file);
            test::test_img_file_with_name(world_file);

            let hello_file = PathBuf::from(hello_file);
            let world_file = PathBuf::from(world_file);

            let head_commit = api::local::commits::head_commit(&repo)?;

            let keys = vec![];
            let targets = vec![];

            let entry_left =
                api::local::entries::get_commit_entry(&repo, &head_commit, &hello_file)?.unwrap();

            let entry_right =
                api::local::entries::get_commit_entry(&repo, &head_commit, &world_file)?.unwrap();

            let compare_entry_1 = CompareEntry {
                commit_entry: Some(entry_left),
                path: hello_file,
            };

            let compare_entry_2 = CompareEntry {
                commit_entry: Some(entry_right),
                path: world_file,
            };

            let result = api::local::compare::compare_files(
                &repo,
                None,
                compare_entry_1,
                compare_entry_2,
                keys,
                targets,
                vec![],
                None,
            );

            assert!(matches!(result.unwrap_err(), OxenError::InvalidFileType(_)));

            Ok(())
        })
    }

    #[test]
    fn test_compare_files() -> Result<(), OxenError> {
        test::run_compare_data_repo_test_fully_commited(|repo| {
            let left_file = PathBuf::from("compare_left.csv");
            let right_file = PathBuf::from("compare_right.csv");
            let head_commit = api::local::commits::head_commit(&repo)?;

            let entry_left =
                api::local::entries::get_commit_entry(&repo, &head_commit, &left_file)?.unwrap();

            let entry_right =
                api::local::entries::get_commit_entry(&repo, &head_commit, &right_file)?.unwrap();

            let compare_entry_1 = CompareEntry {
                commit_entry: Some(entry_left),
                path: left_file,
            };

            let compare_entry_2 = CompareEntry {
                commit_entry: Some(entry_right),
                path: right_file,
            };

            let compare_id = "savingforlater";

            let result = api::local::compare::compare_files(
                &repo,
                Some(compare_id),
                compare_entry_1,
                compare_entry_2,
                vec![
                    "height".to_string(),
                    "weight".to_string(),
                    "gender".to_string(),
                ],
                vec!["target".to_string(), "other_target".to_string()],
                vec![],
                None,
            )?;

            // Should be: 2 removed, 1 added, 6 unchanged, 5 modified

            if let CompareResult::Tabular((compare, _)) = result {
                // Get the actual df for this compare
                let df_path = get_compare_diff_path(&repo, compare_id);
                let df = tabular::read_df(&df_path, DFOpts::empty())?;

                let diff_col = ".oxen.diff.status";
                // Assert the overall height of the dataframe
                let added_df = df
                    .clone()
                    .lazy()
                    .filter(col(diff_col).eq(lit("added")))
                    .collect()?;
                let removed_df = df
                    .clone()
                    .lazy()
                    .filter(col(diff_col).eq(lit("removed")))
                    .collect()?;
                let modified_df = df
                    .clone()
                    .lazy()
                    .filter(col(diff_col).eq(lit("modified")))
                    .collect()?;
                let unchanged_df = df
                    .lazy()
                    .filter(col(diff_col).eq(lit("unchanged")))
                    .collect()?;

                assert_eq!(added_df.height(), 1);
                assert_eq!(removed_df.height(), 2);
                assert_eq!(modified_df.height(), 5);
            } else {
                assert_eq!(true, false, "Wrong result type for input files")
            }

            Ok(())
        })
    }

    #[test]
    fn test_compare_cache_miss_when_branch_ref_updates() -> Result<(), OxenError> {
        test::run_compare_data_repo_test_fully_commited(|repo| {
            let old_head = api::local::commits::head_commit(&repo)?;
            let left_file = PathBuf::from("compare_left.csv");
            let right_file = PathBuf::from("compare_right.csv");
            let left_entry =
                api::local::entries::get_commit_entry(&repo, &old_head, &left_file)?.unwrap();
            let right_entry =
                api::local::entries::get_commit_entry(&repo, &old_head, &right_file)?.unwrap();

            let compare_entry_1 = CompareEntry {
                commit_entry: Some(left_entry),
                path: left_file.clone(),
            };

            let compare_entry_2 = CompareEntry {
                commit_entry: Some(right_entry),
                path: right_file.clone(),
            };

            // Create compare on this commit
            api::local::compare::compare_files(
                &repo,
                Some("a_compare_id"),
                compare_entry_1.clone(),
                compare_entry_2.clone(),
                vec![
                    String::from("height"),
                    String::from("weight"),
                    String::from("gender"),
                ],
                vec![String::from("target"), String::from("other_target")],
                vec![],
                None,
            )?;

            // Check getting via cache
            let compare = api::local::compare::get_cached_compare(
                &repo,
                "a_compare_id",
                compare_entry_1,
                compare_entry_2,
            )?
            .unwrap();

            // Get the actual df for this compare
            let df_path = get_compare_diff_path(&repo, "a_compare_id");
            let df = tabular::read_df(&df_path, DFOpts::empty())?;

            let diff_col = ".oxen.diff.status";
            // Assert the overall height of the dataframe
            let added_df = df
                .clone()
                .lazy()
                .filter(col(diff_col).eq(lit("added")))
                .collect()?;
            let removed_df = df
                .clone()
                .lazy()
                .filter(col(diff_col).eq(lit("removed")))
                .collect()?;
            let modified_df = df
                .clone()
                .lazy()
                .filter(col(diff_col).eq(lit("modified")))
                .collect()?;

            assert_eq!(added_df.height(), 1);
            assert_eq!(removed_df.height(), 2);
            assert_eq!(modified_df.height(), 5);

            // Update one of the files
            let path = Path::new("compare_left.csv");
            let file_path = repo.path.join(path);
            let mut df = tabular::read_df(&file_path, DFOpts::empty())?;
            df = df.slice(0, 6);
            tabular::write_df(&mut df, &file_path)?;

            // Commit the new modification
            command::add(&repo, &repo.path)?;
            command::status(&repo)?;
            command::commit(&repo, "updating compare_left.csv")?;

            // Get new entries and check the cached compare
            let new_head = api::local::commits::head_commit(&repo)?;
            let new_left_entry = api::local::entries::get_commit_entry(
                &repo,
                &new_head,
                &PathBuf::from("compare_left.csv"),
            )?
            .unwrap();
            let new_right_entry = api::local::entries::get_commit_entry(
                &repo,
                &new_head,
                &PathBuf::from("compare_right.csv"),
            )?
            .unwrap();

            let new_compare_entry_1 = CompareEntry {
                commit_entry: Some(new_left_entry),
                path: left_file,
            };

            let new_compare_entry_2 = CompareEntry {
                commit_entry: Some(new_right_entry),
                path: right_file,
            };

            let maybe_compare = api::local::compare::get_cached_compare(
                &repo,
                "no_id",
                new_compare_entry_1.clone(),
                new_compare_entry_2.clone(),
            )?;
            assert!(maybe_compare.is_none());

            // Create the compare and add to the cache to ensure proper update
            api::local::compare::compare_files(
                &repo,
                Some("a_compare_id"),
                new_compare_entry_1,
                new_compare_entry_2,
                vec![
                    String::from("height"),
                    String::from("weight"),
                    String::from("gender"),
                ],
                vec![String::from("target"), String::from("other_target")],
                vec![],
                None,
            )?;

            // Get the actual df for this compare
            let df_path = get_compare_diff_path(&repo, "a_compare_id");
            let df = tabular::read_df(&df_path, DFOpts::empty())?;

            let diff_col = ".oxen.diff.status";
            // Assert the overall height of the dataframe
            let added_df = df
                .clone()
                .lazy()
                .filter(col(diff_col).eq(lit("added")))
                .collect()?;
            let removed_df = df
                .clone()
                .lazy()
                .filter(col(diff_col).eq(lit("removed")))
                .collect()?;
            let modified_df = df
                .clone()
                .lazy()
                .filter(col(diff_col).eq(lit("modified")))
                .collect()?;

            assert_eq!(added_df.height(), 6);
            assert_eq!(removed_df.height(), 0);
            assert_eq!(modified_df.height(), 0);
            Ok(())
        })
    }
}
