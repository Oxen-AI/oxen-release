//! # api::local::diff
//!
//! Compare two files to find changes between them.
//!

use crate::core::db::{self, path_db};
use crate::model::diff::generic_diff_summary::GenericDiffSummary;
use rocksdb::{DBWithThreadMode, MultiThreaded};

use crate::core::df::tabular;
use crate::core::index::object_db_reader::ObjectDBReader;
use crate::core::index::CommitDirEntryReader;
use crate::error::OxenError;
use crate::model::diff::diff_entry_status::DiffEntryStatus;
use crate::model::diff::tabular_diff::{
    TabularDiff, TabularDiffMods, TabularDiffSummary, TabularSchemaDiff,
};
use crate::model::schema::Field;
use crate::model::{Commit, CommitEntry, DataFrameDiff, DiffEntry, LocalRepository, Schema};

use crate::{constants, util};

use crate::core::index::CommitEntryReader;
use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::model::diff::diff_commit_entry::DiffCommitEntry;
use crate::model::diff::diff_entries_counts::DiffEntriesCounts;
use crate::model::diff::schema_diff::SchemaDiff;
use crate::model::diff::AddRemoveModifyCounts;
use crate::model::diff::DiffResult;

use crate::opts::DFOpts;
use crate::view::compare::{CompareDupes, CompareTabular, CompareTabularWithDF};

pub mod join_diff;
pub mod utf8_diff;

const TARGETS_HASH_COL: &str = "_targets_hash";
const KEYS_HASH_COL: &str = "_keys_hash";

fn is_files_tabular(file_1: impl AsRef<Path>, file_2: impl AsRef<Path>) -> bool {
    util::fs::is_tabular(file_1.as_ref()) && util::fs::is_tabular(file_2.as_ref())
}
fn is_files_utf8(file_1: impl AsRef<Path>, file_2: impl AsRef<Path>) -> bool {
    util::fs::is_utf8(file_1.as_ref()) && util::fs::is_utf8(file_2.as_ref())
}

pub fn diff_files(
    file_1: impl AsRef<Path>,
    file_2: impl AsRef<Path>,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<DiffResult, OxenError> {
    if is_files_tabular(&file_1, &file_2) {
        let result = tabular(file_1, file_2, keys, targets, display)?;
        Ok(result)
    } else if is_files_utf8(&file_1, &file_2) {
        let result = utf8_diff::diff(file_1, file_2)?;
        Ok(DiffResult::Text(result))
    } else {
        Err(OxenError::invalid_file_type(format!(
            "Compare not supported for files, found {:?} and {:?}",
            file_1.as_ref(),
            file_2.as_ref()
        )))
    }
}

pub fn tabular(
    file_1: impl AsRef<Path>,
    file_2: impl AsRef<Path>,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<DiffResult, OxenError> {
    let df_1 = tabular::read_df(file_1, DFOpts::empty())?;
    let df_2 = tabular::read_df(file_2, DFOpts::empty())?;

    let schema_1 = Schema::from_polars(&df_1.schema());
    let schema_2 = Schema::from_polars(&df_2.schema());

    validate_required_fields(schema_1, schema_2, keys.clone(), targets.clone())?;

    let compare_df = compare_dfs(&df_1, &df_2, keys, targets, display)?;
    let compare_tabular = CompareTabular::from_with_df(&compare_df);

    let df = compare_df.diff_df;
    let summary = compare_tabular.summary.unwrap();
    let schema_diff = compare_df.schema_diff.unwrap();

    let col_changes = TabularSchemaDiff {
        added: schema_diff
            .added_cols
            .into_iter()
            .map(|c| Field {
                name: c.name,
                dtype: c.dtype,
                metadata: None,
            })
            .collect(),
        removed: schema_diff
            .removed_cols
            .into_iter()
            .map(|c| Field {
                name: c.name,
                dtype: c.dtype,
                metadata: None,
            })
            .collect(),
    };

    let mods = TabularDiffMods {
        col_changes,
        row_counts: AddRemoveModifyCounts {
            added: summary.modifications.added_rows,
            removed: summary.modifications.removed_rows,
            modified: summary.modifications.modified_rows,
        },
    };

    let diff_summary = TabularDiffSummary {
        modifications: mods,
        schema: Schema::from_polars(&df.schema()),
    };

    let tabular_diff = TabularDiff {
        summary: diff_summary,
        contents: df,
    };

    let result = DiffResult::Tabular(tabular_diff);
    Ok(result)
}

fn validate_required_fields(
    schema_1: Schema,
    schema_2: Schema,
    keys: Vec<String>,
    targets: Vec<String>,
) -> Result<(), OxenError> {
    // Keys must be in both dfs
    if !schema_1.has_field_names(&keys) {
        return Err(OxenError::incompatible_schemas(&keys, schema_1));
    };

    if !schema_2.has_field_names(&keys) {
        return Err(OxenError::incompatible_schemas(&keys, schema_2));
    };

    // Targets must be in either df
    for target in targets {
        if !schema_1.has_field_name(&target) && !schema_2.has_field_name(&target) {
            return Err(OxenError::incompatible_schemas(&[target], schema_1));
        }
    }

    Ok(())
}

fn compare_dfs(
    df_1: &DataFrame,
    df_2: &DataFrame,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<CompareTabularWithDF, OxenError> {
    let schema_diff = get_schema_diff(df_1, df_2);

    let (keys, targets) = get_keys_targets_smart_defaults(keys, targets, &schema_diff)?;
    let display = get_display_smart_defaults(&keys, &targets, display, &schema_diff);

    let (df_1, df_2) = hash_dfs(df_1.clone(), df_2.clone(), &keys, &targets)?;

    let mut compare = join_diff::diff(&df_1, &df_2, schema_diff, &keys, &targets, &display)?;

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

fn get_keys_targets_smart_defaults(
    keys: Vec<String>,
    targets: Vec<String>,
    schema_diff: &SchemaDiff,
) -> Result<(Vec<String>, Vec<String>), OxenError> {
    log::debug!(
        "get_keys_targets_smart_defaults keys {:?} targets {:?}",
        keys,
        targets
    );
    let has_keys = !keys.is_empty();
    let has_targets = !targets.is_empty();

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
    keys: &[String],
    targets: &[String],
    display: Vec<String>,
    schema_diff: &SchemaDiff,
) -> Vec<String> {
    if !display.is_empty() {
        return display;
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

fn hash_dfs(
    mut left_df: DataFrame,
    mut right_df: DataFrame,
    keys: &[String],
    targets: &[String],
) -> Result<(DataFrame, DataFrame), OxenError> {
    left_df = tabular::df_hash_rows_on_cols(left_df, targets, TARGETS_HASH_COL)?;
    right_df = tabular::df_hash_rows_on_cols(right_df, targets, TARGETS_HASH_COL)?;

    left_df = tabular::df_hash_rows_on_cols(left_df, keys, KEYS_HASH_COL)?;
    right_df = tabular::df_hash_rows_on_cols(right_df, keys, KEYS_HASH_COL)?;
    Ok((left_df, right_df))
}

pub fn count_added_rows(base_df: DataFrame, head_df: DataFrame) -> Result<usize, OxenError> {
    // Hash the rows
    let base_df = tabular::df_hash_rows(base_df)?;
    let head_df = tabular::df_hash_rows(head_df)?;

    // log::debug!("count_added_rows got base_df {}", base_df);
    // log::debug!("count_added_rows got head_df {}", head_df);

    let base_hash_indices: HashSet<String> = base_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    let head_hash_indices: HashSet<String> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    // Count the number of new rows
    let num_new_rows = head_hash_indices.difference(&base_hash_indices).count();
    // log::debug!("count_added_rows got num_new_rows {}", num_new_rows);
    Ok(num_new_rows)
}

pub fn count_removed_rows(base_df: DataFrame, head_df: DataFrame) -> Result<usize, OxenError> {
    // Hash the rows
    let base_df = tabular::df_hash_rows(base_df)?;
    let head_df = tabular::df_hash_rows(head_df)?;

    // log::debug!("count_removed_rows got base_df {}", base_df);
    // log::debug!("count_removed_rows got head_df {}", head_df);

    let base_hash_indices: HashSet<String> = base_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    let head_hash_indices: HashSet<String> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    // Count the number of removed rows
    let num_removed_rows = base_hash_indices.difference(&head_hash_indices).count();
    // log::debug!(
    //     "count_removed_rows got num_removed_rows {}",
    //     num_removed_rows
    // );
    Ok(num_removed_rows)
}

pub fn compute_new_row_indices(
    base_df: &DataFrame,
    head_df: &DataFrame,
) -> Result<(Vec<u32>, Vec<u32>), OxenError> {
    // Hash the rows
    let base_df = tabular::df_hash_rows(base_df.clone())?;
    let head_df = tabular::df_hash_rows(head_df.clone())?;

    log::debug!("diff_current got current hashes base_df {:?}", base_df);
    log::debug!("diff_current got current hashes head_df {:?}", head_df);

    let base_hash_indices: HashMap<String, u32> = base_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    let head_hash_indices: HashMap<String, u32> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    // Added is all the row hashes that are in head that are not in base
    let mut added_indices: Vec<u32> = head_hash_indices
        .iter()
        .filter(|(hash, _indices)| !base_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    added_indices.sort(); // so is deterministic and returned in correct order

    // Removed is all the row hashes that are in base that are not in head
    let mut removed_indices: Vec<u32> = base_hash_indices
        .iter()
        .filter(|(hash, _indices)| !head_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    removed_indices.sort(); // so is deterministic and returned in correct order

    log::debug!("diff_current added_indices {:?}", added_indices.len());
    log::debug!("diff_current removed_indices {:?}", removed_indices.len());

    Ok((added_indices, removed_indices))
}

pub fn compute_new_rows(
    base_df: &DataFrame,
    head_df: &DataFrame,
    schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    // Compute row indices
    let (added_indices, removed_indices) = compute_new_row_indices(base_df, head_df)?;

    // Take added from the current df
    let added_rows = if !added_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(schema);
        let head_df = tabular::transform(head_df.clone(), opts)?;
        Some(tabular::take(head_df.lazy(), added_indices)?)
    } else {
        None
    };
    log::debug!("diff_current added_rows {:?}", added_rows);

    // Take removed from versioned df
    let removed_rows = if !removed_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(schema);
        let base_df = tabular::transform(base_df.clone(), opts)?;
        Some(tabular::take(base_df.lazy(), removed_indices)?)
    } else {
        None
    };
    log::debug!("diff_current removed_rows {:?}", removed_rows);

    Ok(DataFrameDiff {
        head_schema: Some(schema.to_owned()),
        base_schema: Some(schema.to_owned()),
        added_rows,
        removed_rows,
        added_cols: None,
        removed_cols: None,
    })
}

pub fn compute_new_rows_proj(
    // the lowest common schema dataframes
    base_df: &DataFrame,
    head_df: &DataFrame,
    // the original dataframes
    proj_base_df: &DataFrame,
    proj_head_df: &DataFrame,
    // have to pass in the correct schemas to select the new rows
    base_schema: &Schema,
    head_schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    // Compute row indices
    let (added_indices, removed_indices) = compute_new_row_indices(base_df, head_df)?;

    // Take added from the current df
    let added_rows = if !added_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(head_schema);
        let proj_head_df = tabular::transform(proj_head_df.clone(), opts)?;
        Some(tabular::take(proj_head_df.lazy(), added_indices)?)
    } else {
        None
    };
    log::debug!("diff_current added_rows {:?}", added_rows);

    // Take removed from versioned df
    let removed_rows = if !removed_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(base_schema);
        let proj_base_df = tabular::transform(proj_base_df.clone(), opts)?;
        Some(tabular::take(proj_base_df.lazy(), removed_indices)?)
    } else {
        None
    };
    log::debug!("diff_current removed_rows {:?}", removed_rows);

    Ok(DataFrameDiff {
        head_schema: Some(base_schema.to_owned()),
        base_schema: Some(head_schema.to_owned()),
        added_rows,
        removed_rows,
        added_cols: None,
        removed_cols: None,
    })
}

pub fn compute_new_columns_from_dfs(
    base_df: DataFrame,
    head_df: DataFrame,
    base_schema: &Schema,
    head_schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    let added_fields = head_schema.added_fields(base_schema);
    let removed_fields = head_schema.removed_fields(base_schema);

    let added_cols = if !added_fields.is_empty() {
        let opts = DFOpts::from_columns(added_fields);
        let df_added = tabular::transform(head_df, opts)?;
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
        let opts = DFOpts::from_columns(removed_fields);
        let df_removed = tabular::transform(base_df, opts)?;
        log::debug!("Got removed col df: {}", df_removed);
        if df_removed.width() > 0 {
            Some(df_removed)
        } else {
            None
        }
    } else {
        None
    };

    Ok(DataFrameDiff {
        head_schema: Some(base_schema.to_owned()),
        base_schema: Some(base_schema.to_owned()),
        added_rows: None,
        removed_rows: None,
        added_cols,
        removed_cols,
    })
}

pub fn diff_entries(
    repo: &LocalRepository,
    base_entry: Option<CommitEntry>,
    base_commit: &Commit,
    head_entry: Option<CommitEntry>,
    head_commit: &Commit,
    df_opts: DFOpts,
) -> Result<DiffEntry, OxenError> {
    if base_entry.is_none() && head_entry.is_none() {
        return Err(OxenError::basic_str(
            "diff_entries called with no base or head entry",
        ));
    }

    // Assume both entries exist
    let mut status = DiffEntryStatus::Modified;

    // If base entry is none, then it was added
    if base_entry.is_none() && head_entry.is_some() {
        status = DiffEntryStatus::Added;
    }

    // If head entry is none, then it was removed
    if head_entry.is_none() && base_entry.is_some() {
        status = DiffEntryStatus::Removed;
    }

    let should_do_full_diff = true;

    let entry = DiffEntry::from_commit_entry(
        repo,
        base_entry,
        base_commit,
        head_entry,
        head_commit,
        status,
        should_do_full_diff,
        Some(df_opts),
    )?;

    Ok(entry)
}

pub fn list_diff_entries_in_dir(
    repo: &LocalRepository,
    dir: PathBuf,
    base_commit: &Commit,
    head_commit: &Commit,
    page: usize,
    page_size: usize,
) -> Result<DiffEntriesCounts, OxenError> {
    log::debug!(
        "list_top_level_diff_entries base_commit: '{}', head_commit: '{}'",
        base_commit,
        head_commit
    );

    let object_reader = ObjectDBReader::new(repo)?;
    let base_dir_reader =
        CommitDirEntryReader::new(repo, &base_commit.id, &dir, object_reader.clone())?;
    let head_dir_reader = CommitDirEntryReader::new(repo, &head_commit.id, &dir, object_reader)?;

    let base_entries = base_dir_reader.list_entries_set()?;
    let head_entries = head_dir_reader.list_entries_set()?;

    let base_dirs = base_dir_reader.list_dirs_set()?;
    let head_dirs = head_dir_reader.list_dirs_set()?;

    // TODO TBD: If the logic is an exact match, this can be deduped with list_diff_entries
    let mut dir_entries: Vec<DiffEntry> = vec![];
    collect_added_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;

    collect_removed_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;

    collect_modified_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;

    dir_entries.sort_by(|a, b| a.filename.cmp(&b.filename));

    let mut added_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_added_entries(&base_entries, &head_entries, &mut added_commit_entries)?;

    let mut removed_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_removed_entries(&base_entries, &head_entries, &mut removed_commit_entries)?;

    let mut modified_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_modified_entries(&base_entries, &head_entries, &mut modified_commit_entries)?;

    let counts = AddRemoveModifyCounts {
        added: added_commit_entries.len(),
        removed: removed_commit_entries.len(),
        modified: modified_commit_entries.len(),
    };

    let mut combined: Vec<_> = added_commit_entries
        .into_iter()
        .chain(removed_commit_entries)
        .chain(modified_commit_entries)
        .collect();

    combined.sort_by(|a, b| a.path.cmp(&b.path));

    let (files, pagination) =
        util::paginate::paginate_files_assuming_dirs(&combined, dir_entries.len(), page, page_size);

    let diff_entries: Vec<DiffEntry> = files
        .into_iter()
        .map(|entry| {
            DiffEntry::from_commit_entry(
                repo,
                entry.base_entry,
                base_commit,
                entry.head_entry,
                head_commit,
                entry.status,
                false,
                None,
            )
        })
        .collect::<Result<Vec<DiffEntry>, OxenError>>()?;

    let (dirs, _) =
        util::paginate::paginate_dirs_assuming_files(&dir_entries, combined.len(), page, page_size);

    let all = dirs.into_iter().chain(diff_entries).collect();

    Ok(DiffEntriesCounts {
        entries: all,
        counts,
        pagination,
    })
}

pub fn list_changed_dirs(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<(PathBuf, DiffEntryStatus)>, OxenError> {
    let mut changed_dirs: Vec<(PathBuf, DiffEntryStatus)> = vec![];
    let object_reader = ObjectDBReader::new(repo)?;

    let base_entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &base_commit.id, object_reader.clone())?;
    let head_entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &head_commit.id, object_reader)?;

    let base_dirs = base_entry_reader.list_dirs_set()?;
    let head_dirs = head_entry_reader.list_dirs_set()?;

    let base_dir_hashes_db_path = ObjectDBReader::commit_dir_hash_db(&repo.path, &base_commit.id);
    let head_dir_hashes_db_path = ObjectDBReader::commit_dir_hash_db(&repo.path, &head_commit.id);

    let base_dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(
        &db::opts::default(),
        dunce::simplified(&base_dir_hashes_db_path),
        false,
    )?;
    let head_dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(
        &db::opts::default(),
        dunce::simplified(&head_dir_hashes_db_path),
        false,
    )?;

    let added_dirs = head_dirs.difference(&base_dirs).collect::<HashSet<_>>();
    let removed_dirs = base_dirs.difference(&head_dirs).collect::<HashSet<_>>();
    let modified_or_unchanged_dirs = head_dirs.intersection(&base_dirs).collect::<HashSet<_>>();

    for dir in added_dirs.iter() {
        changed_dirs.push((dir.to_path_buf(), DiffEntryStatus::Added));
    }

    for dir in removed_dirs.iter() {
        changed_dirs.push((dir.to_path_buf(), DiffEntryStatus::Removed));
    }

    for dir in modified_or_unchanged_dirs.iter() {
        let base_dir_hash: Option<String> = path_db::get_entry(&base_dir_hashes_db, dir)?;
        let head_dir_hash: Option<String> = path_db::get_entry(&head_dir_hashes_db, dir)?;

        let base_dir_hash = match base_dir_hash {
            Some(base_dir_hash) => base_dir_hash,
            None => {
                return Err(OxenError::basic_str(
                    format!("Could not calculate dir diff tree: base_dir_hash not found for dir {:?} in commit {}",
                    dir, base_commit.id)
                ))
            }
        };

        let head_dir_hash = match head_dir_hash {
            Some(head_dir_hash) => head_dir_hash,
            None => {
                return Err(OxenError::basic_str(
                    format!("Could not calculate dir diff tree: head_dir_hash not found for dir {:?} in commit {}",
                    dir, head_commit.id)
                ))
            }
        };

        let base_dir_hash = base_dir_hash.to_string();
        let head_dir_hash = head_dir_hash.to_string();

        if base_dir_hash != head_dir_hash {
            changed_dirs.push((dir.to_path_buf(), DiffEntryStatus::Modified));
        }
    }

    // Sort by path for consistency
    changed_dirs.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(changed_dirs)
}

// For a rollup summary presented alongside the diffs WITHIN a dir, calculating added / removed / modified files from scratch
pub fn get_dir_diff_entry(
    repo: &LocalRepository,
    dir: PathBuf,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Option<DiffEntry>, OxenError> {
    // Dir hashes db is cheaper to open than objects reader
    let base_dir_hashes_db_path = ObjectDBReader::commit_dir_hash_db(&repo.path, &base_commit.id);
    let head_dir_hashes_db_path = ObjectDBReader::commit_dir_hash_db(&repo.path, &head_commit.id);

    let base_dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(
        &db::opts::default(),
        dunce::simplified(&base_dir_hashes_db_path),
        false,
    )?;

    let head_dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(
        &db::opts::default(),
        dunce::simplified(&head_dir_hashes_db_path),
        false,
    )?;

    let maybe_base_dir_hash: Option<String> = path_db::get_entry(&base_dir_hashes_db, &dir)?;
    let maybe_head_dir_hash: Option<String> = path_db::get_entry(&head_dir_hashes_db, &dir)?;

    match (maybe_base_dir_hash, maybe_head_dir_hash) {
        (Some(base_dir_hash), Some(head_dir_hash)) => {
            let base_dir_hash = base_dir_hash.to_string();
            let head_dir_hash = head_dir_hash.to_string();

            if base_dir_hash == head_dir_hash {
                Ok(None)
            } else {
                Ok(Some(DiffEntry::from_dir(
                    repo,
                    Some(&dir),
                    base_commit,
                    Some(&dir),
                    head_commit,
                    DiffEntryStatus::Modified,
                )?))
            }
        }
        (None, Some(_)) => Ok(Some(DiffEntry::from_dir(
            repo,
            None,
            base_commit,
            Some(&dir),
            head_commit,
            DiffEntryStatus::Added,
        )?)),
        (Some(_), None) => Ok(Some(DiffEntry::from_dir(
            repo,
            Some(&dir),
            base_commit,
            None,
            head_commit,
            DiffEntryStatus::Removed,
        )?)),
        (None, None) => Err(OxenError::basic_str(
            "Could not calculate dir diff tree: dir does not exist in either commit.",
        )),
    }
}

// Abbreviated version for when summary is known (e.g. computing top-level self node of an already-calculated dir diff)
pub fn get_dir_diff_entry_with_summary(
    repo: &LocalRepository,
    dir: PathBuf,
    base_commit: &Commit,
    head_commit: &Commit,
    summary: GenericDiffSummary,
) -> Result<Option<DiffEntry>, OxenError> {
    // Dir hashes db is cheaper to open than objects reader
    let base_dir_hashes_db_path = ObjectDBReader::commit_dir_hash_db(&repo.path, &base_commit.id);
    let head_dir_hashes_db_path = ObjectDBReader::commit_dir_hash_db(&repo.path, &head_commit.id);

    let base_dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(
        &db::opts::default(),
        dunce::simplified(&base_dir_hashes_db_path),
        false,
    )?;

    let head_dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(
        &db::opts::default(),
        dunce::simplified(&head_dir_hashes_db_path),
        false,
    )?;

    let maybe_base_dir_hash: Option<String> = path_db::get_entry(&base_dir_hashes_db, &dir)?;
    let maybe_head_dir_hash: Option<String> = path_db::get_entry(&head_dir_hashes_db, &dir)?;

    match (maybe_base_dir_hash, maybe_head_dir_hash) {
        (Some(base_dir_hash), Some(head_dir_hash)) => {
            let base_dir_hash = base_dir_hash.to_string();
            let head_dir_hash = head_dir_hash.to_string();

            if base_dir_hash == head_dir_hash {
                Ok(None)
            } else {
                Ok(Some(DiffEntry::from_dir_with_summary(
                    repo,
                    Some(&dir),
                    base_commit,
                    Some(&dir),
                    head_commit,
                    summary,
                    DiffEntryStatus::Modified,
                )?))
            }
        }
        (None, Some(_)) => Ok(Some(DiffEntry::from_dir_with_summary(
            repo,
            None,
            base_commit,
            Some(&dir),
            head_commit,
            summary,
            DiffEntryStatus::Added,
        )?)),
        (Some(_), None) => Ok(Some(DiffEntry::from_dir_with_summary(
            repo,
            Some(&dir),
            base_commit,
            None,
            head_commit,
            summary,
            DiffEntryStatus::Removed,
        )?)),
        (None, None) => Err(OxenError::basic_str(
            "Could not calculate dir diff tree: dir does not exist in either commit.",
        )),
    }
}

/// TODO this is very ugly...
pub fn list_diff_entries(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
    dir: PathBuf,
    page: usize,
    page_size: usize,
) -> Result<DiffEntriesCounts, OxenError> {
    log::debug!(
        "list_diff_entries base_commit: '{}', head_commit: '{}'",
        base_commit,
        head_commit
    );
    // BASE is what we are merging into, HEAD is where it is coming from
    // We want to find all the entries that are added, modified, removed HEAD but not in BASE

    // Read the entries from the base commit and the head commit
    log::debug!(
        "Reading entries from head commit {} -> {}",
        head_commit.id,
        head_commit.message
    );
    // let head_entries = read_entries_from_commit(repo, head_commit)?;
    let object_reader = ObjectDBReader::new(repo)?;

    let base_reader =
        CommitEntryReader::new_from_commit_id(repo, &base_commit.id, object_reader.clone())?;
    let head_reader =
        CommitEntryReader::new_from_commit_id(repo, &head_commit.id, object_reader.clone())?;

    let head_entries = head_reader.list_directory_set(&dir)?;
    let base_entries = base_reader.list_directory_set(&dir)?;

    let head_dirs = head_reader.list_dir_children_set(&dir)?;
    let base_dirs = base_reader.list_dir_children_set(&dir)?;

    log::debug!("Got {} head entries", head_entries.len());
    log::debug!(
        "Reading entries from base commit {} -> {}",
        base_commit.id,
        base_commit.message
    );

    log::debug!("Got {} base entries", base_entries.len());

    // let head_dirs = read_dirs_from_commit(repo, head_commit)?;

    log::debug!("Got {} head_dirs", head_dirs.len());

    // let base_dirs = read_dirs_from_commit(repo, base_commit)?;
    log::debug!("Got {} base_dirs", base_dirs.len());

    let mut dir_entries: Vec<DiffEntry> = vec![];
    collect_added_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;
    log::debug!("Collected {} added_dirs dir_entries", dir_entries.len());
    collect_removed_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;
    log::debug!("Collected {} removed_dirs dir_entries", dir_entries.len());
    collect_modified_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;
    dir_entries.sort_by(|a, b| a.filename.cmp(&b.filename));
    log::debug!("Collected {} modified_dirs dir_entries", dir_entries.len());

    // the DiffEntry takes a little bit of time to compute, so want to just find the commit entries
    // then filter them down to the ones we need
    let mut added_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_added_entries(&base_entries, &head_entries, &mut added_commit_entries)?;
    log::debug!(
        "Collected {} collect_added_entries",
        added_commit_entries.len()
    );

    let mut removed_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_removed_entries(&base_entries, &head_entries, &mut removed_commit_entries)?;
    log::debug!(
        "Collected {} collect_removed_entries",
        removed_commit_entries.len()
    );

    let mut modified_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_modified_entries(&base_entries, &head_entries, &mut modified_commit_entries)?;
    log::debug!(
        "Collected {} collect_modified_entries",
        modified_commit_entries.len()
    );
    let counts = AddRemoveModifyCounts {
        added: added_commit_entries.len(),
        removed: removed_commit_entries.len(),
        modified: modified_commit_entries.len(),
    };
    let mut combined: Vec<_> = added_commit_entries
        .into_iter()
        .chain(removed_commit_entries)
        .chain(modified_commit_entries)
        .collect();
    combined.sort_by(|a, b| a.path.cmp(&b.path));

    log::debug!("Got {} combined files", combined.len());

    let (files, pagination) =
        util::paginate::paginate_files_assuming_dirs(&combined, dir_entries.len(), page, page_size);
    log::debug!("Got {} initial dirs", dir_entries.len());
    log::debug!("Got {} files", files.len());

    let diff_entries: Vec<DiffEntry> = files
        .into_iter()
        .map(|entry| {
            DiffEntry::from_commit_entry(
                repo,
                entry.base_entry,
                base_commit,
                entry.head_entry,
                head_commit,
                entry.status,
                false,
                None,
            )
        })
        .collect::<Result<Vec<DiffEntry>, OxenError>>()?;

    let (dirs, _) =
        util::paginate::paginate_dirs_assuming_files(&dir_entries, combined.len(), page, page_size);
    log::debug!("Got {} filtered dirs", dirs.len());
    log::debug!("Page num {} Page size {}", page, page_size);

    let all = dirs.into_iter().chain(diff_entries).collect();

    Ok(DiffEntriesCounts {
        entries: all,
        counts,
        pagination,
    })
}

// Find the directories that are in HEAD but not in BASE
fn collect_added_directories(
    repo: &LocalRepository,
    base_dirs: &HashSet<PathBuf>,
    base_commit: &Commit,
    head_dirs: &HashSet<PathBuf>,
    head_commit: &Commit,
    diff_entries: &mut Vec<DiffEntry>,
) -> Result<(), OxenError> {
    for head_dir in head_dirs {
        // HEAD entry is *not* in BASE
        if !base_dirs.contains(head_dir) {
            diff_entries.push(DiffEntry::from_dir(
                repo,
                None,
                base_commit,
                Some(head_dir),
                head_commit,
                DiffEntryStatus::Added,
            )?);
        }
    }
    Ok(())
}

// Find the directories that are in HEAD and are in BASE
fn collect_modified_directories(
    repo: &LocalRepository,
    base_dirs: &HashSet<PathBuf>,
    base_commit: &Commit,
    head_dirs: &HashSet<PathBuf>,
    head_commit: &Commit,
    diff_entries: &mut Vec<DiffEntry>,
) -> Result<(), OxenError> {
    for head_dir in head_dirs {
        // HEAD entry is in BASE
        if base_dirs.contains(head_dir) {
            let diff_entry = DiffEntry::from_dir(
                repo,
                Some(head_dir),
                base_commit,
                Some(head_dir),
                head_commit,
                DiffEntryStatus::Modified,
            )?;

            if diff_entry.has_changes() {
                diff_entries.push(diff_entry);
            }
        }
    }
    Ok(())
}

// Find the directories that are in BASE but not in HEAD
fn collect_removed_directories(
    repo: &LocalRepository,
    base_dirs: &HashSet<PathBuf>,
    base_commit: &Commit,
    head_dirs: &HashSet<PathBuf>,
    head_commit: &Commit,
    diff_entries: &mut Vec<DiffEntry>,
) -> Result<(), OxenError> {
    // DEBUG
    // for base_dir in base_dirs.iter() {
    //     log::debug!(
    //         "collect_removed_directories BASE dir {}",
    //         base_dir.display()
    //     );
    // }

    // for head_dir in head_dirs.iter() {
    //     log::debug!(
    //         "collect_removed_directories HEAD dir {}",
    //         head_dir.display()
    //     );
    // }

    for base_dir in base_dirs {
        // HEAD entry is *not* in BASE
        if !head_dirs.contains(base_dir) {
            diff_entries.push(DiffEntry::from_dir(
                repo,
                Some(base_dir),
                base_commit,
                None,
                head_commit,
                DiffEntryStatus::Removed,
            )?);
        }
    }
    Ok(())
}

// Find the entries that are in HEAD but not in BASE
fn collect_added_entries(
    base_entries: &HashSet<CommitEntry>,
    head_entries: &HashSet<CommitEntry>,
    diff_entries: &mut Vec<DiffCommitEntry>,
) -> Result<(), OxenError> {
    log::debug!(
        "Computing difference for add entries head {} base {}",
        head_entries.len(),
        base_entries.len()
    );
    let diff = head_entries.difference(base_entries);
    for head_entry in diff {
        // HEAD entry is *not* in BASE
        diff_entries.push(DiffCommitEntry {
            path: head_entry.path.to_owned(),
            base_entry: None,
            head_entry: Some(head_entry.to_owned()),
            status: DiffEntryStatus::Added,
        });
    }
    Ok(())
}

// Find the entries that are in BASE but not in HEAD
fn collect_removed_entries(
    base_entries: &HashSet<CommitEntry>,
    head_entries: &HashSet<CommitEntry>,
    diff_entries: &mut Vec<DiffCommitEntry>,
) -> Result<(), OxenError> {
    for base_entry in base_entries {
        // BASE entry is *not* in HEAD
        if !head_entries.contains(base_entry) {
            diff_entries.push(DiffCommitEntry {
                path: base_entry.path.to_owned(),
                base_entry: Some(base_entry.to_owned()),
                head_entry: None,
                status: DiffEntryStatus::Removed,
            });
        }
    }
    Ok(())
}

// Find the entries that are in both base and head, but have different hashes
fn collect_modified_entries(
    base_entries: &HashSet<CommitEntry>,
    head_entries: &HashSet<CommitEntry>,
    diff_entries: &mut Vec<DiffCommitEntry>,
) -> Result<(), OxenError> {
    log::debug!(
        "collect_modified_entries modified entries base.len() {} head.len() {}",
        base_entries.len(),
        head_entries.len()
    );
    for head_entry in head_entries {
        // HEAD entry *is* in BASE
        if let Some(base_entry) = base_entries.get(head_entry) {
            // log::debug!(
            //     "collect_modified_entries found in base! {} != {}",
            //     head_entry.hash,
            //     base_entry.hash
            // );
            // HEAD entry has a different hash than BASE entry
            if head_entry.hash != base_entry.hash {
                diff_entries.push(DiffCommitEntry {
                    path: base_entry.path.to_owned(),
                    base_entry: Some(base_entry.to_owned()),
                    head_entry: Some(head_entry.to_owned()),
                    status: DiffEntryStatus::Modified,
                });
            }
        }
    }
    Ok(())
}

// TODO: linear scan is not the most efficient way to do this
pub fn get_add_remove_modify_counts(entries: &[DiffEntry]) -> AddRemoveModifyCounts {
    let mut added = 0;
    let mut removed = 0;
    let mut modified = 0;
    for entry in entries {
        if entry.is_dir {
            continue;
        }

        match DiffEntryStatus::from_str(&entry.status).unwrap() {
            DiffEntryStatus::Added => added += 1,
            DiffEntryStatus::Removed => removed += 1,
            DiffEntryStatus::Modified => modified += 1,
        }
    }
    AddRemoveModifyCounts {
        added,
        removed,
        modified,
    }
}

// fn read_dirs_from_commit(
//     repo: &LocalRepository,
//     commit: &Commit,
// ) -> Result<HashSet<PathBuf>, OxenError> {
//     let reader = CommitEntryReader::new(repo, commit)?;
//     let entries = reader.list_dirs()?;
//     Ok(HashSet::from_iter(
//         entries.into_iter().filter(|p| p != Path::new("")),
//     ))
// }

// fn read_entries_from_commit(
//     repo: &LocalRepository,
//     commit: &Commit,
// ) -> Result<HashSet<CommitEntry>, OxenError> {
//     let reader = CommitEntryReader::new(repo, commit)?;
//     let entries = reader.list_entries_set()?;
//     Ok(entries)
// }

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::model::diff::diff_entry_status::DiffEntryStatus;
    use crate::opts::RmOpts;
    use crate::test;
    use crate::util;

    #[test]
    fn test_diff_entries_add_multiple() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed(|repo| {
            // get og commit
            let base_commit = api::local::commits::head_commit(&repo)?;

            // add a new file
            let hello_file = repo.path.join("Hello.txt");
            let world_file = repo.path.join("World.txt");
            test::write_txt_file_to_path(&hello_file, "Hello")?;
            test::write_txt_file_to_path(&world_file, "World")?;

            command::add(&repo, &hello_file)?;
            command::add(&repo, &world_file)?;
            let head_commit = command::commit(&repo, "Adding two files")?;

            let entries = api::local::diff::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                0,
                10,
            )?;
            let entries = entries.entries;
            assert_eq!(2, entries.len());
            assert_eq!(DiffEntryStatus::Added.to_string(), entries[0].status);
            assert_eq!(DiffEntryStatus::Added.to_string(), entries[1].status);

            Ok(())
        })
    }

    #[test]
    fn test_diff_entries_modify_one_tabular() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed(|repo| {
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(bbox_filename);

            // get og commit
            let base_commit = api::local::commits::head_commit(&repo)?;

            // Remove a row
            let bbox_file = test::modify_txt_file(
                bbox_file,
                r"
file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
train/dog_2.jpg,dog,7.0,29.5,246,247
train/cat_2.jpg,cat,30.5,44.0,333,396
",
            )?;

            command::add(&repo, bbox_file)?;
            let head_commit = command::commit(&repo, "Removing a row from train bbox data")?;

            let entries = api::local::diff::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                0,
                10,
            )?;
            let entries = entries.entries;
            // Recursively marks parent dirs as modified
            assert_eq!(3, entries.len());
            for entry in entries.iter() {
                assert_eq!(DiffEntryStatus::Modified.to_string(), entry.status);
            }

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_diff_entries_remove_one_tabular() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // get og commit
            let base_commit = api::local::commits::head_commit(&repo)?;

            // Remove the file
            util::fs::remove_file(bbox_file)?;

            let opts = RmOpts::from_path(&bbox_filename);
            command::rm(&repo, &opts).await?;
            let head_commit = command::commit(&repo, "Removing a the training data file")?;
            let entries = api::local::diff::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                0,
                10,
            )?;

            let entries = entries.entries;
            for entry in entries.iter().enumerate() {
                println!("entry {}: {:?}", entry.0, entry.1);
            }

            // it currently shows all the parent dirs as being
            // CHANGE: through the merkle logic, this is now removing these directories...
            // do we want this?
            assert_eq!(3, entries.len());

            assert_eq!(entries[0].status, DiffEntryStatus::Removed.to_string());
            assert_eq!(entries[1].status, DiffEntryStatus::Removed.to_string());
            assert_eq!(entries[2].status, DiffEntryStatus::Removed.to_string());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_get_add_remove_modify_counts() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            // Get initial commit
            let base_commit = api::local::commits::head_commit(&repo)?;
            // Add two files
            let hello_file = repo.path.join("Hello.txt");
            let world_file = repo.path.join("World.txt");
            test::write_txt_file_to_path(&hello_file, "Hello")?;
            test::write_txt_file_to_path(&world_file, "World")?;

            command::add(&repo, &hello_file)?;
            command::add(&repo, &world_file)?;
            command::commit(&repo, "Removing a row from train bbox data")?;

            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // Remove the file
            util::fs::remove_file(bbox_file)?;

            let opts = RmOpts::from_path(&bbox_filename);
            command::rm(&repo, &opts).await?;
            let head_commit = command::commit(&repo, "Removing a the training data file")?;

            let entries = api::local::diff::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                0,
                10,
            )?;
            let entries = entries.entries;
            for entry in entries.iter().enumerate() {
                println!("entry {}: {:?}", entry.0, entry.1);
            }

            let counts = api::local::diff::get_add_remove_modify_counts(&entries);

            assert_eq!(5, entries.len());
            assert_eq!(2, counts.added);
            assert_eq!(1, counts.removed);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_in_dir_at_root() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            let new_bbox_filename = Path::new("annotations")
                .join("train")
                .join("new_bounding_box.csv");
            let new_bbox_file = repo.path.join(&new_bbox_filename);

            let new_root_filename = Path::new("READMENOW.md");

            let new_root_file = repo.path.join(new_root_filename);

            let add_dir = PathBuf::from("annotations").join("schmannotations");
            let add_dir_added_file = PathBuf::from("annotations")
                .join("schmannotations")
                .join("added_file.txt");

            let add_root_dir = PathBuf::from("not_annotations");
            let add_root_dir_added_file = PathBuf::from("not_annotations").join("added_file.txt");

            util::fs::create_dir_all(repo.path.join(add_dir))?;
            util::fs::create_dir_all(repo.path.join(add_root_dir))?;

            test::write_txt_file_to_path(&new_root_file, "Hello,world")?;
            test::write_txt_file_to_path(&new_bbox_file, "Hello,world")?;
            test::write_txt_file_to_path(repo.path.join(add_dir_added_file), "Hello,world!!")?;
            test::write_txt_file_to_path(repo.path.join(add_root_dir_added_file), "Hello,world!!")?;

            // get og commit
            let base_commit = api::local::commits::head_commit(&repo)?;

            // Remove the file
            util::fs::remove_file(bbox_file)?;

            let opts = RmOpts::from_path(&bbox_filename);
            command::rm(&repo, &opts).await?;
            command::add(&repo, &repo.path)?;
            let head_commit = command::commit(&repo, "Removing a the training data file")?;
            let entries = api::local::diff::list_diff_entries_in_dir(
                &repo,
                PathBuf::from(""),
                &base_commit,
                &head_commit,
                0,
                10,
            )?;

            let entries = entries.entries;

            let annotation_diff_entries = api::local::diff::list_diff_entries_in_dir(
                &repo,
                PathBuf::from("annotations"),
                &base_commit,
                &head_commit,
                0,
                10,
            )?;

            // We should have...
            // 1. A modification in the `annotations` directory
            // 2. The addition of the README.md file
            log::debug!("Got entries: {:?}", entries);

            assert_eq!(3, entries.len());

            assert_eq!(entries[0].status, DiffEntryStatus::Modified.to_string());
            assert_eq!(entries[1].status, DiffEntryStatus::Added.to_string());
            assert_eq!(entries[2].status, DiffEntryStatus::Added.to_string());

            // 1. Schmannotations dir added
            // 2. Train dir modified

            assert_eq!(2, annotation_diff_entries.entries.len());

            log::debug!(
                "Got annotation_diff_entries: {:?}",
                annotation_diff_entries.entries
            );

            assert_eq!(
                annotation_diff_entries.entries[0].status,
                DiffEntryStatus::Added.to_string()
            );
            assert_eq!(
                annotation_diff_entries.entries[1].status,
                DiffEntryStatus::Modified.to_string()
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_remove_one_tabular_in_dir() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // get og commit
            let base_commit = api::local::commits::head_commit(&repo)?;

            // Remove the file
            util::fs::remove_file(bbox_file)?;

            let opts = RmOpts::from_path(&bbox_filename);
            command::rm(&repo, &opts).await?;
            let head_commit = command::commit(&repo, "Removing a the training data file")?;
            let entries = api::local::diff::list_diff_entries_in_dir(
                &repo,
                PathBuf::from(""),
                &base_commit,
                &head_commit,
                0,
                10,
            )?;

            let entries = entries.entries;
            for entry in entries.iter().enumerate() {
                println!("entry {}: {:?}", entry.0, entry.1);
            }

            assert_eq!(1, entries.len());

            // Dir is removed because all its children were removed
            assert_eq!(entries[0].status, DiffEntryStatus::Removed.to_string());

            Ok(())
        })
        .await
    }
}
