use serde::{Deserialize, Serialize};

use crate::core::df::tabular;
use crate::core::index::CommitDirEntryReader;
use crate::error::OxenError;
use crate::model::diff::diff_entry_status::DiffEntryStatus;
use crate::model::{Commit, CommitEntry, DataFrameDiff, DiffEntry, LocalRepository, Schema};
use crate::opts::DFOpts;
use crate::view::compare::AddRemoveModifyCounts;
use crate::view::Pagination;
use crate::{constants, util};

use crate::core::index::CommitEntryReader;
use colored::Colorize;
use difference::{Changeset, Difference};
use polars::export::ahash::HashMap;
use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub struct EntriesDiff {
    pub entries: Vec<DiffEntry>,
    pub counts: AddRemoveModifyCounts,
    pub pagination: Pagination,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct DiffCommitEntry {
    pub status: DiffEntryStatus,
    // path for sorting so we don't have to dive into the optional commit entries
    pub path: PathBuf,

    // CommitEntry
    pub head_entry: Option<CommitEntry>,
    pub base_entry: Option<CommitEntry>,
}

/// Get a String representation of a diff between two commits given a file
pub fn single_diff_str(
    repo: &LocalRepository,
    original: &Commit,
    compare: &Commit,
    path: impl AsRef<Path>,
) -> Result<String, OxenError> {
    let base_path = get_version_file_from_commit(repo, original, &path)?;
    let head_path = get_version_file_from_commit(repo, compare, &path)?;
    diff_files(base_path, head_path)
}

pub fn get_version_file_from_commit(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();

    // Extract parent so we can use it to instantiate CommitDirEntryReader
    let parent = match path.parent() {
        Some(parent) => parent,
        None => return Err(OxenError::file_has_no_parent(path)),
    };

    // Instantiate CommitDirEntryReader to fetch entry
    let relative_parent = util::fs::path_relative_to_dir(parent, &repo.path)?;
    let commit_entry_reader = CommitDirEntryReader::new(repo, &commit.id, &relative_parent)?;
    let file_name = match path.file_name() {
        Some(file_name) => file_name,
        None => return Err(OxenError::file_has_no_name(path)),
    };

    // Get entry from the reader
    let entry = match commit_entry_reader.get_entry(file_name) {
        Ok(Some(entry)) => entry,
        _ => return Err(OxenError::entry_does_not_exist_in_commit(path, &commit.id)),
    };

    Ok(util::fs::version_path(repo, &entry))
}

pub fn diff_files(
    original: impl AsRef<Path>,
    compare: impl AsRef<Path>,
) -> Result<String, OxenError> {
    let original = original.as_ref();
    let compare = compare.as_ref();
    if util::fs::is_tabular(original) && util::fs::is_tabular(compare) {
        let tabular_diff = diff_tabular(original, compare)?;
        return Ok(tabular_diff.to_string());
    } else if util::fs::is_utf8(original) && util::fs::is_utf8(compare) {
        return diff_utf8(original, compare);
    }
    Err(OxenError::basic_str(format!(
        "Diff not supported for files: {original:?} and {compare:?}"
    )))
}

pub fn diff_utf8(
    original: impl AsRef<Path>,
    compare: impl AsRef<Path>,
) -> Result<String, OxenError> {
    let original = original.as_ref();
    let compare = compare.as_ref();
    let original_data = util::fs::read_from_path(original)?;
    let compare_data = util::fs::read_from_path(compare)?;
    let Changeset { diffs, .. } = Changeset::new(&original_data, &compare_data, "\n");

    let mut outputs: Vec<String> = vec![];
    for diff in diffs {
        match diff {
            Difference::Same(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!(" {split}\n").normal().to_string());
                }
            }
            Difference::Add(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!("+{split}\n").green().to_string());
                }
            }
            Difference::Rem(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!("-{split}\n").red().to_string());
                }
            }
        }
    }

    Ok(outputs.join(""))
}

pub fn diff_tabular(
    base_path: impl AsRef<Path>,
    head_path: impl AsRef<Path>,
) -> Result<DataFrameDiff, OxenError> {
    let base_path = base_path.as_ref();
    let head_path = head_path.as_ref();
    // Make sure files exist
    if !base_path.exists() {
        return Err(OxenError::entry_does_not_exist(base_path));
    }

    if !head_path.exists() {
        return Err(OxenError::entry_does_not_exist(head_path));
    }

    // Read DFs and get schemas
    let base_df = tabular::read_df(base_path, DFOpts::empty())?;
    let head_df = tabular::read_df(head_path, DFOpts::empty())?;
    let base_schema = Schema::from_polars(&base_df.schema());
    let head_schema = Schema::from_polars(&head_df.schema());

    log::debug!(
        "Original df {} {base_path:?}\n{base_df:?}",
        base_schema.hash
    );
    log::debug!("Compare df {} {head_path:?}\n{head_df:?}", head_schema.hash);

    // If schemas don't match, figure out which columns are different
    if base_schema.hash != head_schema.hash {
        compute_new_columns_from_paths(base_path, head_path, &base_schema, &head_schema)
    } else {
        log::debug!("Computing diff for {base_path:?} to {head_path:?}");
        compute_new_rows(base_df, head_df, &base_schema)
    }
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
        .utf8()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    let head_hash_indices: HashSet<String> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
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
        .utf8()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    let head_hash_indices: HashSet<String> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
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

pub fn compute_new_rows(
    base_df: DataFrame,
    head_df: DataFrame,
    schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    // Hash the rows
    let base_df = tabular::df_hash_rows(base_df)?;
    let head_df = tabular::df_hash_rows(head_df)?;

    // log::debug!("diff_current got current hashes {}", current_df);

    let base_hash_indices: HashMap<String, u32> = base_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    let head_hash_indices: HashMap<String, u32> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
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

    // log::debug!("diff_current added_indices {:?}", added_indices);

    // log::debug!("diff_current removed_indices {:?}", removed_indices);

    // Take added from the current df
    let opts = DFOpts::from_schema_columns(schema);
    let head_df = tabular::transform(head_df, opts)?;
    let added_rows = tabular::take(head_df.lazy(), added_indices)?;

    // Take removed from versioned df
    let opts = DFOpts::from_schema_columns(schema);
    let base_df = tabular::transform(base_df, opts)?;
    let removed_rows = tabular::take(base_df.lazy(), removed_indices)?;

    Ok(DataFrameDiff {
        head_schema: Some(schema.to_owned()),
        base_schema: Some(schema.to_owned()),
        added_rows: if added_rows.height() > 0 {
            Some(added_rows)
        } else {
            None
        },
        removed_rows: if removed_rows.height() > 0 {
            Some(removed_rows)
        } else {
            None
        },
        added_cols: None,
        removed_cols: None,
    })
}

pub fn compute_new_columns_from_paths(
    base_path: &Path,
    head_path: &Path,
    base_schema: &Schema,
    head_schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    let added_fields = head_schema.added_fields(base_schema);
    let removed_fields = head_schema.removed_fields(base_schema);

    let added_cols = if !added_fields.is_empty() {
        let opts = DFOpts::from_columns(added_fields);
        let df_added = tabular::read_df(head_path, opts)?;
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
        let df_removed = tabular::read_df(base_path, opts)?;
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
    );

    Ok(entry)
}

/// TODO this is insane. Need more efficient data structure or to use a database like duckdb.
pub fn list_diff_entries(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
    page: usize,
    page_size: usize,
) -> Result<EntriesDiff, OxenError> {
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
    let head_entries = read_entries_from_commit(repo, head_commit)?;
    log::debug!("Got {} head entries", head_entries.len());
    log::debug!(
        "Reading entries from base commit {} -> {}",
        base_commit.id,
        base_commit.message
    );
    let base_entries = read_entries_from_commit(repo, base_commit)?;
    log::debug!("Got {} base entries", base_entries.len());

    let head_dirs = read_dirs_from_commit(repo, head_commit)?;
    log::debug!("Got {} head_dirs", head_dirs.len());

    let base_dirs = read_dirs_from_commit(repo, base_commit)?;
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
        .collect();

    let (dirs, _) =
        util::paginate::paginate_dirs_assuming_files(&dir_entries, combined.len(), page, page_size);
    log::debug!("Got {} filtered dirs", dirs.len());
    log::debug!("Page num {} Page size {}", page, page_size);

    let all = dirs.into_iter().chain(diff_entries).collect();

    Ok(EntriesDiff {
        entries: all,
        counts,
        pagination,
    })
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
    for base_dir in base_dirs.iter() {
        log::debug!(
            "collect_removed_directories BASE dir {}",
            base_dir.display()
        );
    }

    for head_dir in head_dirs.iter() {
        log::debug!(
            "collect_removed_directories HEAD dir {}",
            head_dir.display()
        );
    }

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
    log::debug!("done difference for add entries");
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

fn read_dirs_from_commit(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<HashSet<PathBuf>, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    let entries = reader.list_dirs()?;
    Ok(HashSet::from_iter(
        entries.into_iter().filter(|p| p != Path::new("")),
    ))
}

fn read_entries_from_commit(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<HashSet<CommitEntry>, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    let entries = reader.list_entries_set()?;
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

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

            let entries =
                api::local::diff::list_diff_entries(&repo, &base_commit, &head_commit, 0, 10)?;
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

            let entries =
                api::local::diff::list_diff_entries(&repo, &base_commit, &head_commit, 0, 10)?;
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

            let entries =
                api::local::diff::list_diff_entries(&repo, &base_commit, &head_commit, 0, 10)?;
            let entries = entries.entries;
            for entry in entries.iter().enumerate() {
                println!("entry {}: {:?}", entry.0, entry.1);
            }

            // it currently shows all the parent dirs as being modified
            assert_eq!(3, entries.len());

            assert_eq!(entries[0].status, DiffEntryStatus::Modified.to_string());
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

            let entries =
                api::local::diff::list_diff_entries(&repo, &base_commit, &head_commit, 0, 10)?;
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
}
