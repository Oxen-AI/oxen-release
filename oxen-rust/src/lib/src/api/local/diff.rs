use crate::core::df::tabular;
use crate::core::index::CommitDirEntryReader;
use crate::error::OxenError;
use crate::model::entry::diff_entry::DiffEntryStatus;
use crate::model::{Commit, CommitEntry, DataFrameDiff, DiffEntry, LocalRepository, Schema};
use crate::opts::DFOpts;
use crate::view::compare::AddRemoveModifyCounts;
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

pub fn diff(
    repo: &LocalRepository,
    original: &Commit,
    compare: &Commit,
    path: impl AsRef<Path>,
) -> Result<String, OxenError> {
    let original_path = get_version_file_from_commit(repo, original, &path)?;
    let compare_path = get_version_file_from_commit(repo, compare, &path)?;
    diff_files(original_path, compare_path)
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
    original_path: impl AsRef<Path>,
    compare_path: impl AsRef<Path>,
) -> Result<DataFrameDiff, OxenError> {
    let original_path = original_path.as_ref();
    let compare_path = compare_path.as_ref();
    // Make sure files exist
    if !original_path.exists() {
        return Err(OxenError::entry_does_not_exist(original_path));
    }

    if !compare_path.exists() {
        return Err(OxenError::entry_does_not_exist(compare_path));
    }

    // Read DFs and get schemas
    let original_df = tabular::read_df(original_path, DFOpts::empty())?;
    let compare_df = tabular::read_df(compare_path, DFOpts::empty())?;
    let original_schema = Schema::from_polars(&original_df.schema());
    let compare_schema = Schema::from_polars(&compare_df.schema());

    log::debug!(
        "Original df {} {original_path:?}\n{original_df:?}",
        original_schema.hash
    );
    log::debug!(
        "Compare df {} {compare_path:?}\n{compare_df:?}",
        compare_schema.hash
    );

    // If schemas don't match, figure out which columns are different
    if original_schema.hash != compare_schema.hash {
        compute_new_columns(
            original_path,
            compare_path,
            &original_schema,
            &compare_schema,
        )
    } else {
        log::debug!("Computing diff for {original_path:?} to {compare_path:?}");
        compute_new_rows(original_df, compare_df, &original_schema)
    }
}

fn compute_new_rows(
    original_df: DataFrame,
    compare_df: DataFrame,
    schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    // Hash the rows
    let versioned_df = tabular::df_hash_rows(original_df)?;
    let current_df = tabular::df_hash_rows(compare_df)?;

    // log::debug!("diff_current got current hashes {}", current_df);

    let current_hash_indices: HashMap<String, u32> = current_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    let versioned_hash_indices: HashMap<String, u32> = versioned_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    // Added is all the row hashes that are in current that are not in other
    let mut added_indices: Vec<u32> = current_hash_indices
        .iter()
        .filter(|(hash, _indices)| !versioned_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    added_indices.sort(); // so is deterministic and returned in correct order

    // Removed is all the row hashes that are in other that are not in current
    let mut removed_indices: Vec<u32> = versioned_hash_indices
        .iter()
        .filter(|(hash, _indices)| !current_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    removed_indices.sort(); // so is deterministic and returned in correct order

    // log::debug!("diff_current added_indices {:?}", added_indices);

    // log::debug!("diff_current removed_indices {:?}", removed_indices);

    // Take added from the current df
    let opts = DFOpts::from_schema_columns(schema);
    let current_df = tabular::transform(current_df, opts)?;
    let added_rows = tabular::take(current_df.lazy(), added_indices)?;

    // Take removed from versioned df
    let opts = DFOpts::from_schema_columns(schema);
    let versioned_df = tabular::transform(versioned_df, opts)?;
    let removed_rows = tabular::take(versioned_df.lazy(), removed_indices)?;

    Ok(DataFrameDiff {
        base_schema: schema.to_owned(),
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

fn compute_new_columns(
    versioned_path: &Path,
    current_path: &Path,
    versioned_schema: &Schema,
    current_schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    let added_fields = current_schema.added_fields(versioned_schema);
    let removed_fields = current_schema.removed_fields(versioned_schema);

    let added_cols = if !added_fields.is_empty() {
        let opts = DFOpts::from_columns(added_fields);
        let df_added = tabular::read_df(current_path, opts)?;
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
        let df_removed = tabular::read_df(versioned_path, opts)?;
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
        base_schema: versioned_schema.to_owned(),
        added_rows: None,
        removed_rows: None,
        added_cols,
        removed_cols,
    })
}

pub fn list_diff_entries(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<DiffEntry>, OxenError> {
    log::debug!(
        "list_diff_entries base_commit: '{}', head_commit: '{}'",
        base_commit.message,
        head_commit.message
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
    log::debug!(
        "Reading entries from base commit {} -> {}",
        head_commit.id,
        head_commit.message
    );
    let base_entries = read_entries_from_commit(repo, base_commit)?;

    let mut diff_entries: Vec<DiffEntry> = vec![];
    collect_added_entries(repo, &base_entries, &head_entries, &mut diff_entries)?;
    collect_removed_entries(repo, &base_entries, &head_entries, &mut diff_entries)?;
    collect_modified_entries(repo, &base_entries, &head_entries, &mut diff_entries)?;

    Ok(diff_entries)
}

// TODO: linear scan is not the most efficient way to do this
pub fn get_add_remove_modify_counts(entries: &[DiffEntry]) -> AddRemoveModifyCounts {
    let mut added = 0;
    let mut removed = 0;
    let mut modified = 0;
    for entry in entries {
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

// Find the entries that are in HEAD but not in BASE
fn collect_added_entries(
    repo: &LocalRepository,
    base_entries: &HashSet<CommitEntry>,
    head_entries: &HashSet<CommitEntry>,
    diff_entries: &mut Vec<DiffEntry>,
) -> Result<(), OxenError> {
    for head_entry in head_entries {
        // HEAD entry is *not* in BASE
        if !base_entries.contains(head_entry) {
            diff_entries.push(DiffEntry::from_commit_entry(
                repo,
                None,
                Some(head_entry),
                DiffEntryStatus::Added,
            ));
        }
    }
    Ok(())
}

// Find the entries that are in BASE but not in HEAD
fn collect_removed_entries(
    repo: &LocalRepository,
    base_entries: &HashSet<CommitEntry>,
    head_entries: &HashSet<CommitEntry>,
    diff_entries: &mut Vec<DiffEntry>,
) -> Result<(), OxenError> {
    for base_entry in base_entries {
        // BASE entry is *not* in HEAD
        if !head_entries.contains(base_entry) {
            diff_entries.push(DiffEntry::from_commit_entry(
                repo,
                Some(base_entry),
                None,
                DiffEntryStatus::Removed,
            ));
        }
    }
    Ok(())
}

// Find the entries that are in both base and head, but have different hashes
fn collect_modified_entries(
    repo: &LocalRepository,
    base_entries: &HashSet<CommitEntry>,
    head_entries: &HashSet<CommitEntry>,
    diff_entries: &mut Vec<DiffEntry>,
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
                diff_entries.push(DiffEntry::from_commit_entry(
                    repo,
                    Some(base_entry),
                    Some(head_entry),
                    DiffEntryStatus::Modified,
                ));
            }
        }
    }
    Ok(())
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
    use crate::model::entry::diff_entry::DiffEntryStatus;
    use crate::opts::RmOpts;
    use crate::test;
    use crate::util;

    #[test]
    fn test_list_diff_entries_add_multiple() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
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

            let entries = api::local::diff::list_diff_entries(&repo, &base_commit, &head_commit)?;
            assert_eq!(2, entries.len());
            assert_eq!(entries[0].status, DiffEntryStatus::Added.to_string());
            assert_eq!(entries[1].status, DiffEntryStatus::Added.to_string());

            Ok(())
        })
    }

    #[test]
    fn test_list_diff_entries_modify_one_tabular() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
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

            let entries = api::local::diff::list_diff_entries(&repo, &base_commit, &head_commit)?;
            assert_eq!(1, entries.len());
            assert_eq!(
                entries.first().unwrap().status,
                DiffEntryStatus::Modified.to_string()
            );

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_list_diff_entries_remove_one_tabular() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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

            let entries = api::local::diff::list_diff_entries(&repo, &base_commit, &head_commit)?;
            assert_eq!(1, entries.len());
            assert_eq!(
                entries.first().unwrap().status,
                DiffEntryStatus::Removed.to_string()
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_add_remove_modify_counts() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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

            let entries = api::local::diff::list_diff_entries(&repo, &base_commit, &head_commit)?;
            let counts = api::local::diff::get_add_remove_modify_counts(&entries);

            assert_eq!(4, entries.len());
            assert_eq!(2, counts.added);
            assert_eq!(1, counts.removed);

            Ok(())
        })
        .await
    }
}
