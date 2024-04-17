//! # ModStager
//!
//! Stages modifications in the remote staging area that can later be applied
//! to files on commit.
//!

use std::path::{Path, PathBuf};

use polars::frame::DataFrame;

use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};

use crate::constants::{FILES_DIR, MODS_DIR, OXEN_HIDDEN_DIR, STAGED_DIR};
use crate::core::db::{self, df_db, staged_df_db, str_json_db};
use crate::core::df::tabular;
use crate::core::index::remote_df_stager;
use crate::error::OxenError;
use crate::model::diff::DiffResult;
use crate::model::entry::mod_entry::NewMod;
use crate::model::{Branch, CommitEntry, LocalRepository, Schema};

use crate::{api, util};

use super::{remote_dir_stager, SchemaReader};

pub fn mods_db_path(
    repo: &LocalRepository,
    branch: &Branch,
    identifier: &str,
    path: impl AsRef<Path>,
) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());

    remote_dir_stager::branch_staging_dir(repo, branch, identifier)
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(MODS_DIR)
        .join(MODS_DIR)
        .join(path_hash)
}

pub fn mods_df_db_path(
    repo: &LocalRepository,
    branch: &Branch,
    identifier: &str,
    path: impl AsRef<Path>,
) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    remote_dir_stager::branch_staging_dir(repo, branch, identifier)
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("db")
}

pub fn mods_commit_ref_path(
    repo: &LocalRepository,
    branch: &Branch,
    identifier: &str,
    path: impl AsRef<Path>,
) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    remote_dir_stager::branch_staging_dir(repo, branch, identifier)
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("COMMIT_ID")
}

fn files_db_path(repo: &LocalRepository, branch: &Branch, identifier: &str) -> PathBuf {
    remote_dir_stager::branch_staging_dir(repo, branch, identifier)
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(MODS_DIR)
        .join(FILES_DIR)
}

pub fn add_row(
    repo: &LocalRepository,
    branch: &Branch,
    identifier: &str,
    new_mod: &NewMod,
) -> Result<DataFrame, OxenError> {
    let schema_reader = SchemaReader::new(repo, &new_mod.entry.commit_id)?;
    if let Some(schema) = schema_reader.get_schema_for_file(&new_mod.entry.path)? {
        // Add a name to the schema - todo probably should be an impl on a struct
        let schema = Schema {
            name: Some("STAGED".to_string()),
            ..schema
        };

        let db_path = mods_df_db_path(repo, branch, identifier, &new_mod.entry.path);
        let conn = df_db::get_connection(db_path)?;

        let df =
            tabular::parse_data_into_df(&new_mod.data, &schema, new_mod.content_type.to_owned())?;

        let result = staged_df_db::append_row(&conn, &df)?;

        track_mod_commit_entry(repo, branch, identifier, &new_mod.entry)?;

        Ok(result)
    } else {
        let err = format!("Schema not found for file {:?}", new_mod.entry.path);
        Err(OxenError::basic_str(err))
    }
}

pub fn unstage_df(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    remote_df_stager::unindex_df(repo, branch, identity, &path)?;

    let opts = db::opts::default();
    let files_db_path = files_db_path(repo, branch, identity);
    let files_db: DBWithThreadMode<MultiThreaded> =
        rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
    let key = path.as_ref().to_string_lossy();
    str_json_db::delete(&files_db, key)?;

    Ok(())
}

pub fn delete_row(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    path: &Path,
    uuid: &str,
) -> Result<DataFrame, OxenError> {
    let db_path = mods_df_db_path(repo, branch, identity, path);
    let deleted_row = {
        let conn = df_db::get_connection(db_path)?;
        staged_df_db::delete_row(&conn, uuid)?
    };

    // TODO: Better way of tracking when a file is restored to its original state without diffing
    // TODO: Better way of tracking when a file is restored to its original state without diffing
    let diff = api::local::diff::diff_staged_df(repo, branch, PathBuf::from(path), identity)?;

    if let DiffResult::Tabular(diff) = diff {
        log::debug!("in tabular diff");
        if !diff.has_changes() {
            log::debug!("no changes, deleting file from staged db");
            // Restored to original state == delete file from staged db
            let opts = db::opts::default();
            let files_db_path = files_db_path(repo, branch, identity);
            let files_db: DBWithThreadMode<MultiThreaded> =
                rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
            let key = path.to_string_lossy();
            str_json_db::delete(&files_db, key)?;
        }
    }
    Ok(deleted_row)
}

// pub fn list_mods_raw(
//     repo: &LocalRepository,
//     branch: &Branch,
//     identity: &str,
//     path: &Path,
// ) -> Result<Vec<ModEntry>, OxenError> {
//     let db_path = mods_db_path(repo, branch, identity, path);
//     log::debug!(
//         "{} Opening mods_db_path at: {:?}",
//         current_function!(),
//         db_path
//     );

//     let opts = db::opts::default();
//     let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
//     list_mods_raw_from_db(&db)
// }

// pub fn list_mods_raw_from_db(
//     db: &DBWithThreadMode<MultiThreaded>,
// ) -> Result<Vec<ModEntry>, OxenError> {
//     let mut results: Vec<ModEntry> = str_json_db::list_vals(db)?;
//     results.sort_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap());
//     Ok(results)
// }

// pub fn list_mods_df(
//     repo: &LocalRepository,
//     branch: &Branch,
//     identity: &str,
//     entry: &CommitEntry,
// ) -> Result<DataFrameDiff, OxenError> {
//     let schema_reader = SchemaReader::new(repo, &entry.commit_id)?;
//     if let Some(schema) = schema_reader.get_schema_for_file(&entry.path)? {
//         let mods = list_mods_raw(repo, branch, identity, &entry.path)?;
//         let mut df = polars::frame::DataFrame::default();
//         for modification in mods.iter() {
//             log::debug!("Applying modification: {:?}", modification);
//             let mod_df = modification.to_df()?;
//             df = df.vstack(&mod_df).unwrap();
//         }

//         Ok(DataFrameDiff {
//             head_schema: Some(schema.clone()),
//             base_schema: Some(schema),
//             added_rows: Some(df),
//             removed_rows: None,
//             added_cols: None,
//             removed_cols: None,
//         })
//     } else {
//         Err(OxenError::schema_does_not_exist_for_file(&entry.path))
//     }
// }

pub fn list_mod_entries(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
) -> Result<Vec<PathBuf>, OxenError> {
    let db_path = files_db_path(repo, branch, identity);
    log::debug!("list_mod_entries from files_db_path {db_path:?}");
    let opts = db::opts::default();
    let db: DBWithThreadMode<SingleThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::list_vals(&db)
}

fn track_mod_commit_entry(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    entry: &CommitEntry,
) -> Result<(), OxenError> {
    let db_path = files_db_path(repo, branch, identity);
    log::debug!("track_mod_commit_entry from files_db_path {db_path:?}");
    let opts = db::opts::default();
    let db: DBWithThreadMode<MultiThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let key = entry.path.to_string_lossy();
    str_json_db::put(&db, &key, &key)
}

pub fn branch_is_ahead_of_staging(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    let commit_path = mods_commit_ref_path(repo, branch, identity, path);
    let commit_id = std::fs::read_to_string(commit_path)?;

    log::debug!("read commit id {:?}", commit_id);
    log::debug!("branch commit id {:?}", branch.commit_id);
    Ok(commit_id != branch.commit_id)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::OXEN_ID_COL;
    use crate::core::index::mod_stager;
    use crate::core::index::remote_df_stager;
    use crate::error::OxenError;
    use crate::model::diff::DiffResult;
    use crate::model::entry::mod_entry::ModType;
    use crate::model::entry::mod_entry::NewMod;
    use crate::model::ContentType;
    use crate::opts::DFOpts;
    use crate::test;

    #[test]
    fn test_stage_json_append_tabular() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let identity = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "{\"file\":\"dawg1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data: data.to_string(),
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };
            let opts = DFOpts::empty();
            remote_df_stager::index_dataset(&repo, &branch, &file_path, &identity, &opts)?;
            mod_stager::add_row(&repo, &branch, &identity, &new_mod)?;

            // List the files that are changed
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &identity)?;
            assert_eq!(commit_entries.len(), 1);

            let diff = api::local::diff::diff_staged_df(&repo, &branch, file_path, &identity)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_stage_csv_append_tabular() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let identity = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "dawg1.jpg,dog,13,14,100,100";
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data: data.to_string(),
                mod_type: ModType::Append,
                content_type: ContentType::Csv,
            };

            let opts = DFOpts::empty();
            remote_df_stager::index_dataset(&repo, &branch, &file_path, &identity, &opts)?;

            mod_stager::add_row(&repo, &branch, &identity, &new_mod)?;

            // List the files that are changed
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &identity)?;
            assert_eq!(commit_entries.len(), 1);

            let diff = api::local::diff::diff_staged_df(&repo, &branch, file_path, &identity)?;

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_stage_delete_appended_mod() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let identity = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "dawg1.jpg,dog,13,14,100,100".to_string();
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data,
                mod_type: ModType::Append,
                content_type: ContentType::Csv,
            };

            let opts = DFOpts::empty();
            remote_df_stager::index_dataset(&repo, &branch, &file_path, &identity, &opts)?;

            let append_entry_1 = mod_stager::add_row(&repo, &branch, &identity, &new_mod)?;
            let append_1_id = append_entry_1.column(OXEN_ID_COL)?.get(0)?.to_string();
            let append_1_id = append_1_id.replace('"', "");

            let data = "dawg2.jpg,dog,13,14,100,100".to_string();
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data,
                mod_type: ModType::Append,
                content_type: ContentType::Csv,
            };
            let _append_entry_2 = mod_stager::add_row(&repo, &branch, &identity, &new_mod)?;

            // List the files that are changed
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &identity)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let diff =
                api::local::diff::diff_staged_df(&repo, &branch, file_path.clone(), &identity)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Delete the first append
            mod_stager::delete_row(
                &repo,
                &branch,
                &identity,
                commit_entries.first().unwrap(),
                &append_1_id,
            )?;

            // Should only be one mod now
            let diff = api::local::diff::diff_staged_df(&repo, &branch, file_path, &identity)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_clear_staged_mods() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let identity = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "dawg1.jpg,dog,13,14,100,100".to_string();
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data,
                mod_type: ModType::Append,
                content_type: ContentType::Csv,
            };
            let opts = DFOpts::empty();

            remote_df_stager::index_dataset(&repo, &branch, &file_path, &identity, &opts)?;

            mod_stager::add_row(&repo, &branch, &identity, &new_mod)?;

            let data = "dawg2.jpg,dog,13,14,100,100".to_string();
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data,
                mod_type: ModType::Append,
                content_type: ContentType::Csv,
            };
            mod_stager::add_row(&repo, &branch, &identity, &new_mod)?;

            // List the files that are changed
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &identity)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let diff =
                api::local::diff::diff_staged_df(&repo, &branch, file_path.clone(), &identity)?;

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => panic!("Expected tabular diff result"),
            }
            // Delete the first append
            mod_stager::unstage_df(&repo, &branch, &identity, &file_path)?;

            // Should be zero staged files
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &identity)?;
            assert_eq!(commit_entries.len(), 0);

            // Should be zero mods left
            let diff = api::local::diff::diff_staged_df(&repo, &branch, file_path, &identity)?;

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }
            Ok(())
        })
    }
}
