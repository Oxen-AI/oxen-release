//! # ModStager
//!
//! Stages modifications in the remote staging area that can later be applied
//! to files on commit.
//!

use std::path::{Path, PathBuf};

use polars::frame::DataFrame;
use polars::lazy::dsl::{col, lit};
use polars::lazy::frame::IntoLazy;
use polars::series::ChunkCompare;
use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};
use sql_query_builder::Select;

use time::OffsetDateTime;

use crate::constants::{FILES_DIR, MODS_DIR, OXEN_HIDDEN_DIR, STAGED_DIR, TABLE_NAME};
use crate::core::db::{self, df_db, staged_df_db, str_json_db};
use crate::core::df::tabular;
use crate::core::index::{self, mod_stager, remote_df_stager};
use crate::error::OxenError;
use crate::model::diff::DiffResult::Tabular;
use crate::model::entry::mod_entry::{ModType, NewMod};
use crate::model::{Branch, CommitEntry, DataFrameDiff, LocalRepository, ModEntry, Schema};

use crate::{api, current_function, util};
use staged_df_db::{OXEN_MOD_STATUS_COL, OXEN_ROW_INDEX_COL};

use super::{remote_dir_stager, SchemaReader};

fn mods_db_path(
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

pub fn mods_duckdb_path(
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

pub fn create_mod(
    repo: &LocalRepository,
    branch: &Branch,
    identifier: &str,
    new_mod: &NewMod,
) -> Result<ModEntry, OxenError> {
    // Try to track the mod
    let mod_entry = stage_mod(repo, branch, identifier, new_mod)?;

    // Track the parent file
    track_mod_commit_entry(repo, branch, identifier, &new_mod.entry)?;

    Ok(mod_entry)
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
            name: Some("todo".to_string()), // TODONOW
            ..schema
        };

        // create_duckdb_table_from_schema(repo, branch, identity, &schema, &new_mod.entry.path)?;

        let db_path = mods_duckdb_path(repo, branch, identifier, &new_mod.entry.path);
        let conn = df_db::get_connection(&db_path)?;

        // TODONOW: don't reindex every time
        log::debug!("checking table exists");
        let table_exists = df_db::table_exists(&conn, &TABLE_NAME)?;
        if !table_exists {
            log::debug!("table doesn't exist, indexing");
            remote_df_stager::index_dataset(&repo, &branch, &new_mod.entry.path, identifier)?;
        }

        log::debug!(
            "after indexing, table exists? {}",
            df_db::table_exists(&conn, TABLE_NAME)?
        );

        let df =
            tabular::parse_data_into_df(&new_mod.data, &schema, new_mod.content_type.to_owned())?;

        log::debug!("here's our append df {:?}", df);

        let result = staged_df_db::append_row(&conn, &df)?;

        log::debug!("tracking mod commit entry");
        track_mod_commit_entry(repo, branch, identifier, &new_mod.entry)?;

        Ok(result)
    } else {
        let err = format!("Schema not found for file {:?}", new_mod.entry.path);
        Err(OxenError::basic_str(err))
    }
}

// pub fn delete_mod_from_path(
//     repo: &LocalRepository,
//     branch: &Branch,
//     identity: &str,
//     file_path: &Path,
//     uuid: &str,
// ) -> Result<ModEntry, OxenError> {
//     let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
//     match api::local::entries::get_commit_entry(repo, &commit, file_path)? {
//         Some(_) => match delete_mod(repo, branch, identity, file_path, uuid) {
//             Ok(mod_entry) => Ok(mod_entry),
//             Err(e) => {
//                 log::error!("Error deleting mod [{}]: {}", uuid, e);
//                 Err(e)
//             }
//         },
//         None => Err(OxenError::entry_does_not_exist_in_commit(
//             file_path, &commit.id,
//         )),
//     }
// }

pub fn delete_row(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    path: &Path,
    uuid: &str,
) -> Result<DataFrame, OxenError> {
    let db_path = mods_duckdb_path(repo, branch, identity, path);
    let conn = df_db::get_connection(&db_path)?;
    let deleted_row = staged_df_db::delete_row(&conn, uuid)?;

    // TODO: Better way of tracking when a file is restored to its original state without diffing
    let diff = api::local::diff::diff_staged_df(repo, branch, PathBuf::from(path), identity)?;

    match diff {
        Tabular(diff) => {
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
            } else {
                log::debug!("has change,s here's the diff: {:?}", diff);
            }
        }
        _ => {}
    }
    Ok(deleted_row)
}

// pub fn delete_mod(
//     repo: &LocalRepository,
//     branch: &Branch,
//     identity: &str,
//     path: &Path,
//     uuid: &str,
// ) -> Result<ModEntry, OxenError> {
//     // TODO: put these actions in a queue or lock to prevent race conditions
//     let db_path = mods_db_path(repo, branch, identity, path);
//     log::debug!(
//         "{} Opening mods_db_path at: {:?}",
//         current_function!(),
//         db_path
//     );

//     let opts = db::opts::default();
//     let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;

//     match str_json_db::get(&db, uuid) {
//         Ok(Some(mod_entry)) => {
//             str_json_db::delete(&db, uuid)?;

//             // If there are no more mods for this file, remove the file from the db
//             let remaining = list_mods_raw_from_db(&db)?;
//             if remaining.is_empty() {
//                 let files_db_path = files_db_path(repo, branch, identity);
//                 let files_db: DBWithThreadMode<MultiThreaded> =
//                     rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
//                 let key = path.to_string_lossy();
//                 str_json_db::delete(&files_db, key)?;
//             }

//             Ok(mod_entry)
//         }
//         Ok(None) => Err(OxenError::basic_str(format!(
//             "uuid {} does not exist",
//             uuid
//         ))),
//         Err(e) => Err(e),
//     }
// }

pub fn list_mods_raw(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    path: &Path,
) -> Result<Vec<ModEntry>, OxenError> {
    let db_path = mods_db_path(repo, branch, identity, path);
    log::debug!(
        "{} Opening mods_db_path at: {:?}",
        current_function!(),
        db_path
    );

    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    list_mods_raw_from_db(&db)
}

pub fn list_mods_raw_from_db(
    db: &DBWithThreadMode<MultiThreaded>,
) -> Result<Vec<ModEntry>, OxenError> {
    let mut results: Vec<ModEntry> = str_json_db::list_vals(db)?;
    results.sort_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap());
    Ok(results)
}

pub fn list_mods_df(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    entry: &CommitEntry,
) -> Result<DataFrameDiff, OxenError> {
    let schema_reader = SchemaReader::new(repo, &entry.commit_id)?;
    if let Some(schema) = schema_reader.get_schema_for_file(&entry.path)? {
        let mods = list_mods_raw(repo, branch, identity, &entry.path)?;
        let mut df = polars::frame::DataFrame::default();
        for modification in mods.iter() {
            log::debug!("Applying modification: {:?}", modification);
            let mod_df = modification.to_df()?;
            df = df.vstack(&mod_df).unwrap();
        }

        Ok(DataFrameDiff {
            head_schema: Some(schema.clone()),
            base_schema: Some(schema),
            added_rows: Some(df),
            removed_rows: None,
            added_cols: None,
            removed_cols: None,
        })
    } else {
        Err(OxenError::schema_does_not_exist_for_file(&entry.path))
    }
}

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

fn stage_mod(
    repo: &LocalRepository,
    branch: &Branch,
    identifier: &str,
    new_mod: &NewMod,
) -> Result<ModEntry, OxenError> {
    let version_path = util::fs::version_path(repo, &new_mod.entry);

    log::debug!("Here's the mod for {:?}: {:?}", version_path, new_mod);

    if util::fs::is_tabular(&version_path) {
        stage_tabular_mod(repo, branch, identifier, new_mod)
    } else {
        Err(OxenError::basic_str(format!(
            "{:?} not supported for file type",
            new_mod.mod_type
        )))
    }
}

fn stage_tabular_mod(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    new_mod: &NewMod,
) -> Result<ModEntry, OxenError> {
    // Read the schema of the data frame
    log::debug!(
        "staging tabmodnew for commit [{}] for entry {:?}",
        new_mod.entry.commit_id,
        new_mod.entry.path
    );

    let schema_reader = SchemaReader::new(repo, &new_mod.entry.commit_id)?;
    if let Some(schema) = schema_reader.get_schema_for_file(&new_mod.entry.path)? {
        // Add a name to the schema - todo probably should be an impl on a struct
        let schema = Schema {
            name: Some("todo".to_string()), // TODONOW
            ..schema
        };

        // create_duckdb_table_from_schema(repo, branch, identity, &schema, &new_mod.entry.path)?;

        let db_path = mods_duckdb_path(repo, branch, identity, &new_mod.entry.path);
        let conn = df_db::get_connection(&db_path)?;

        // TODONOW: don't reindex every time
        let table_exists = df_db::table_exists(&conn, &TABLE_NAME)?;
        log::debug!("pre index table exists: {}", table_exists);
        remote_df_stager::index_dataset(&repo, &branch, &new_mod.entry.path, identity)?;
        let table_exists = df_db::table_exists(&conn, &TABLE_NAME)?;
        log::debug!("post index table exists: {}", table_exists);

        match new_mod.mod_type {
            ModType::Append => {
                let db_path = mods_duckdb_path(repo, branch, identity, &new_mod.entry.path);
                let conn = df_db::get_connection(&db_path)?;

                let df = tabular::parse_data_into_df(
                    &new_mod.data,
                    &schema,
                    new_mod.content_type.to_owned(),
                )?;

                log::debug!("here's our append df {:?}", df);

                let mod_row = staged_df_db::append_row(&conn, &df)?;
            }
            ModType::Delete => {
                let db_path = mods_duckdb_path(repo, branch, identity, &new_mod.entry.path);
                let conn = df_db::get_connection(&db_path)?;

                let df = tabular::parse_data_into_df(
                    &new_mod.data,
                    &schema,
                    new_mod.content_type.to_owned(),
                )?;

                // let mod_row = staged_df_db::delete_row(&conn, &df)?;
            }
            ModType::Modify => {
                let df = tabular::parse_data_into_df(
                    &new_mod.data,
                    &schema,
                    new_mod.content_type.to_owned(),
                )?;

                // let mod_row = staged_df_db::modify_row(&conn, &df)?;
            }
        }

        let dummy_mod = ModEntry {
            uuid: "dummy".to_string(),
            data: "".to_string(),
            schema: Some(schema),
            modification_type: new_mod.mod_type.to_owned(),
            content_type: new_mod.content_type.to_owned(),
            path: new_mod.entry.path.to_owned(),
            timestamp: OffsetDateTime::now_utc(),
        };

        Ok(dummy_mod)
    } else {
        let err = format!("Schema not found for file {:?}", new_mod.entry.path);
        Err(OxenError::basic_str(err))
    }
}

pub fn unindex_df(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    let mods_duckdb_path = mods_duckdb_path(repo, branch, identity, path);
    let conn = df_db::get_connection(&mods_duckdb_path)?;
    df_db::drop_table(&conn, &TABLE_NAME)?;

    // Remove file from files db
    let opts = db::opts::default();
    let files_db_path = files_db_path(repo, branch, identity);
    let files_db: DBWithThreadMode<MultiThreaded> =
        rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
    let key = path.to_string_lossy();
    str_json_db::delete(&files_db, key)?;

    Ok(())
}

// pub fn clear_mods(
//     repo: &LocalRepository,
//     branch: &Branch,
//     identity: &str,
//     path: impl AsRef<Path>,
// ) -> Result<(), OxenError> {
//     let path = path.as_ref();
//     log::debug!("clear_mods for {path:?}");
//     // Remove all mods from mod db
//     let db_path = mods_db_path(repo, branch, identity, path);
//     log::debug!("clear_mods mods_db_path for {db_path:?}");

//     let opts = db::opts::default();
//     let db: DBWithThreadMode<MultiThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
//     str_json_db::clear(&db)?;

//     // Remove file from files db
//     let files_db_path = files_db_path(repo, branch, identity);
//     log::debug!("clear_mods files_db_path for {files_db_path:?}");

//     let files_db: DBWithThreadMode<MultiThreaded> =
//         rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
//     let key = path.to_string_lossy();
//     str_json_db::delete(&files_db, key)
// }

pub fn branch_is_ahead_of_staging(
    repo: &LocalRepository,
    branch: &Branch,
    identity: &str,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    let commit_path = mods_commit_ref_path(repo, branch, identity, path);
    let commit_id = std::fs::read_to_string(&commit_path)?;

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
            remote_df_stager::index_dataset(&repo, &branch, &file_path, &identity)?;
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
                _ => assert!(false, "Expected tabular diff result"),
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

            remote_df_stager::index_dataset(&repo, &branch, &file_path, &identity)?;

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
                _ => assert!(false, "Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_stage_delete_appended_mod() -> Result<(), OxenError> {
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

            remote_df_stager::index_dataset(&repo, &branch, &file_path, &identity)?;

            let append_entry_1 = mod_stager::add_row(&repo, &branch, &identity, &new_mod)?;
            let append_1_id = append_entry_1.column(OXEN_ID_COL)?.get(0)?.to_string();
            let append_1_id = append_1_id.replace("\"", "");

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
                _ => assert!(false, "Expected tabular diff result"),
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
                _ => assert!(false, "Expected tabular diff result"),
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

            remote_df_stager::index_dataset(&repo, &branch, &file_path, &identity)?;

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
                _ => assert!(false, "Expected tabular diff result"),
            }
            // Delete the first append
            mod_stager::unindex_df(&repo, &branch, &identity, &file_path)?;

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
                _ => assert!(false, "Expected tabular diff result"),
            }
            Ok(())
        })
    }
}
