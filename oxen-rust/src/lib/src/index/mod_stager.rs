use std::path::{Path, PathBuf};

use rocksdb::{DBWithThreadMode, MultiThreaded};
use time::OffsetDateTime;

use crate::constants::{FILES_DIR, MODS_DIR, OXEN_HIDDEN_DIR, STAGED_DIR};
use crate::db::{self, str_json_db};
use crate::df::tabular;
use crate::error::OxenError;
use crate::model::entry::mod_entry::ModType;
use crate::model::schema::Field;
use crate::model::{Branch, CommitEntry, ContentType, DataFrameDiff, LocalRepository, ModEntry};
use crate::{api, util};

use super::{remote_dir_stager, SchemaReader};

fn mods_db_path(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    path: impl AsRef<Path>,
) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    remote_dir_stager::branch_staging_dir(repo, branch, user_id)
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(MODS_DIR)
        .join(MODS_DIR)
        .join(path_hash)
}

fn files_db_path(repo: &LocalRepository, branch: &Branch, user_id: &str) -> PathBuf {
    remote_dir_stager::branch_staging_dir(repo, branch, user_id)
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(MODS_DIR)
        .join(FILES_DIR)
}

pub fn create_mod(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    file_path: &Path,
    content_type: ContentType,
    mod_type: ModType,
    content: String,
) -> Result<ModEntry, OxenError> {
    log::debug!(
        "Looking for file in commit... [{}] {:?}",
        branch.commit_id,
        file_path
    );
    // should be safe to unwrap if we have a branch
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    match api::local::entries::get_entry_for_commit(repo, &commit, file_path)? {
        Some(commit_entry) => {
            // Try to track the mod
            match stage_mod(
                repo,
                branch,
                user_id,
                &commit_entry,
                content_type,
                mod_type,
                content,
            ) {
                Ok(mod_entry) => {
                    // If successful track the file it is modifying
                    track_mod_commit_entry(repo, branch, user_id, &commit_entry)?;

                    Ok(mod_entry)
                }
                Err(e) => {
                    log::error!("Error staging mod: {}", e);
                    Err(e)
                }
            }
        }
        None => Err(OxenError::file_does_not_exist_in_commit(
            file_path, &commit.id,
        )),
    }
}

pub fn delete_mod_from_path(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    file_path: &Path,
    uuid: &str,
) -> Result<ModEntry, OxenError> {
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    match api::local::entries::get_entry_for_commit(repo, &commit, file_path)? {
        Some(_) => match delete_mod(repo, branch, user_id, file_path, uuid) {
            Ok(mod_entry) => Ok(mod_entry),
            Err(e) => {
                log::error!("Error deleting mod [{}]: {}", uuid, e);
                Err(e)
            }
        },
        None => Err(OxenError::file_does_not_exist_in_commit(
            file_path, &commit.id,
        )),
    }
}

pub fn delete_mod(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    path: &Path,
    uuid: &str,
) -> Result<ModEntry, OxenError> {
    // TODO: put these actions in a queue or lock to prevent race conditions
    let db_path = mods_db_path(repo, branch, user_id, path);
    log::debug!("Opening db at: {:?}", db_path);

    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;

    match str_json_db::get(&db, uuid) {
        Ok(Some(mod_entry)) => {
            str_json_db::delete(&db, uuid)?;

            // If there are no more mods for this file, remove the file from the db
            let remaining = list_mods_raw_from_db(&db)?;
            if remaining.is_empty() {
                let files_db_path = files_db_path(repo, branch, user_id);
                let files_db = rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
                let key = path.to_string_lossy();
                str_json_db::delete(&files_db, key)?;
            }

            Ok(mod_entry)
        }
        Ok(None) => Err(OxenError::basic_str(format!(
            "uuid {} does not exist",
            uuid
        ))),
        Err(e) => Err(e),
    }
}

pub fn list_mods_raw(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    path: &Path,
) -> Result<Vec<ModEntry>, OxenError> {
    let db_path = mods_db_path(repo, branch, user_id, path);
    log::debug!("Opening db at: {:?}", db_path);

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
    user_id: &str,
    entry: &CommitEntry,
) -> Result<DataFrameDiff, OxenError> {
    let schema_reader = SchemaReader::new(repo, &entry.commit_id)?;
    if let Some(schema) = schema_reader.get_schema_for_file(&entry.path)? {
        let mods = list_mods_raw(repo, branch, user_id, &entry.path)?;
        let mut df = polars::frame::DataFrame::default();
        for modification in mods.iter() {
            log::debug!("Applying modification: {:?}", modification);
            let mod_df = modification.to_df()?;
            df = df.vstack(&mod_df).unwrap();
        }

        Ok(DataFrameDiff {
            base_schema: schema,
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
    user_id: &str,
) -> Result<Vec<PathBuf>, OxenError> {
    let db_path = files_db_path(repo, branch, user_id);
    log::debug!("list_mod_entries from {db_path:?}");
    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::list_vals(&db)
}

fn track_mod_commit_entry(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    entry: &CommitEntry,
) -> Result<(), OxenError> {
    let db_path = files_db_path(repo, branch, user_id);
    log::debug!("track_mod_commit_entry from {db_path:?}");
    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let key = entry.path.to_string_lossy();
    str_json_db::put(&db, &key, &key)
}

fn stage_mod(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    entry: &CommitEntry,
    content_type: ContentType,
    mod_type: ModType,
    content: String,
) -> Result<ModEntry, OxenError> {
    let version_path = util::fs::version_path(repo, entry);
    if util::fs::is_tabular(&version_path) {
        stage_tabular_mod(
            repo,
            branch,
            user_id,
            entry,
            content_type,
            mod_type,
            content,
        )
    } else {
        Err(OxenError::basic_str(format!(
            "{mod_type:?} not supported for file type"
        )))
    }
}

/// Throws an error if the content cannot be parsed into the proper tabular schema
fn stage_tabular_mod(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    commit_entry: &CommitEntry,
    content_type: ContentType,
    mod_type: ModType,
    content: String,
) -> Result<ModEntry, OxenError> {
    // Read the schema of the data frame
    log::debug!(
        "Looking for schema on commit [{}] for entry {:?}",
        commit_entry.commit_id,
        commit_entry.path
    );
    let schema_reader = SchemaReader::new(repo, &commit_entry.commit_id)?;
    if let Some(schema) = schema_reader.get_schema_for_file(&commit_entry.path)? {
        // Parse the data into DF
        match tabular::parse_data_into_df(&content, &schema, content_type.to_owned()) {
            Ok(df) => {
                log::debug!("Successfully parsed df {:?}", df);
                // Make sure it contains each field
                let polars_schema = df.schema();
                if schema.has_all_field_names(&polars_schema) {
                    // hash uuid to make a smaller key
                    let uuid =
                        util::hasher::hash_buffer(uuid::Uuid::new_v4().to_string().as_bytes());
                    let timestamp = OffsetDateTime::now_utc();

                    let mod_entry = ModEntry {
                        uuid,
                        data: content,
                        schema: Some(schema),
                        modification_type: mod_type,
                        content_type,
                        path: commit_entry.path.to_owned(),
                        timestamp,
                    };

                    stage_raw_mod_content(repo, branch, user_id, commit_entry, mod_entry)
                } else {
                    let schema_fields_str = Field::all_fields_to_string(&schema.fields);
                    let err = format!("Json schema does not contain same fields as DataFrame schema. {schema_fields_str}");
                    Err(OxenError::basic_str(err))
                }
            }
            Err(err) => {
                let err = format!("Error parsing content: {err}");
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        let err = format!("Schema not found for file {:?}", commit_entry.path);
        Err(OxenError::basic_str(err))
    }
}

fn stage_raw_mod_content(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    commit_entry: &CommitEntry,
    entry: ModEntry,
) -> Result<ModEntry, OxenError> {
    let db_path = mods_db_path(repo, branch, user_id, &commit_entry.path);
    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;

    str_json_db::put(&db, &entry.uuid, &entry)?;

    Ok(entry)
}

pub fn clear_mods(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    log::debug!("clear_mods for {path:?}");
    // Remove all mods from mod db
    let db_path = mods_db_path(repo, branch, user_id, path);
    log::debug!("clear_mods mods_db_path for {db_path:?}");

    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::clear(&db)?;

    // Remove file from files db
    let files_db_path = files_db_path(repo, branch, user_id);
    log::debug!("clear_mods files_db_path for {files_db_path:?}");

    let files_db = rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
    let key = path.to_string_lossy();
    str_json_db::delete(&files_db, key)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::config::UserConfig;
    use crate::error::OxenError;
    use crate::index::mod_stager;
    use crate::model::entry::mod_entry::ModType;
    use crate::model::ContentType;
    use crate::test;

    #[test]
    fn test_stage_json_append_tabular() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = command::create_checkout_branch(&repo, branch_name)?;
            let user_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_entry_for_commit(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "{\"file\":\"dawg1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            mod_stager::create_mod(
                &repo,
                &branch,
                &user_id,
                &file_path,
                ContentType::Json,
                ModType::Append,
                data.to_string(),
            )?;

            // List the files that are changed
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &user_id)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let mods = mod_stager::list_mods_df(&repo, &branch, &user_id, &commit_entry)?;
            assert_eq!(mods.added_rows.unwrap().height(), 1);
            Ok(())
        })
    }

    #[test]
    fn test_stage_csv_append_tabular() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = command::create_checkout_branch(&repo, branch_name)?;
            let user_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_entry_for_commit(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "dawg1.jpg,dog,13,14,100,100";
            mod_stager::create_mod(
                &repo,
                &branch,
                &user_id,
                &file_path,
                ContentType::Csv,
                ModType::Append,
                data.to_string(),
            )?;

            // List the files that are changed
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &user_id)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let mods = mod_stager::list_mods_df(&repo, &branch, &user_id, &commit_entry)?;
            assert_eq!(mods.added_rows.unwrap().height(), 1);
            Ok(())
        })
    }

    #[test]
    fn test_stage_delete_appended_mod() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = command::create_checkout_branch(&repo, branch_name)?;
            let user_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_entry_for_commit(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "dawg1.jpg,dog,13,14,100,100".to_string();

            let append_entry_1 = mod_stager::create_mod(
                &repo,
                &branch,
                &user_id,
                &file_path,
                ContentType::Csv,
                ModType::Append,
                data,
            )?;

            let data = "dawg2.jpg,dog,13,14,100,100".to_string();
            let _append_entry_2 = mod_stager::create_mod(
                &repo,
                &branch,
                &user_id,
                &file_path,
                ContentType::Csv,
                ModType::Append,
                data,
            )?;

            // List the files that are changed
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &user_id)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let mods = mod_stager::list_mods_raw(&repo, &branch, &user_id, &commit_entry.path)?;
            assert_eq!(mods.len(), 2);

            // Delete the first append
            mod_stager::delete_mod(
                &repo,
                &branch,
                &user_id,
                commit_entries.first().unwrap(),
                &append_entry_1.uuid,
            )?;

            // Should only be one mod now
            let mods = mod_stager::list_mods_raw(&repo, &branch, &user_id, &commit_entry.path)?;
            assert_eq!(mods.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_clear_staged_mods() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = command::create_checkout_branch(&repo, branch_name)?;
            let user_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_entry_for_commit(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "dawg1.jpg,dog,13,14,100,100".to_string();

            mod_stager::create_mod(
                &repo,
                &branch,
                &user_id,
                &file_path,
                ContentType::Csv,
                ModType::Append,
                data,
            )?;

            let data = "dawg2.jpg,dog,13,14,100,100".to_string();
            mod_stager::create_mod(
                &repo,
                &branch,
                &user_id,
                &file_path,
                ContentType::Csv,
                ModType::Append,
                data,
            )?;

            // List the files that are changed
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &user_id)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let mods = mod_stager::list_mods_raw(&repo, &branch, &user_id, &commit_entry.path)?;
            assert_eq!(mods.len(), 2);

            // Delete the first append
            mod_stager::clear_mods(&repo, &branch, &user_id, &file_path)?;

            // Should be zero staged files
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch, &user_id)?;
            assert_eq!(commit_entries.len(), 0);

            // Should be zero mods left
            let mods = mod_stager::list_mods_raw(&repo, &branch, &user_id, &commit_entry.path)?;
            assert_eq!(mods.len(), 0);

            Ok(())
        })
    }
}
