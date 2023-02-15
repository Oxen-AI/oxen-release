use polars::prelude::*;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use time::OffsetDateTime;

use crate::constants::{FILES_DIR, MODS_DIR, OXEN_HIDDEN_DIR};
use crate::db::{self, str_json_db};
use crate::error::OxenError;
use crate::model::entry::mod_entry::ModType;
use crate::model::schema::Field;
use crate::model::{Branch, CommitEntry, LocalRepository, ModEntry};
use crate::{api, util};

use super::stager::STAGED_DIR;
use super::SchemaReader;

fn mods_db_path(repo: &LocalRepository, branch: &Branch, entry: &CommitEntry) -> PathBuf {
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(&branch.name)
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(MODS_DIR)
        .join(MODS_DIR)
        .join(&entry.hash)
}

fn files_db_path(repo: &LocalRepository, branch: &Branch) -> PathBuf {
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(&branch.name)
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(MODS_DIR)
        .join(FILES_DIR)
}

pub fn create_mod(
    repo: &LocalRepository,
    branch: &Branch,
    file_path: &Path,
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
            match stage_mod(repo, branch, &commit_entry, mod_type, content) {
                Ok(mod_entry) => {
                    // If successful track the file it is modifying
                    track_mod_commit_entry(repo, branch, &commit_entry)?;

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

pub fn list_mods(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
) -> Result<Vec<ModEntry>, OxenError> {
    let db_path = mods_db_path(repo, branch, entry);
    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let mut results: Vec<ModEntry> = str_json_db::list_vals(&db)?;
    results.sort_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap());
    Ok(results)
}

pub fn list_mod_entries(
    repo: &LocalRepository,
    branch: &Branch,
) -> Result<Vec<CommitEntry>, OxenError> {
    let db_path = files_db_path(repo, branch);
    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::list_vals(&db)
}

fn track_mod_commit_entry(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
) -> Result<(), OxenError> {
    let db_path = files_db_path(repo, branch);
    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let key = entry.path.to_string_lossy();
    str_json_db::put(&db, key, &entry)
}

fn stage_mod(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
    mod_type: ModType,
    content: String,
) -> Result<ModEntry, OxenError> {
    let version_path = util::fs::version_path(repo, entry);
    if util::fs::is_tabular(&version_path) {
        stage_tabular_mod(repo, branch, entry, mod_type, content)
    } else if util::fs::is_utf8(&version_path) {
        stage_raw_mod_content(repo, branch, entry, mod_type, content)
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
    entry: &CommitEntry,
    mod_type: ModType,
    content: String,
) -> Result<ModEntry, OxenError> {
    // Read the schema of the data frame
    log::debug!(
        "Looking for schema on commit [{}] for entry {:?}",
        entry.commit_id,
        entry.path
    );
    let schema_reader = SchemaReader::new(repo, &entry.commit_id)?;
    if let Some(schema) = schema_reader.get_schema_for_file(&entry.path)? {
        // Parse the json
        let cursor = Cursor::new(content.as_bytes());
        match JsonLineReader::new(cursor).finish() {
            Ok(df) => {
                log::debug!("Successfully parsed df {:?}", df);
                // Make sure it contains each field
                let df_schema = df.schema();
                if schema.has_all_field_names(&df_schema) {
                    stage_raw_mod_content(repo, branch, entry, mod_type, content)
                } else {
                    let schema_fields_str = Field::all_fields_to_string(&schema.fields);
                    let err = format!("Json schema does not contain same fields as DataFrame schema. {schema_fields_str}");
                    Err(OxenError::basic_str(err))
                }
            }
            Err(err) => {
                let err = format!("Error parsing json: {err}");
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        let err = format!("Schema not found for file {:?}", entry.path);
        Err(OxenError::basic_str(err))
    }
}

fn stage_raw_mod_content(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
    mod_type: ModType,
    content: String,
) -> Result<ModEntry, OxenError> {
    let db_path = mods_db_path(repo, branch, entry);
    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let uuid = uuid::Uuid::new_v4().to_string();
    let timestamp = OffsetDateTime::now_utc();

    let entry = ModEntry {
        uuid: uuid.to_owned(),
        data: content,
        modification_type: mod_type,
        path: entry.path.to_owned(),
        timestamp,
    };

    str_json_db::put(&db, &uuid, &entry)?;

    Ok(entry)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::index::mod_stager;
    use crate::model::entry::mod_entry::ModType;
    use crate::test;

    #[test]
    fn test_stage_append() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = command::create_checkout_branch(&repo, branch_name)?;
            let file_path = Path::new("README.md");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_entry_for_commit(&repo, &commit, file_path)?.unwrap();

            // Append the data to staging area
            let data = "Appending this text....".to_string();
            let append_entry =
                mod_stager::create_mod(&repo, &branch, file_path, ModType::Append, data)?;

            // List the files that are changed
            let commit_entries = mod_stager::list_mod_entries(&repo, &branch)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let mods = mod_stager::list_mods(&repo, &branch, &commit_entry)?;
            assert_eq!(mods.len(), 1);
            assert_eq!(mods.first().unwrap().uuid, append_entry.uuid);
            assert_eq!(mods.first().unwrap().path, commit_entry.path);

            Ok(())
        })
    }
}
