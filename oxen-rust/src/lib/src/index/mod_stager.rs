use std::path::{Path, PathBuf};

use time::OffsetDateTime;

use crate::api;
use crate::constants::{FILES_DIR, MODS_DIR, OXEN_HIDDEN_DIR};
use crate::db::{self, str_json_db};
use crate::error::OxenError;
use crate::model::entry::mod_entry::ModType;
use crate::model::{Branch, CommitEntry, LocalRepository, ModEntry};

use super::stager::STAGED_DIR;

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
        Some(entry) => {
            // We track the files that are modified
            track_mod_commit_entry(repo, branch, &entry)?;
            // Then the mod itself
            stage_mod(repo, branch, &entry, mod_type, content)
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
