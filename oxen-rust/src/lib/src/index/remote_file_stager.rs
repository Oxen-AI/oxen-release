use std::path::{Path, PathBuf};

use time::OffsetDateTime;

use crate::api;
use crate::constants::{APPENDS_DIR, OXEN_HIDDEN_DIR};
use crate::db::{self, str_json_db};
use crate::error::OxenError;
use crate::model::{AppendEntry, Branch, CommitEntry, LocalRepository};

use super::stager::STAGED_DIR;

/// Returns a uuid for the append
pub fn append_to_file(
    repo: &LocalRepository,
    branch: &Branch,
    file_path: &Path,
    content: String,
) -> Result<AppendEntry, OxenError> {
    // should be safe to unwrap if we have a branch
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    match api::local::entries::get_entry_for_commit(repo, &commit, file_path)? {
        Some(entry) => {
            // We are going to create a rocksdb in .oxen/staged/BRANCH_NAME/.oxen/staged/appends/FILE_HASH/
            stage_append(repo, branch, &entry, content)
        }
        None => {
            Err(OxenError::file_does_not_exist_in_commit(
                file_path, &commit.id,
            ))
        }
    }
}

pub fn list_staged_appends(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
) -> Result<Vec<AppendEntry>, OxenError> {
    let db_path = append_db_path(repo, branch, entry);
    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::list_vals(&db)
}

fn append_db_path(repo: &LocalRepository, branch: &Branch, entry: &CommitEntry) -> PathBuf {
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(&branch.name)
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(APPENDS_DIR)
        .join(&entry.hash)
}

fn stage_append(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
    content: String,
) -> Result<AppendEntry, OxenError> {
    let db_path = append_db_path(repo, branch, entry);
    let opts = db::opts::default();
    let db = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let uuid = uuid::Uuid::new_v4().to_string();
    let timestamp = OffsetDateTime::now_utc();

    let entry = AppendEntry {
        uuid: uuid.to_owned(),
        data: content,
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
    use crate::index::remote_file_stager;
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
                remote_file_stager::append_to_file(&repo, &branch, file_path, data)?;

            // List the staged appends
            let appends = remote_file_stager::list_staged_appends(&repo, &branch, &commit_entry)?;
            assert_eq!(appends.len(), 1);
            assert_eq!(appends.first().unwrap().uuid, append_entry.uuid);

            Ok(())
        })
    }
}
