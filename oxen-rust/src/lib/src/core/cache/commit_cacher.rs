//! This module goes through a repo and caches values on commits that will never change
//! but are expensive to compute at runtime

use crate::constants::{CACHE_DIR, HISTORY_DIR};
use crate::core::cache::cacher_status::{CacherStatus, CacherStatusType};
use crate::core::db::{self, str_json_db};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;

use super::cachers::{content_stats, content_validator, repo_size};
use lazy_static::lazy_static;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::PathBuf;

type CommitCacher = fn(&LocalRepository, &Commit) -> Result<(), OxenError>;

lazy_static! {
    /// These are all the cachers we are going to run in `run_all`
    /// TODO: make this a config file that users can extend or run their own cachers
    static ref CACHERS: Vec<(String, CommitCacher)> = vec![
        (String::from("COMMIT_CONTENT_IS_VALID"), content_validator::compute as CommitCacher),
        (String::from("REPO_SIZE"), repo_size::compute as CommitCacher),
        (String::from("COMMIT_STATS"), content_stats::compute as CommitCacher),
        // (String::from("ARROW_CONVERSION"), convert_to_arrow::convert_to_arrow as CommitCacher),
    ];
}

fn cached_status_db_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("status.db")
}

/// Pick most appropriate status to return given the status's in the db
pub fn get_status(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Option<CacherStatusType>, OxenError> {
    let vals = get_all_statuses(repo, commit)?;
    if vals.iter().any(|v| CacherStatusType::Pending == v.status) {
        return Ok(Some(CacherStatusType::Pending));
    }

    if vals.iter().any(|v| CacherStatusType::Failed == v.status) {
        return Ok(Some(CacherStatusType::Failed));
    }

    Ok(vals
        .into_iter()
        .find(|v| CacherStatusType::Success == v.status)
        .map(|v| v.status))
}

/// Return all the statuses from cacher processes that were run
pub fn get_failures(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<CacherStatus>, OxenError> {
    let db_path = cached_status_db_path(repo, commit);
    let opts = db::opts::default();
    let db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let vals = str_json_db::list_vals::<MultiThreaded, CacherStatus>(&db)?
        .into_iter()
        .filter(|v| v.status == CacherStatusType::Failed)
        .collect();
    Ok(vals)
}

/// Return all the statuses from cacher processes that were run
pub fn get_all_statuses(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<CacherStatus>, OxenError> {
    let db_path = cached_status_db_path(repo, commit);
    // Check if db path exists
    if !db_path.exists() {
        return Ok(vec![]);
    }
    let opts = db::opts::default();
    let db = DBWithThreadMode::open(&opts, dunce::simplified(&db_path));
    match db {
        Ok(db) => {
            let vals = str_json_db::list_vals::<MultiThreaded, CacherStatus>(&db)?;
            Ok(vals)
        }
        Err(_) => {
            // If db path exists, and error, it's probably because we are trying to open from different processes
            log::debug!("Could not open db....still processing");
            Ok(vec![CacherStatus::pending()])
        }
    }
}

/// Run all the cachers and update their status's as you go
pub fn run_all(repo: &LocalRepository, commit: &Commit, force: bool) -> Result<(), OxenError> {
    // Create kvdb of NAME -> STATUS
    let db_path = cached_status_db_path(repo, commit);
    let opts = db::opts::default();
    let db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    for (name, cacher) in CACHERS.iter() {
        // Skip ones that are already cached successfully
        if let Some(val) = str_json_db::get::<MultiThreaded, &str, CacherStatus>(&db, name)? {
            if CacherStatusType::Success == val.status && !force {
                continue;
            }
        }

        // set as pending
        let pending_status = CacherStatus::pending();
        str_json_db::put(&db, name, &pending_status)?;

        // run and set appropriate status
        match cacher(repo, commit) {
            Ok(_) => {
                let status_success = CacherStatus::success();
                str_json_db::put(&db, name, &status_success)?;
            }
            Err(err) => {
                let err = format!("Err: {err}");
                log::error!("{}", err);
                let status_failed = CacherStatus::failed(&err);
                str_json_db::put(&db, name, &status_failed)?;
            }
        }
    }

    Ok(())
}
