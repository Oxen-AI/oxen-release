use crate::error::OxenError;
use crate::index::RefReader;
use crate::model::{Commit, LocalRepository};

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::str;

pub struct CommitDBReader {}

impl CommitDBReader {
    pub fn head_commit(
        repo: &LocalRepository,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<Commit, OxenError> {
        let ref_reader = RefReader::new(repo)?;
        match ref_reader.head_commit_id() {
            Ok(commit_id) => {
                let commit = CommitDBReader::get_commit_by_id(db, &commit_id)?
                    .ok_or(OxenError::commit_db_corrupted(commit_id))?;
                Ok(commit)
            }
            Err(err) => Err(err),
        }
    }

    pub fn root_commit(
        repo: &LocalRepository,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<Commit, OxenError> {
        let head_commit = CommitDBReader::head_commit(repo, db)?;
        CommitDBReader::rget_root_commit(repo, db, &head_commit.id)
    }

    fn rget_root_commit(
        repo: &LocalRepository,
        db: &DBWithThreadMode<MultiThreaded>,
        commit_id: &str,
    ) -> Result<Commit, OxenError> {
        let commit = CommitDBReader::get_commit_by_id(db, commit_id)?
            .ok_or(OxenError::commit_db_corrupted(commit_id))?;
        if let Some(parent_id) = &commit.parent_id {
            Ok(CommitDBReader::rget_root_commit(repo, db, parent_id)?)
        } else {
            Ok(commit)
        }
    }

    pub fn get_commit_by_id(
        db: &DBWithThreadMode<MultiThreaded>,
        commit_id: &str,
    ) -> Result<Option<Commit>, OxenError> {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match db.get(key) {
            Ok(Some(value)) => {
                let commit: Commit = serde_json::from_str(str::from_utf8(&*value)?)?;
                Ok(Some(commit))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!(
                    "Error commits_db to find commit_id {:?}\nErr: {}",
                    commit_id, err
                );
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn commit_id_exists(db: &DBWithThreadMode<MultiThreaded>, commit_id: &str) -> bool {
        match CommitDBReader::get_commit_by_id(db, commit_id) {
            Ok(Some(_commit)) => true,
            Ok(None) => false,
            Err(err) => {
                log::error!("commit_id_exists err: {:?}", err);
                false
            }
        }
    }

    pub fn history_from_commit(
        db: &DBWithThreadMode<MultiThreaded>,
        commit: &Commit,
    ) -> Result<Vec<Commit>, OxenError> {
        let mut commit_msgs: Vec<Commit> = vec![];
        // Start with head, and the get parents until there are no parents
        CommitDBReader::history_from_commit_id(db, &commit.id, &mut commit_msgs)?;
        Ok(commit_msgs)
    }

    pub fn history_from_commit_id(
        db: &DBWithThreadMode<MultiThreaded>,
        commit_id: &str,
        commits: &mut Vec<Commit>,
    ) -> Result<(), OxenError> {
        if let Some(commit) = CommitDBReader::get_commit_by_id(db, commit_id)? {
            commits.push(commit.clone());
            if let Some(parent_id) = &commit.parent_id {
                CommitDBReader::history_from_commit_id(db, parent_id, commits)?;
            }
        }
        Ok(())
    }
}
