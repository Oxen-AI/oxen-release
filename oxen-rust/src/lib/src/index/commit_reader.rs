
use crate::constants::COMMITS_DB;
use crate::error::OxenError;
use crate::index::{RefReader, CommitDBReader};
use crate::model::Commit;
use crate::util;

use rocksdb::{DBWithThreadMode, LogLevel, MultiThreaded, Options};
use std::str;

use crate::model::LocalRepository;


pub struct CommitReader {
    repository: LocalRepository,
    db: DBWithThreadMode<MultiThreaded>,
}

impl CommitReader {
    fn db_opts() -> Options {
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Fatal);
        opts.create_if_missing(true);
        opts
    }

    /// Create a new reader that can find commits, list history, etc
    pub fn new(repository: &LocalRepository) -> Result<CommitReader, OxenError> {
        let db_path = util::fs::oxen_hidden_dir(&repository.path).join(COMMITS_DB);
        let opts = CommitReader::db_opts();
        Ok(CommitReader {
            repository: repository.clone(),
            db: DBWithThreadMode::open_for_read_only(&opts, &db_path, false)?,
        })
    }

    /// Return the head commit
    pub fn head_commit(&self) -> Result<Commit, OxenError> {
        CommitDBReader::head_commit(&self.repository, &self.db)
    }

    /// List the commit history starting at a commit id
    pub fn history_from_commit_id(&self, commit_id: &str) -> Result<Vec<Commit>, OxenError> {
        let mut commits: Vec<Commit> = vec![];
        self.p_list_commits(&commit_id, &mut commits)?;
        Ok(commits)
    }

    /// List the commit history from the HEAD commit
    pub fn history_from_head(&self) -> Result<Vec<Commit>, OxenError> {
        let mut commit_msgs: Vec<Commit> = vec![];
        let ref_reader = RefReader::new(&self.repository)?;
        // Start with head, and the get parents until there are no parents
        match ref_reader.head_commit_id() {
            Ok(commit_id) => {
                self.p_list_commits(&commit_id, &mut commit_msgs)?;
                Ok(commit_msgs)
            }
            Err(_) => Ok(commit_msgs),
        }
    }

    /// See if a commit id exists
    pub fn commit_id_exists(&self, commit_id: &str) -> bool {
        CommitDBReader::commit_id_exists(&self.db, commit_id)
    }

    /// Get a commit object from an ID
    pub fn get_commit_by_id(&self, commit_id: &str) -> Result<Option<Commit>, OxenError> {
        CommitDBReader::get_commit_by_id(&self.db, commit_id)
    }

    fn p_list_commits(&self, commit_id: &str, commits: &mut Vec<Commit>) -> Result<(), OxenError> {
        if let Some(commit) = self.get_commit_by_id(commit_id)? {
            commits.push(commit.clone());
            if let Some(parent_id) = &commit.parent_id {
                self.p_list_commits(parent_id, commits)?;
            }
        }
        Ok(())
    }
}