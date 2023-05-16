use crate::constants::COMMITS_DIR;
use crate::core::db;
use crate::core::index::CommitDBReader;
use crate::error::OxenError;
use crate::model::Commit;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::str;

use crate::model::LocalRepository;

pub struct CommitReader {
    repository: LocalRepository,
    db: DBWithThreadMode<MultiThreaded>,
}

impl CommitReader {
    /// Create a new reader that can find commits, list history, etc
    pub fn new(repository: &LocalRepository) -> Result<CommitReader, OxenError> {
        let db_path = util::fs::oxen_hidden_dir(&repository.path).join(COMMITS_DIR);
        let opts = db::opts::default();

        log::debug!("CommitReader::new db_path: {:?}", db_path);

        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
        }

        Ok(CommitReader {
            repository: repository.clone(),
            db: DBWithThreadMode::open_for_read_only(&opts, &db_path, false)?,
        })
    }

    /// Return the head commit
    pub fn head_commit(&self) -> Result<Commit, OxenError> {
        CommitDBReader::head_commit(&self.repository, &self.db)
    }

    /// Get the root commit of the db
    pub fn root_commit(&self) -> Result<Commit, OxenError> {
        CommitDBReader::root_commit(&self.repository, &self.db)
    }

    /// List the commit history starting at a commit id
    pub fn history_from_commit_id(&self, commit_id: &str) -> Result<Vec<Commit>, OxenError> {
        let mut commits: HashSet<Commit> = HashSet::new();
        CommitDBReader::history_from_commit_id(&self.db, commit_id, &mut commits)?;
        let mut commits: Vec<Commit> = commits.into_iter().collect();
        commits.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(commits)
    }

    pub fn history_from_base_to_head(
        &self,
        base_commit_id: &str,
        head_commit_id: &str,
    ) -> Result<Vec<Commit>, OxenError> {
        let mut commits: HashSet<Commit> = HashSet::new();
        CommitDBReader::history_from_base_to_head(
            &self.db,
            base_commit_id,
            head_commit_id,
            &mut commits,
        )?;
        let mut commits: Vec<Commit> = commits.into_iter().collect();
        commits.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(commits)
    }

    /// List the commit history from the HEAD commit
    pub fn history_from_head(&self) -> Result<Vec<Commit>, OxenError> {
        if self.repository.is_shallow_clone() {
            return Err(OxenError::repo_is_shallow());
        }

        let head_commit = self.head_commit()?;
        let mut commits: Vec<Commit> = CommitDBReader::history_from_commit(&self.db, &head_commit)?
            .into_iter()
            .collect();
        commits.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(commits)
    }

    /// List the commit history from a commit keeping track of depth along the way
    pub fn history_with_depth_from_commit(
        &self,
        commit: &Commit,
    ) -> Result<HashMap<Commit, usize>, OxenError> {
        CommitDBReader::history_with_depth_from_commit(&self.db, commit)
    }

    /// List the commit history from a commit keeping track of depth along the way
    pub fn history_with_depth_from_head(&self) -> Result<HashMap<Commit, usize>, OxenError> {
        let head = self.head_commit()?;
        CommitDBReader::history_with_depth_from_commit(&self.db, &head)
    }

    /// See if a commit id exists
    pub fn commit_id_exists(&self, commit_id: &str) -> bool {
        CommitDBReader::commit_id_exists(&self.db, commit_id)
    }

    /// Get a commit object from an ID
    pub fn get_commit_by_id<S: AsRef<str>>(
        &self,
        commit_id: S,
    ) -> Result<Option<Commit>, OxenError> {
        CommitDBReader::get_commit_by_id(&self.db, commit_id.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::constants::INITIAL_COMMIT_MSG;
    use crate::core::index::CommitReader;
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_get_root_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commit_reader = CommitReader::new(&repo)?;
            let root_commit = commit_reader.root_commit()?;

            assert_eq!(root_commit.message, INITIAL_COMMIT_MSG);

            Ok(())
        })
    }

    #[test]
    fn test_commit_history_order() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let train_dir = repo.path.join("train");
            command::add(&repo, train_dir)?;
            command::commit(&repo, "adding train dir")?;

            let test_dir = repo.path.join("test");
            command::add(&repo, test_dir)?;
            let most_recent_message = "adding test dir";
            command::commit(&repo, most_recent_message)?;

            let commit_reader = CommitReader::new(&repo)?;
            let history = commit_reader.history_from_head()?;
            assert_eq!(history.len(), 3);

            assert_eq!(history.first().unwrap().message, most_recent_message);
            assert_eq!(history.last().unwrap().message, INITIAL_COMMIT_MSG);

            Ok(())
        })
    }

    #[test]
    fn test_get_commit_history_base_head() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let new_file = repo.path.join("new_1.txt");
            test::write_txt_file_to_path(&new_file, "new 1")?;
            command::add(&repo, new_file)?;
            let base_commit = command::commit(&repo, "commit 1")?;

            let new_file = repo.path.join("new_2.txt");
            test::write_txt_file_to_path(&new_file, "new 2")?;
            command::add(&repo, new_file)?;
            let first_new_commit = command::commit(&repo, "commit 2")?;

            let new_file = repo.path.join("new_3.txt");
            test::write_txt_file_to_path(&new_file, "new 3")?;
            command::add(&repo, new_file)?;
            let head_commit = command::commit(&repo, "commit 3")?;

            let new_file = repo.path.join("new_4.txt");
            test::write_txt_file_to_path(&new_file, "new 4")?;
            command::add(&repo, new_file)?;
            command::commit(&repo, "commit 4")?;

            let commit_reader = CommitReader::new(&repo)?;
            let history =
                commit_reader.history_from_base_to_head(&base_commit.id, &head_commit.id)?;
            assert_eq!(history.len(), 2);

            assert_eq!(history.first().unwrap().message, head_commit.message);
            assert_eq!(history.last().unwrap().message, first_new_commit.message);

            Ok(())
        })
    }
}
