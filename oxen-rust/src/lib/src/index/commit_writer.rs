use crate::config::AuthConfig;
use crate::constants::{COMMITS_DB, VERSIONS_DIR};
use crate::db;
use crate::error::OxenError;
use crate::index::{CommitDBReader, CommitEntryReader, CommitEntryWriter, RefReader, RefWriter};
use crate::model::{Commit, StagedData};
use crate::util;

use chrono::Utc;
use indicatif::ProgressBar;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;

use crate::model::LocalRepository;

pub struct CommitWriter {
    pub commits_db: DBWithThreadMode<MultiThreaded>,
    versions_dir: PathBuf,
    auth_cfg: AuthConfig,
    repository: LocalRepository,
}

impl CommitWriter {
    pub fn versions_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(VERSIONS_DIR))
    }

    pub fn commit_db_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(COMMITS_DB))
    }

    pub fn new(repository: &LocalRepository) -> Result<CommitWriter, OxenError> {
        let db_path = CommitWriter::commit_db_dir(&repository.path);
        let versions_path = CommitWriter::versions_dir(&repository.path);

        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }

        let opts = db::opts::default();
        Ok(CommitWriter {
            commits_db: DBWithThreadMode::open(&opts, &db_path)?,
            versions_dir: versions_path,
            auth_cfg: AuthConfig::default().unwrap(),
            repository: repository.clone(),
        })
    }

    fn create_commit_obj(&self, id_str: &str, message: &str) -> Result<Commit, OxenError> {
        let ref_reader = RefReader::new(&self.repository)?;
        // Commit
        //  - parent_ids (can be empty if root)
        //  - message
        //  - date
        //  - author
        match ref_reader.head_commit_id() {
            Ok(parent_id) => {
                // We have a parent
                Ok(Commit {
                    id: String::from(id_str),
                    parent_ids: vec![parent_id],
                    message: String::from(message),
                    author: self.auth_cfg.user.name.clone(),
                    date: Utc::now(),
                })
            }
            Err(_) => {
                // We are creating initial commit, no parents
                Ok(Commit {
                    id: String::from(id_str),
                    parent_ids: vec![],
                    message: String::from(message),
                    author: self.auth_cfg.user.name.clone(),
                    date: Utc::now(),
                })
            }
        }
    }

    // Create a db in the history/ dir under the id
    // We will have something like:
    // history/
    //   3a54208a-f792-45c1-8505-e325aa4ce5b3/
    //     annotations.txt -> b"{entry_json}"
    //     train/image_1.png -> b"{entry_json}"
    //     train/image_2.png -> b"{entry_json}"
    //     test/image_2.png -> b"{entry_json}"
    pub fn commit(&self, status: &StagedData, message: &str) -> Result<Commit, OxenError> {
        // Generate uniq id for this commit
        let commit_id = format!("{}", uuid::Uuid::new_v4());

        // Create a commit object, that either points to parent or not
        // must create this before anything else so that we know if it has parent or not.
        let commit = self.create_commit_obj(&commit_id, message)?;
        log::debug!("COMMIT_START {} message [{}]", commit.id, commit.message,);

        // Write entries
        self.add_commit_from_status(&commit, status)?;

        // println!("COMMIT_COMPLETE {} -> {}", commit.id, commit.message);

        Ok(commit)
    }

    pub fn add_commit_from_empty_status(&self, commit: &Commit) -> Result<(), OxenError> {
        // Empty Status
        let status = StagedData::empty();
        self.add_commit_from_status(commit, &status)
    }

    pub fn add_commit_from_status(
        &self,
        commit: &Commit,
        status: &StagedData,
    ) -> Result<(), OxenError> {
        // Write entries
        let entry_writer = CommitEntryWriter::new(&self.repository, commit)?;
        // Commit all staged files from db
        entry_writer.add_staged_entries(commit, &status.added_files)?;

        // Commit all staged dirs from db, and recursively add all the files
        entry_writer.add_staged_dirs(commit, &status.added_dirs)?;

        // Add to commits db id -> commit_json
        self.add_commit_to_db(commit)?;

        // Move head to commit id
        let ref_writer = RefWriter::new(&self.repository)?;
        ref_writer.set_head_commit_id(&commit.id)?;

        Ok(())
    }

    pub fn add_commit_to_db(&self, commit: &Commit) -> Result<(), OxenError> {
        // Write commit json to db
        let commit_json = serde_json::to_string(&commit)?;
        self.commits_db.put(&commit.id, commit_json.as_bytes())?;
        Ok(())
    }

    pub fn set_working_repo_to_commit_id(&self, commit_id: &str) -> Result<(), OxenError> {
        if !CommitDBReader::commit_id_exists(&self.commits_db, commit_id) {
            return Err(OxenError::commit_id_does_not_exist(commit_id));
        }
        log::debug!("set_working_repo_to_commit_id: {}", commit_id);

        let head_commit = CommitDBReader::head_commit(&self.repository, &self.commits_db)?;
        if head_commit.id == commit_id {
            // Don't do anything if we tried to switch to same commit
            return Ok(());
        }

        // Keep track of directories, since we do not explicitly store which ones are tracked...
        // we will remove them later if no files exist in them.
        let mut candidate_dirs_to_rm: HashSet<PathBuf> = HashSet::new();

        // Iterate over files in that are in *current head* and make sure they should all be there
        // if they aren't in commit db we are switching to, remove them
        // Safe to unwrap because we check if it exists above
        let commit = CommitDBReader::get_commit_by_id(&self.commits_db, commit_id)?.unwrap();
        log::debug!(
            "set_working_repo_to_commit_id: Commit: {} => '{}'",
            commit_id,
            commit.message
        );

        // Two readers, one for HEAD and one for this current commit
        let head_entry_reader = CommitEntryReader::new_from_head(&self.repository)?;
        let commit_entry_reader = CommitEntryReader::new(&self.repository, &commit)?;
        let commit_entries = head_entry_reader.list_files()?;
        log::debug!(
            "set_working_repo_to_commit_id got {} entries in commit",
            commit_entries.len()
        );

        for path in commit_entries.iter() {
            let repo_path = self.repository.path.join(path);
            log::debug!(
                "set_working_repo_to_commit_id commit_entries[{:?}]",
                repo_path
            );
            if repo_path.is_file() {
                log::debug!(
                    "set_working_repo_to_commit_id commit_id {} path {:?}",
                    commit_id,
                    path
                );

                // Keep track of parents to see if we clear them
                if let Some(parent) = path.parent() {
                    if parent.parent().is_some() {
                        // only add one directory below top level
                        // println!("set_working_repo_to_commit_id candidate dir {:?}", parent);
                        candidate_dirs_to_rm.insert(parent.to_path_buf());
                    }
                }

                if commit_entry_reader.has_file(path) {
                    // We already have file ✅
                    log::debug!(
                        "set_working_repo_to_commit_id we already have file ✅ {:?}",
                        repo_path
                    );
                } else {
                    // sorry, we don't know you, bye
                    log::debug!("set_working_repo_to_commit_id see ya 💀 {:?}", repo_path);
                    std::fs::remove_file(repo_path)?;
                }
            }
        }

        // Iterate over files in current commit db, and make sure the hashes match,
        // if different, copy the correct version over
        let commit_entries = commit_entry_reader.list_entries()?;
        println!("Setting working directory to {}", commit_id);
        let size: u64 = unsafe { std::mem::transmute(commit_entries.len()) };
        let bar = ProgressBar::new(size);
        for entry in commit_entries.iter() {
            bar.inc(1);
            let path = &entry.path;
            log::debug!("Checking committed entry: {:?} => {:?}", path, entry);
            if let Some(parent) = path.parent() {
                // Check if parent directory exists, if it does, we no longer have
                // it as a candidate to remove
                // println!("CHECKING {:?}", parent);
                if candidate_dirs_to_rm.contains(parent) {
                    candidate_dirs_to_rm.remove(&parent.to_path_buf());
                }
            }

            let dst_path = self.repository.path.join(path);

            // Check the versioned file hash
            let version_filename = entry.filename();
            let entry_version_dir = self.versions_dir.join(&entry.id);

            for f in util::fs::rlist_files_in_dir(&entry_version_dir) {
                log::debug!("VERSION FILE: {:?} version: {:?}", path, f);
            }

            let version_path = entry_version_dir.join(version_filename);

            // If we do not have the file, restore it from our versioned history
            if !dst_path.exists() {
                log::debug!(
                    "set_working_repo_to_commit_id restore file, she new 🙏 {:?} -> {:?}",
                    version_path,
                    dst_path
                );

                // mkdir if not exists for the parent
                if let Some(parent) = dst_path.parent() {
                    if !parent.exists() {
                        std::fs::create_dir_all(parent)?;
                    }
                }

                std::fs::copy(version_path, dst_path)?;
            } else {
                // we do have it, check if we need to update it
                let dst_hash = util::hasher::hash_file_contents(&dst_path)?;

                // let old_contents = util::fs::read_from_path(&version_path)?;
                // let current_contents = util::fs::read_from_path(&dst_path)?;
                // log::debug!("old_contents {:?}\n{}", version_path, old_contents);
                // log::debug!("current_contents {:?}\n{}", dst_path, current_contents);

                // If the hash of the file from the commit is different than the one on disk, update it
                if entry.hash != dst_hash {
                    // we need to update working dir
                    log::debug!(
                        "set_working_repo_to_commit_id restore file diff hash 🙏 {:?} -> {:?}",
                        version_path,
                        dst_path
                    );
                    std::fs::copy(version_path, dst_path)?;
                } else {
                    log::debug!(
                        "set_working_repo_to_commit_id hashes match! {:?} -> {:?}",
                        version_path,
                        dst_path
                    );
                }
            }
        }

        bar.finish();

        if !candidate_dirs_to_rm.is_empty() {
            println!("Cleaning up...");
        }

        // Remove un-tracked directories
        for dir in candidate_dirs_to_rm.iter() {
            let full_dir = self.repository.path.join(dir);
            // println!("set_working_repo_to_commit_id remove dis dir {:?}", full_dir);
            std::fs::remove_dir_all(full_dir)?;
        }

        Ok(())
    }

    pub fn set_working_repo_to_branch(&self, name: &str) -> Result<(), OxenError> {
        let ref_reader = RefReader::new(&self.repository)?;
        if let Some(commit_id) = ref_reader.get_commit_id_for_branch(name)? {
            self.set_working_repo_to_commit_id(&commit_id)
        } else {
            let err = format!("Could not get commit id for branch: {}", name);
            Err(OxenError::basic_str(&err))
        }
    }
    pub fn get_commit_by_id(&self, commit_id: &str) -> Result<Option<Commit>, OxenError> {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match self.commits_db.get(key) {
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
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::index::{CommitDBReader, CommitEntryReader, CommitWriter};
    use crate::model::StagedData;
    use crate::test;

    // This is how we initialize
    #[test]
    fn test_commit_no_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            let status = StagedData::empty();
            log::debug!("run_empty_stager_test before CommitWriter::new...");
            let commit_writer = CommitWriter::new(&repo)?;
            commit_writer.commit(&status, "Init")?;
            stager.unstage()?;

            Ok(())
        })
    }

    #[test]
    fn test_commit_staged() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create committer with no commits
            let repo_path = &repo.path;
            let entry_reader = CommitEntryReader::new_from_head(&repo)?;
            let commit_writer = CommitWriter::new(&repo)?;

            let train_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&train_dir)?;
            let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 1")?;
            let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 2")?;
            let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 3")?;
            let annotation_file = test::add_txt_file_to_dir(repo_path, "some annotations...")?;

            let test_dir = repo_path.join("test_data");
            std::fs::create_dir_all(&test_dir)?;
            let _ = test::add_txt_file_to_dir(&test_dir, "Test Ex 1")?;
            let _ = test::add_txt_file_to_dir(&test_dir, "Test Ex 2")?;

            // Add a file and a directory
            stager.add_file(&annotation_file, &entry_reader)?;
            stager.add_dir(&train_dir, &entry_reader)?;

            let message = "Adding training data to 🐂";
            let status = stager.status(&entry_reader)?;
            let commit = commit_writer.commit(&status, message)?;
            stager.unstage()?;

            let commit_history =
                CommitDBReader::history_from_commit(&commit_writer.commits_db, &commit)?;

            // always start with an initial commit
            assert_eq!(commit_history.len(), 2);
            assert_eq!(commit_history[0].id, commit.id);
            assert_eq!(commit_history[0].message, message);

            // Check that the files are no longer staged
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 0);
            let dirs = stager.list_added_directories()?;
            assert_eq!(dirs.len(), 0);

            Ok(())
        })
    }
}
