use crate::config::UserConfig;
use crate::constants::{COMMITS_DB, MERGE_HEAD_FILE, ORIG_HEAD_FILE};
use crate::df::DFOpts;
use crate::error::OxenError;
use crate::index::{
    mod_stager, CommitDBReader, CommitDirReader, CommitEntryWriter, RefReader, RefWriter,
};
use crate::model::{
    Branch, Commit, CommitEntry, NewCommit, StagedData, StagedEntry, StagedEntryStatus,
};
use crate::opts::RestoreOpts;
use crate::{command, db};
use crate::{df, util};
use polars::prelude::*;

use indicatif::ProgressBar;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::str;
use time::OffsetDateTime;

use crate::model::LocalRepository;

use super::remote_dir_stager;

pub struct CommitWriter {
    pub commits_db: DBWithThreadMode<MultiThreaded>,
    repository: LocalRepository,
}

impl CommitWriter {
    pub fn commit_db_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(COMMITS_DB))
    }

    pub fn new(repository: &LocalRepository) -> Result<CommitWriter, OxenError> {
        let db_path = CommitWriter::commit_db_dir(&repository.path);

        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }

        let opts = db::opts::default();
        Ok(CommitWriter {
            commits_db: DBWithThreadMode::open(&opts, &db_path)?,
            repository: repository.clone(),
        })
    }

    fn create_new_commit_data(&self, message: &str) -> Result<NewCommit, OxenError> {
        let cfg = UserConfig::get()?;
        let timestamp = OffsetDateTime::now_utc();
        let ref_reader = RefReader::new(&self.repository)?;
        // Commit
        //  - parent_ids (can be empty if root)
        //  - message
        //  - date
        //  - author
        match ref_reader.head_commit_id() {
            Ok(Some(parent_id)) => {
                // We might be in a merge commit, in which case we would have multiple parents
                if self.is_merge_commit() {
                    log::debug!("Create merge commit...");
                    self.create_merge_commit(message)
                } else {
                    // We have one parent
                    log::debug!("Create commit with parent {:?}", parent_id);
                    Ok(NewCommit {
                        parent_ids: vec![parent_id],
                        message: String::from(message),
                        author: cfg.name,
                        email: cfg.email,
                        timestamp,
                    })
                }
            }
            _ => {
                // We are creating initial commit, no parents
                log::debug!("Create initial commit...");
                Ok(NewCommit {
                    parent_ids: vec![],
                    message: String::from(message),
                    author: cfg.name,
                    email: cfg.email,
                    timestamp,
                })
            }
        }
    }

    // Reads commit ids from merge commit files then removes them
    fn create_merge_commit(&self, message: &str) -> Result<NewCommit, OxenError> {
        let cfg = UserConfig::get()?;
        let timestamp = OffsetDateTime::now_utc();
        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
        let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
        let orig_head_path = hidden_dir.join(ORIG_HEAD_FILE);

        // Read parent commit ids
        let merge_commit_id = util::fs::read_from_path(&merge_head_path)?;
        let head_commit_id = util::fs::read_from_path(&orig_head_path)?;

        // Cleanup
        std::fs::remove_file(merge_head_path)?;
        std::fs::remove_file(orig_head_path)?;

        Ok(NewCommit {
            parent_ids: vec![merge_commit_id, head_commit_id],
            message: String::from(message),
            author: cfg.name,
            email: cfg.email,
            timestamp,
        })
    }

    fn is_merge_commit(&self) -> bool {
        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
        let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
        merge_head_path.exists()
    }

    // Create a db in the history/ dir under the id
    // We will have something like:
    // history/
    //   d7966d81ab35ffdf/
    //     annotations.txt -> b"{entry_json}"
    //     train/
    //       image_1.png -> b"{entry_json}"
    //       image_2.png -> b"{entry_json}"
    //       image_2.png -> b"{entry_json}"
    pub fn commit(&self, status: &mut StagedData, message: &str) -> Result<Commit, OxenError> {
        // Create a commit object, that either points to parent or not
        // must create this before anything else so that we know if it has parent or not.
        log::debug!("---COMMIT START---"); // for debug logging / timing purposes
        let new_commit = self.create_new_commit_data(message)?;
        log::debug!("Created commit obj {:?}", new_commit);
        let commit = self.commit_from_new(&new_commit, status, &self.repository.path, None)?;
        log::debug!("COMMIT_COMPLETE {} -> {}", commit.id, commit.message);

        // User output
        println!("Commit {} done.", commit.id);
        log::debug!("---COMMIT END---"); // for debug logging / timing purposes
        Ok(commit)
    }

    pub fn commit_from_new(
        &self,
        new_commit: &NewCommit,
        status: &mut StagedData,
        origin_path: &Path,
        branch: Option<Branch>,
    ) -> Result<Commit, OxenError> {
        let commit = self.gen_commit(new_commit, status);
        log::debug!("Commit Id computed {} -> [{}]", commit.id, commit.message);

        if let Some(branch) = &branch {
            self.apply_mods(branch, status)?;
        }

        // Write entries
        self.add_commit_from_status(&commit, status, origin_path, branch)?;

        Ok(commit)
    }

    pub fn apply_mods(&self, branch: &Branch, status: &mut StagedData) -> Result<(), OxenError> {
        let entries = mod_stager::list_mod_entries(&self.repository, branch)?;
        log::debug!("CommitWriter Apply {} mods", entries.len());
        for entry in entries.iter() {
            let branch_staging_dir =
                remote_dir_stager::branch_staging_dir(&self.repository, branch);

            // Copy the version file to the staging dir and make the mods
            let version_path = util::fs::version_path(&self.repository, entry);
            let entry_path = branch_staging_dir.join(&entry.path);
            if let Some(parent) = entry_path.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            std::fs::copy(&version_path, &entry_path)?;

            let new_hash = self.apply_mods_to_file(branch, entry, &entry_path)?;
            status.added_files.insert(
                entry.path.to_owned(),
                StagedEntry {
                    hash: new_hash.to_owned(),
                    status: StagedEntryStatus::Modified,
                },
            );
        }
        Ok(())
    }

    fn apply_mods_to_file(
        &self,
        branch: &Branch,
        entry: &CommitEntry,
        path: &Path,
    ) -> Result<String, OxenError> {
        if util::fs::is_tabular(path) {
            self.apply_tabular_mods(branch, entry, path)
        } else if util::fs::is_utf8(path) {
            self.apply_utf8_mods(branch, entry, path)
        } else {
            Err(OxenError::basic_str(
                "File type not supported for modifications",
            ))
        }
    }

    fn apply_tabular_mods(
        &self,
        branch: &Branch,
        entry: &CommitEntry,
        path: &Path,
    ) -> Result<String, OxenError> {
        let mut df = df::tabular::read_df(path, DFOpts::empty())?;
        let mods = mod_stager::list_mods(&self.repository, branch, entry)?;
        for modification in mods.iter() {
            let cursor = Cursor::new(modification.data.as_bytes());
            let mod_df = JsonLineReader::new(cursor).finish().unwrap();
            df = df.vstack(&mod_df).unwrap();
        }
        df::tabular::write_df(&mut df, path)?;
        let new_hash = util::hasher::hash_file_contents(path)?;
        Ok(new_hash)
    }

    fn apply_utf8_mods(
        &self,
        branch: &Branch,
        entry: &CommitEntry,
        path: &Path,
    ) -> Result<String, OxenError> {
        let mods = mod_stager::list_mods(&self.repository, branch, entry)?;
        for modification in mods.iter() {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .append(true)
                .open(path)?;
            log::debug!("append to file {:?} -> '{}'", path, modification.data);
            file.write_all(modification.data.as_bytes())?;
        }

        let new_hash = util::hasher::hash_file_contents(path)?;
        Ok(new_hash)
    }

    fn gen_commit(&self, commit_data: &NewCommit, status: &StagedData) -> Commit {
        log::debug!("gen_commit from {} files", status.added_files.len());
        let entries: Vec<StagedEntry> = status.added_files.values().cloned().collect();
        let id = util::hasher::compute_commit_hash(commit_data, &entries);
        log::debug!("gen_commit id {}", id);
        Commit::from_new_and_id(commit_data, id)
    }

    pub fn commit_with_parent_ids(
        &self,
        status: &StagedData,
        parent_ids: Vec<String>,
        message: &str,
    ) -> Result<Commit, OxenError> {
        let cfg = UserConfig::get()?;
        let timestamp = OffsetDateTime::now_utc();

        let commit = NewCommit {
            parent_ids,
            message: String::from(message),
            author: cfg.name,
            email: cfg.email,
            timestamp,
        };
        let entries: Vec<StagedEntry> = status.added_files.values().cloned().collect();
        let id = util::hasher::compute_commit_hash(&commit, &entries);
        let commit = Commit::from_new_and_id(&commit, id);
        self.add_commit_from_status(&commit, status, &self.repository.path, None)?;
        Ok(commit)
    }

    pub fn add_commit_from_empty_status(&self, commit: &Commit) -> Result<(), OxenError> {
        // Empty Status
        let status = StagedData::empty();
        self.add_commit_from_status(commit, &status, &self.repository.path, None)
    }

    pub fn add_commit_from_status(
        &self,
        commit: &Commit,
        status: &StagedData,
        origin_path: &Path,
        branch: Option<Branch>, // optional branch because usually we just want to commit off of HEAD
    ) -> Result<(), OxenError> {
        // Write entries
        let entry_writer = CommitEntryWriter::new(&self.repository, commit)?;
        // Commit all staged files from db
        entry_writer.commit_staged_entries(commit, status, origin_path)?;

        // Add to commits db id -> commit_json
        self.add_commit_to_db(commit)?;

        let ref_writer = RefWriter::new(&self.repository)?;
        if let Some(branch) = branch {
            ref_writer.set_branch_commit_id(&branch.name, &commit.id)?;
        } else {
            ref_writer.set_head_commit_id(&commit.id)?;
        }

        Ok(())
    }

    pub fn add_commit_to_db(&self, commit: &Commit) -> Result<(), OxenError> {
        // Write commit json to db
        let commit_json = serde_json::to_string(&commit)?;
        log::debug!("add_commit_to_db [{}] -> {}", commit.id, commit_json);
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
            log::debug!(
                "set_working_repo_to_commit_id, do nothing... head commit == commit_id {}",
                commit_id
            );

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
        let head_entry_reader = CommitDirReader::new_from_head(&self.repository)?;
        let commit_entry_reader = CommitDirReader::new(&self.repository, &commit)?;
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

                // TODO: Why are we doing...parent.parent here?
                // Keep track of parents to see if we clear them
                if let Some(parent) = path.parent() {
                    log::debug!("adding candidate dir {:?}", parent);

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
        log::debug!("Setting working directory to {}", commit_id);
        log::debug!("got {} candidate dirs", candidate_dirs_to_rm.len());

        // Iterate over files in current commit db, and make sure the hashes match,
        // if different, copy the correct version over
        let commit_entries = commit_entry_reader.list_entries()?;
        println!("Setting working directory to {commit_id}");
        let size: u64 = unsafe { std::mem::transmute(commit_entries.len()) };
        let bar = ProgressBar::new(size);
        for entry in commit_entries.iter() {
            bar.inc(1);
            let path = &entry.path;
            log::debug!("Checking committed entry: {:?} => {:?}", path, entry);
            if let Some(parent) = path.parent() {
                // Check if parent directory exists, if it does, we no longer have
                // it as a candidate to remove
                log::debug!("We aren't going to delete candidate {:?}", parent);
                if candidate_dirs_to_rm.contains(parent) {
                    candidate_dirs_to_rm.remove(&parent.to_path_buf());
                }
            }

            let dst_path = self.repository.path.join(path);
            let version_path = util::fs::version_path(&self.repository, entry);

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

                command::restore(
                    &self.repository,
                    RestoreOpts::from_path_ref(&entry.path, commit_id),
                )?;
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

                    command::restore(
                        &self.repository,
                        RestoreOpts::from_path_ref(&entry.path, commit_id),
                    )?;
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

        log::debug!("candidate_dirs_to_rm {}", candidate_dirs_to_rm.len());
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
            let err = format!("Could not get commit id for branch: {name}");
            Err(OxenError::basic_str(err))
        }
    }

    pub fn get_commit_by_id(&self, commit_id: &str) -> Result<Option<Commit>, OxenError> {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match self.commits_db.get(key) {
            Ok(Some(value)) => {
                let commit: Commit = serde_json::from_str(str::from_utf8(&value)?)?;
                Ok(Some(commit))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!("Error commits_db to find commit_id {commit_id:?}\nErr: {err}");
                Err(OxenError::basic_str(err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::df::DFOpts;
    use crate::error::OxenError;
    use crate::index::{self, remote_dir_stager, CommitDBReader, CommitDirReader, CommitWriter};
    use crate::model::entry::mod_entry::ModType;
    use crate::model::{StagedData, User};
    use crate::{api, command, df, test, util};

    // This is how we initialize
    #[test]
    fn test_commit_no_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            let mut status = StagedData::empty();
            log::debug!("run_empty_stager_test before CommitWriter::new...");
            let commit_writer = CommitWriter::new(&repo)?;
            commit_writer.commit(&mut status, "Init")?;
            stager.unstage()?;

            Ok(())
        })
    }

    #[test]
    fn test_commit_staged() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create committer with no commits
            let repo_path = &repo.path;
            let entry_reader = CommitDirReader::new_from_head(&repo)?;
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
            let mut status = stager.status(&entry_reader)?;
            let commit = commit_writer.commit(&mut status, message)?;
            stager.unstage()?;

            let commit_history =
                CommitDBReader::history_from_commit(&commit_writer.commits_db, &commit)?;

            // should be two commits now
            assert_eq!(commit_history.len(), 2);

            // Check that the files are no longer staged
            let status = stager.status(&entry_reader)?;
            let files = status.added_files;
            assert_eq!(files.len(), 0);
            let dirs = stager.list_added_dirs()?;
            assert_eq!(dirs.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_commit_text_appends_staged() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let repo_path = &repo.path;

            let readme_file = Path::new("README.md");
            let full_path = repo_path.join(readme_file);
            let og_contents = util::fs::read_from_path(&full_path)?;

            // Stage an append
            let branch = command::current_branch(&repo)?.unwrap();
            let branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch).unwrap();

            let append_contents = "\n## New Section".to_string();
            index::mod_stager::create_mod(
                &repo,
                &branch,
                readme_file,
                ModType::Append,
                append_contents.clone(),
            )?;

            let commit = remote_dir_stager::commit_staged(
                &repo,
                &branch_repo,
                &branch,
                &User {
                    name: "Test User".to_string(),
                    email: "test@oxen.ai".to_string(),
                },
                "Appending data",
            )?;

            // Make sure version file is updated
            let entry =
                api::local::entries::get_entry_for_commit(&repo, &commit, readme_file)?.unwrap();
            let version_file = util::fs::version_path(&repo, &entry);
            let new_contents = util::fs::read_from_path(&version_file)?;
            assert_eq!(new_contents, format!("{og_contents}{append_contents}"));

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_append_invalid_schema() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Try stage an append
            let readme_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let branch = command::current_branch(&repo)?.unwrap();

            let append_contents = "{\"file\": \"images/test.jpg\"}".to_string();
            let result = index::mod_stager::create_mod(
                &repo,
                &branch,
                &readme_file,
                ModType::Append,
                append_contents,
            );
            // Should be an error
            assert!(result.is_err());

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_appends_staged() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let annotations_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Stage an append
            let branch = command::current_branch(&repo)?.unwrap();
            let branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch).unwrap();

            let append_contents = "{\"file\": \"images/test.jpg\", \"label\": \"dog\", \"min_x\": 2.0, \"min_y\": 3.0, \"width\": 100, \"height\": 120}".to_string();
            index::mod_stager::create_mod(
                &repo,
                &branch,
                &annotations_file,
                ModType::Append,
                append_contents,
            )?;

            let commit = remote_dir_stager::commit_staged(
                &repo,
                &branch_repo,
                &branch,
                &User {
                    name: "Test User".to_string(),
                    email: "test@oxen.ai".to_string(),
                },
                "Appending tabular data",
            )?;

            // Make sure version file is updated
            let entry =
                api::local::entries::get_entry_for_commit(&repo, &commit, &annotations_file)?
                    .unwrap();
            let version_file = util::fs::version_path(&repo, &entry);

            let data_frame = df::tabular::read_df(version_file, DFOpts::empty())?;
            println!("{data_frame}");
            assert_eq!(
                format!("{data_frame}"),
                r"shape: (7, 6)
┌─────────────────┬───────┬───────┬───────┬───────┬────────┐
│ file            ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
│ ---             ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
│ str             ┆ str   ┆ f64   ┆ f64   ┆ i64   ┆ i64    │
╞═════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
│ train/dog_1.jpg ┆ dog   ┆ 101.5 ┆ 32.0  ┆ 385   ┆ 330    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/dog_1.jpg ┆ dog   ┆ 102.5 ┆ 31.0  ┆ 386   ┆ 330    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/dog_2.jpg ┆ dog   ┆ 7.0   ┆ 29.5  ┆ 246   ┆ 247    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/dog_3.jpg ┆ dog   ┆ 19.0  ┆ 63.5  ┆ 376   ┆ 421    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/cat_1.jpg ┆ cat   ┆ 57.0  ┆ 35.5  ┆ 304   ┆ 427    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/cat_2.jpg ┆ cat   ┆ 30.5  ┆ 44.0  ┆ 333   ┆ 396    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ images/test.jpg ┆ dog   ┆ 2.0   ┆ 3.0   ┆ 100   ┆ 120    │
└─────────────────┴───────┴───────┴───────┴───────┴────────┘"
            );

            Ok(())
        })
    }
}
