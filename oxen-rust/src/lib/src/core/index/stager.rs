//! # Stager
//!
//! Struct responsible for interacting with the staged data before commit
//! Adds files during `oxen add` and computes files for `oxen status`
//!

use crate::constants;
use crate::core::db;
use crate::core::db::path_db;
use crate::core::df::tabular;
use crate::core::index::oxenignore;
use crate::core::index::{
    CommitDirEntryReader, CommitEntryReader, CommitReader, MergeConflictReader, Merger,
    StagedDirEntryDB,
};
use crate::error::OxenError;
use crate::opts::DFOpts;

use crate::model::schema;
use crate::model::{
    CommitEntry, LocalRepository, MergeConflict, StagedData, StagedDirStats, StagedEntry,
    StagedEntryStatus,
};
use crate::util;

use filetime::FileTime;
use ignore::gitignore::Gitignore;
use indicatif::ProgressBar;
use itertools::Itertools;
use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use rocksdb::SingleThreaded;
use rocksdb::ThreadMode;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str;

use super::StagedDirEntryReader;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FileStatus {
    Added,
    Untracked,
    Modified,
    Removed,
}

pub struct Stager {
    dir_db: DBWithThreadMode<MultiThreaded>,
    schemas_db: DBWithThreadMode<MultiThreaded>,
    pub repository: LocalRepository,
    merger: Option<Merger>,
}

impl Stager {
    pub fn dirs_db_path(path: &Path) -> Result<PathBuf, OxenError> {
        let path = util::fs::oxen_hidden_dir(path)
            .join(Path::new(constants::STAGED_DIR))
            .join(constants::DIRS_DIR);

        log::debug!("Stager new dir dir_db_path {:?}", path);
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        Ok(path)
    }

    pub fn schemas_db_path(path: &Path) -> Result<PathBuf, OxenError> {
        let path = util::fs::oxen_hidden_dir(path)
            .join(Path::new(constants::STAGED_DIR))
            .join(constants::SCHEMAS_DIR);
        log::debug!("Stager new dir schemas_db_path {:?}", path);
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(path)
    }

    pub fn new(repository: &LocalRepository) -> Result<Stager, OxenError> {
        let dir_db_path = Stager::dirs_db_path(&repository.path)?;
        let schemas_db_path = Stager::schemas_db_path(&repository.path)?;

        let opts = db::opts::default();
        Ok(Stager {
            dir_db: DBWithThreadMode::open(&opts, dunce::simplified(&dir_db_path))?,
            schemas_db: DBWithThreadMode::open(&opts, dunce::simplified(&schemas_db_path))?,
            repository: repository.clone(),
            merger: None,
        })
    }

    pub fn new_with_merge(repository: &LocalRepository) -> Result<Stager, OxenError> {
        let dir_db_path = Stager::dirs_db_path(&repository.path)?;
        let schemas_db_path = Stager::schemas_db_path(&repository.path)?;

        let opts = db::opts::default();
        Ok(Stager {
            dir_db: DBWithThreadMode::open(&opts, dunce::simplified(&dir_db_path))?,
            schemas_db: DBWithThreadMode::open(&opts, dunce::simplified(&schemas_db_path))?,
            repository: repository.clone(),
            merger: Some(Merger::new(&repository.clone())?),
        })
    }

    fn should_ignore_path(&self, ignore: &Option<Gitignore>, path: &Path) -> bool {
        // If the path is the .oxen dir or is in the ignore file, ignore it
        let should_ignore = if let Some(ignore) = ignore {
            ignore.matched(path, path.is_dir()).is_ignore()
        } else {
            false
        };

        should_ignore || util::fs::is_in_oxen_hidden_dir(path)
    }

    pub fn add(
        &self,
        path: &Path,
        commit_reader: &CommitEntryReader,
        ignore: &Option<Gitignore>,
    ) -> Result<(), OxenError> {
        if self.repository.is_shallow_clone() {
            return Err(OxenError::repo_is_shallow());
        }

        if self.should_ignore_path(ignore, path) {
            return Ok(());
        }

        log::debug!("stager.add({:?})", path);

        // Be able to add the current dir
        if path == Path::new(".") {
            for entry in (std::fs::read_dir(path)?).flatten() {
                let path = entry.path();
                let entry_path = self.repository.path.join(path);
                self.add(&entry_path, commit_reader, ignore)?;
            }
            log::debug!("ADD CURRENT DIR: {:?}", path);
            return Ok(());
        }

        // If it doesn't exist on disk, it might have been removed, and we can't tell if it is a file or dir
        // so we have to check if it is committed, and what the backup version is
        if !path.exists() {
            let relative_path = util::fs::path_relative_to_dir(path, &self.repository.path)?;
            log::debug!(
                "Stager.add() !path.exists() checking relative path: {:?}",
                relative_path
            );
            // Since entries that are committed are only files.. we will have to have different logic for dirs
            if let Ok(Some(value)) = commit_reader.get_entry(&relative_path) {
                self.add_removed_file(&relative_path, &value)?;
                return Ok(());
            }

            let files_in_dir = commit_reader.list_directory(&relative_path)?;
            log::debug!(
                "Stager.add() !path.exists() {} files in dir {:?}",
                files_in_dir.len(),
                relative_path
            );
            if !files_in_dir.is_empty() {
                println!("Removing {} files", files_in_dir.len());
                let pb = ProgressBar::new(files_in_dir.len() as u64);
                for entry in files_in_dir.iter() {
                    self.add_removed_file(&entry.path, entry)?;
                    pb.inc(1);
                }
                pb.finish();

                log::debug!(
                    "Stager.add() !path.exists() !files_in_dir.is_empty() {:?}",
                    path
                );
                return Ok(());
            }
        }

        log::debug!("Stager.add() is_dir? {} path: {:?}", path.is_dir(), path);
        if path.is_dir() {
            match self.add_dir(path, commit_reader) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            match self.add_file(path, commit_reader) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        }
    }

    pub fn status(&self, entry_reader: &CommitEntryReader) -> Result<StagedData, OxenError> {
        log::debug!("-----status START-----");
        let result = self.compute_staged_data(&self.repository.path, entry_reader);
        log::debug!("-----status END-----");
        result
    }

    // TODO: allow status for just certain type of files (add, mod, removed, etc) for performance gains

    // TODO: allow status for a certain directory for performance gains

    pub fn status_from_dir(
        &self,
        entry_reader: &CommitEntryReader,
        dir: &Path,
    ) -> Result<StagedData, OxenError> {
        log::debug!("-----status_from_dir START-----");
        let result = self.compute_staged_data(dir, entry_reader);
        log::debug!("-----status_from_dir END-----");
        result
    }

    fn list_merge_conflicts(&self) -> Result<Vec<MergeConflict>, OxenError> {
        let merger = MergeConflictReader::new(&self.repository)?;
        merger.list_conflicts()
    }

    fn compute_staged_data(
        &self,
        dir: &Path,
        entry_reader: &CommitEntryReader,
    ) -> Result<StagedData, OxenError> {
        log::debug!(
            "compute_staged_data listing eligible repo -> {:?} dir -> {:?}",
            self.repository.path,
            dir
        );

        if self.repository.is_shallow_clone() {
            return Err(OxenError::repo_is_shallow());
        }

        let mut staged_data = StagedData::empty();
        let ignore = oxenignore::create(&self.repository);

        let mut candidate_dirs: HashSet<PathBuf> = HashSet::new();
        // Start with candidate dirs from committed and added, not all the dirs
        let mut added_dirs = self.list_staged_dirs()?;
        // If we specified a dir, only get the added dirs that are in that dir
        if dir.is_relative() && dir != self.repository.path {
            added_dirs.retain(|(path, _)| path.starts_with(dir))
        }

        log::debug!("compute_staged_data Got <added> dirs: {}", added_dirs.len());
        for (dir, status) in added_dirs {
            log::debug!("compute_staged_data considering added dir {:?}", dir);
            let full_path = self.repository.path.join(&dir);
            let stats = self.compute_staged_dir_stats(&full_path, &status)?;
            staged_data.added_dirs.add_stats(&stats);
            log::debug!("compute_staged_data got stats {:?}", stats);

            log::debug!("compute_staged_data adding <added> dir {:?}", dir);
            candidate_dirs.insert(self.repository.path.join(dir));
        }

        let mut committed_dirs = entry_reader.list_dirs()?;
        if dir.is_relative() && dir != self.repository.path {
            committed_dirs.retain(|path| path.starts_with(dir))
        }
        log::debug!(
            "compute_staged_data Got <committed> dirs: {}",
            committed_dirs.len()
        );
        for dir in committed_dirs.iter() {
            log::debug!("compute_staged_data adding <committed> dir {:?}", dir);
            if !self.should_ignore_path(&ignore, dir) {
                candidate_dirs.insert(self.repository.path.join(dir));
            }
        }

        log::debug!("compute_staged_data Considering <current> dir: {:?}", dir);
        candidate_dirs.insert(dir.to_path_buf());

        for dir in candidate_dirs.iter() {
            log::debug!("compute_staged_data CANDIDATE DIR {:?}", dir);
            self.process_dir(dir, &mut staged_data, &ignore)?;
        }

        // Find merge conflicts
        staged_data.merge_conflicts = self.list_merge_conflicts()?;

        // Populate schemas from db
        let mut schemas: HashMap<PathBuf, schema::Schema> = HashMap::new();
        for (path, schema) in path_db::list_path_entries(&self.schemas_db, Path::new(""))? {
            schemas.insert(path, schema);
        }
        staged_data.added_schemas = schemas;

        Ok(staged_data)
    }

    fn process_dir(
        &self,
        full_dir: &Path,
        staged_data: &mut StagedData,
        ignore: &Option<Gitignore>,
    ) -> Result<(), OxenError> {
        // log::debug!("process_dir {:?}", full_dir);
        // Only check at level of this dir, no need to deep dive recursively
        let committer = CommitReader::new(&self.repository)?;
        let commit = committer.head_commit()?;
        let root_commit_dir_reader = CommitEntryReader::new(&self.repository, &commit)?;
        let relative_dir = util::fs::path_relative_to_dir(full_dir, &self.repository.path)?;
        let staged_dir_db: StagedDirEntryDB<SingleThreaded> =
            StagedDirEntryDB::new(&self.repository, &relative_dir)?;
        let root_commit_entry_reader =
            CommitDirEntryReader::new(&self.repository, &commit.id, &relative_dir)?;

        // Create candidate files paths to look at
        let mut candidate_files: HashSet<PathBuf> = HashSet::new();

        // Only consider working dir if it is on disk, otherwise we will grab from history
        let read_dir = std::fs::read_dir(full_dir);
        if read_dir.is_ok() {
            // Files in working directory as candidates
            for path in read_dir? {
                let path = path?.path();
                let path = util::fs::path_relative_to_dir(&path, &self.repository.path)?;
                if !self.should_ignore_path(ignore, &path) {
                    // log::debug!("adding candidate from dir {:?}", path);
                    candidate_files.insert(path);
                }
            }
        }

        // and files that were in commit as candidates
        for entry in root_commit_entry_reader.list_entries()? {
            // log::debug!("adding candidate from commit {:?}", entry.path);
            if !self.should_ignore_path(ignore, &entry.path) {
                candidate_files.insert(entry.path);
            }
        }
        log::debug!(
            "Got {} candidates in directory {:?}",
            candidate_files.len(),
            relative_dir
        );

        for relative in candidate_files.iter() {
            // log::debug!("process_dir checking relative path {:?}", relative);
            if util::fs::is_in_oxen_hidden_dir(relative) {
                continue;
            }

            let fullpath = self.repository.path.join(relative);

            // log::debug!(
            //     "process_dir checking is_dir? {} {:?}",
            //     fullpath.is_dir(),
            //     fullpath
            // );

            if fullpath.is_dir() {
                if !self.has_staged_dir(relative)
                    && !staged_data.added_dirs.contains_key(relative)
                    && !root_commit_dir_reader.has_dir(relative)
                {
                    // log::debug!("process_dir adding untracked dir {:?}", relative);
                    let count = util::fs::count_items_in_dir(&fullpath);
                    staged_data
                        .untracked_dirs
                        .push((relative.to_path_buf(), count));
                }
            } else {
                // is file
                let file_status = Stager::get_file_status(
                    &self.repository.path,
                    relative,
                    &staged_dir_db,
                    &root_commit_entry_reader,
                );
                // log::debug!("process_dir got status {:?} {:?}", relative, file_status);
                if let Some(file_type) = file_status {
                    match file_type {
                        FileStatus::Added => {
                            let file_name = relative.file_name().unwrap();
                            let result = staged_dir_db.get_entry(file_name);
                            if let Ok(Some(entry)) = result {
                                staged_data
                                    .added_files
                                    .insert(relative.to_path_buf(), entry);
                            }
                        }
                        FileStatus::Untracked => {
                            staged_data.untracked_files.push(relative.to_path_buf());
                        }
                        FileStatus::Modified => {
                            staged_data.modified_files.push(relative.to_path_buf());
                        }
                        FileStatus::Removed => {
                            staged_data.removed_files.push(relative.to_path_buf());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_file_status<T: ThreadMode>(
        full_dir: &Path,
        path: &Path,
        staged_dir_db: &StagedDirEntryDB<T>,
        commit_dir_db: &CommitDirEntryReader,
    ) -> Option<FileStatus> {
        let file_name = path.file_name().unwrap();
        // log::debug!("get_file_status check path in staging? {:?}", file_name);

        // Have to check the file basename not the path, because basenames are stored in each dir
        if staged_dir_db.has_entry(file_name) {
            return Some(FileStatus::Added);
        } else {
            // Not in the staged DB
            // log::debug!("get_file_status check if commit db? {:?}", file_name);
            // check if it is in the HEAD commit to see if it is modified or removed
            if let Some(file_name) = path.file_name() {
                if let Ok(Some(commit_entry)) = commit_dir_db.get_entry(file_name) {
                    if Stager::file_is_removed(full_dir, &commit_entry) {
                        return Some(FileStatus::Removed);
                    } else if Stager::file_is_modified(full_dir, &commit_entry) {
                        return Some(FileStatus::Modified);
                    }
                } else {
                    return Some(FileStatus::Untracked);
                }
            }
        }

        None
    }

    fn file_is_removed(repo_path: &Path, commit_entry: &CommitEntry) -> bool {
        let full_path = repo_path.join(&commit_entry.path);
        // log::debug!(
        //     "CHECKING REMOVED {:?} -> {:?}",
        //     repo_path,
        //     commit_entry.path
        // );
        // log::debug!("CHECKING REMOVED {:?}", full_path);
        !full_path.exists()
    }

    fn file_is_modified(repo_path: &Path, commit_entry: &CommitEntry) -> bool {
        // Get last modified time
        let full_path = repo_path.join(&commit_entry.path);
        // log::debug!(
        //     "CHECKING MODIFIED {:?} -> {:?}",
        //     repo_path,
        //     commit_entry.path
        // );
        // log::debug!("CHECKING MODIFIED {:?}", full_path);

        if !full_path.exists() {
            // might have been removed
            return false;
        }

        let metadata = fs::metadata(&full_path).unwrap();
        let mtime = FileTime::from_last_modification_time(&metadata);

        // log::debug!(
        //     "file_is_modified comparing timestamps: {} to {}",
        //     commit_entry.last_modified_nanoseconds,
        //     mtime.nanoseconds()
        // );

        if commit_entry.has_different_modification_time(&mtime) {
            // log::debug!(
            //     "file_is_modified modification times are different! {:?}",
            //     full_path
            // );

            // Then check the hashes, because the data might not be different, timestamp is just an optimization
            let hash = util::hasher::hash_file_contents(&full_path).unwrap();
            if hash != commit_entry.hash {
                return true;
            }
        }

        false
    }

    pub fn has_staged_file(&self, path: &Path) -> Result<bool, OxenError> {
        if let (Some(parent), Some(filename)) = (path.parent(), path.file_name()) {
            let reader = StagedDirEntryReader::new(&self.repository, parent)?;
            Ok(reader.has_entry(filename))
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    pub fn remove_staged_file(&self, path: &Path) -> Result<(), OxenError> {
        log::debug!("remove_staged_file {:?}", path);
        if let (Some(parent), Some(filename)) = (path.parent(), path.file_name()) {
            log::debug!(
                "remove_staged_file got filename {:?} and parent {:?}",
                filename,
                parent
            );

            let staged_dir: StagedDirEntryDB<SingleThreaded> =
                StagedDirEntryDB::new(&self.repository, parent)?;
            staged_dir.remove_path(filename)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    fn add_removed_file(&self, path: &Path, entry: &CommitEntry) -> Result<StagedEntry, OxenError> {
        log::debug!("add_removed_file {:?}", path);
        if let (Some(parent), Some(filename)) = (path.parent(), path.file_name()) {
            log::debug!(
                "add_removed_file got filename {:?} and parent {:?}",
                filename,
                parent
            );

            // add parent to staged dir db
            let short_path = util::fs::path_relative_to_dir(parent, &self.repository.path)?;
            path_db::put(&self.dir_db, short_path, &StagedEntryStatus::Removed)?;

            let staged_dir: StagedDirEntryDB<SingleThreaded> =
                StagedDirEntryDB::new(&self.repository, parent)?;
            staged_dir.add_removed_file(filename, entry)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    // Returns a map of directories to files to add, and a total count
    // Reads the dirs in parallel to quickly find out what needs to be added
    fn list_unadded_files_in_dir<P: AsRef<Path>>(
        &self,
        dir: P,
    ) -> (HashMap<PathBuf, Vec<PathBuf>>, usize) {
        let mut files: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let mut total: usize = 0;
        let repository = self.repository.to_owned();

        // TODO:
        // * Fix to be more readable
        let walk_dir = WalkDirGeneric::<((), Option<bool>)>::new(&dir)
            .skip_hidden(true)
            .process_read_dir(move |_, parent, _, children| {
                let parent = util::fs::path_relative_to_dir(parent, &repository.path).unwrap();
                let reader = match StagedDirEntryReader::new(&repository, &parent) {
                    Ok(db) => db,
                    Err(err) => {
                        log::error!("Error creating staged dir db: {:?}", err);
                        return;
                    }
                };

                children.par_iter_mut().for_each(|child_result| {
                    match child_result {
                        Ok(child) => {
                            // log::debug!(
                            //     "list_unadded_files_in_dir checking file type {:?}",
                            //     dir_entry
                            // );
                            if !child.file_type.is_dir() {
                                // Entry is file
                                let path = child.path();
                                let path =
                                    match util::fs::path_relative_to_dir(path, &repository.path) {
                                        Ok(p) => p,
                                        Err(err) => {
                                            log::error!("Error path_relative_to_dir: {:?}", err);
                                            return;
                                        }
                                    };

                                let is_added = reader.has_entry(path);
                                child.client_state = Some(is_added);
                            }
                        }
                        Err(err) => {
                            log::error!("list_unadded_files_in_dir dir entry is err: {:?}", err);
                        }
                    }
                });
            });

        for dir_entry_result in walk_dir {
            // log::debug!(
            //     "list_unadded_files_in_dir in for loop {:?}",
            //     dir_entry_result,
            // );
            match dir_entry_result {
                Ok(dir_entry) => {
                    // log::debug!(
                    //     "list_unadded_files_in_dir match dir_entry_result {:?}",
                    //     &dir_entry.client_state,
                    // );
                    if let Some(is_added) = &dir_entry.client_state {
                        if !*is_added {
                            let path = util::fs::path_relative_to_dir(
                                &dir_entry.path(),
                                &self.repository.path,
                            )
                            .unwrap();
                            // log::debug!(
                            //     "list_unadded_files_in_dir got path {:?}",
                            //     path,
                            // );
                            if let Some(parent) = path.parent() {
                                // log::debug!(
                                //     "list_unadded_files_in_dir adding {:?} -> {:?}",
                                //     parent,
                                //     path
                                // );

                                files.entry(parent.to_path_buf()).or_default().push(path);
                                total += 1;
                            }
                        }
                    }
                    // log::debug!(
                    //     "list_unadded_files_in_dir match dir_entry_result done. {:?}",
                    //     dir_entry,
                    // );
                }
                Err(error) => {
                    log::error!("Read dir_entry error: {error}");
                }
            }
        }
        (files, total)
    }

    pub fn add_dir(&self, dir: &Path, entry_reader: &CommitEntryReader) -> Result<(), OxenError> {
        if !dir.exists() || !dir.is_dir() {
            let err = format!("Cannot stage non-existant dir: {dir:?}");
            return Err(OxenError::basic_str(err));
        }

        // add the the directory to list of dirs we are tracking so that when we find untracked files
        // they are added to the list
        let short_path = util::fs::path_relative_to_dir(dir, &self.repository.path)?;
        path_db::put(&self.dir_db, short_path, &StagedEntryStatus::Added)?;
        // log::debug!("Stager.add_dir added path {short_path:?}");

        // Add all untracked files and modified files
        let (dir_paths, total) = self.list_unadded_files_in_dir(dir);
        // log::debug!("Stager.add_dir {:?} -> {}", dir, total);

        // println!("Adding files in directory: {short_path:?}");
        let size: u64 = unsafe { std::mem::transmute(total) };
        let bar = ProgressBar::new(size);
        dir_paths.par_iter().for_each(|(parent, paths)| {
            // log::debug!("dir_paths.par_iter().foreach {:?} -> {:?}", parent, paths.len());

            let staged_db: StagedDirEntryDB<MultiThreaded> =
                StagedDirEntryDB::new(&self.repository, parent).unwrap();
            let entry_reader = match CommitDirEntryReader::new(
                &self.repository,
                &entry_reader.commit_id,
                parent,
            ) {
                Ok(reader) => reader,
                Err(err) => {
                    log::error!("Could not create CommitDirEntryReader: {}", err);
                    return;
                }
            };

            // log::debug!("paths.len() {:?}", paths.len());
            paths.par_iter().for_each(|path| {
                // log::debug!("paths.par_iter().foreach {:?}", path);

                let full_path = self.repository.path.join(path);
                match self.add_staged_entry_in_dir_db(&full_path, &entry_reader, &staged_db) {
                    Ok(_) => {
                        // all good
                    }
                    Err(err) => {
                        log::error!("Could not add file: {:?}\nErr: {}", path, err);
                    }
                }
                bar.inc(1);
            });
        });

        bar.finish();

        Ok(())
    }

    pub fn has_staged_dir<P: AsRef<Path>>(&self, dir: P) -> bool {
        path_db::has_entry(&self.dir_db, dir)
    }

    pub fn has_entry<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = path.as_ref();
        if let Ok(relative) = util::fs::path_relative_to_dir(path, &self.repository.path) {
            if let Some(parent) = relative.parent() {
                if let Ok(staged_dir) = StagedDirEntryReader::new(&self.repository, parent) {
                    let filename = relative.file_name().unwrap().to_str().unwrap();
                    return staged_dir.has_entry(filename);
                } else {
                    log::debug!(
                        "Stager.has_entry({:?}) could not find parent db {:?}",
                        path,
                        parent
                    );
                }
            }
        }
        false
    }

    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<StagedEntry>, OxenError> {
        let path = path.as_ref();
        let relative = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        if let Some(parent) = relative.parent() {
            if let Some(file_name) = relative.file_name() {
                log::debug!("get_entry got parent for path {:?} -> {:?}", path, parent);
                log::debug!("get_entry relative {:?}", file_name);

                let staged_db = StagedDirEntryReader::new(&self.repository, parent)?;
                return staged_db.get_entry(file_name);
            } else {
                log::warn!("get_entry could not get file_name: {:?}", path);
            }
        } else {
            log::warn!("get_entry no parent for path: {:?}", path);
        }
        Ok(None)
    }

    pub fn add_file(
        &self,
        path: &Path,
        entry_reader: &CommitEntryReader,
    ) -> Result<PathBuf, OxenError> {
        log::debug!("--- START OXEN ADD {:?} ---", path);
        let relative = self.add_staged_entry(path, entry_reader)?;

        // We should tracking changes to this parent dir too
        let path_parent = path.parent();
        if let Some(parent) = path_parent {
            let relative_parent = util::fs::path_relative_to_dir(parent, &self.repository.path)?;
            log::debug!("add_file got parent {:?}", relative_parent);
            if !self.has_entry(&relative_parent) && relative_parent != Path::new("") {
                log::debug!("add_file({:?}) adding parent {:?}", path, relative_parent);
                path_db::put(&self.dir_db, relative_parent, &StagedEntryStatus::Added)?;
            }
        }

        log::debug!("--- END OXEN ADD ({:?}) ---", path);

        Ok(relative)
    }

    /// Update the name of a staged schema, assuming it exists
    pub fn update_schema_names_for_hash(&self, hash: &str, name: &str) -> Result<(), OxenError> {
        for (path, mut schema) in path_db::list_path_entries::<MultiThreaded, schema::Schema>(
            &self.schemas_db,
            Path::new(""),
        )? {
            if schema.hash == hash {
                schema.name = Some(String::from(name));
                path_db::put(&self.schemas_db, path, &schema)?;
            }
        }
        Ok(())
    }

    pub fn get_staged_schema(&self, schema_ref: &str) -> Result<Option<schema::Schema>, OxenError> {
        for schema in path_db::list_entries::<MultiThreaded, schema::Schema>(&self.schemas_db)? {
            if schema.hash == schema_ref || schema.name == Some(schema_ref.to_string()) {
                return Ok(Some(schema));
            }
        }
        Ok(None)
    }

    pub fn list_staged_schemas(&self) -> Result<Vec<schema::Schema>, OxenError> {
        Ok(
            path_db::list_entries::<MultiThreaded, schema::Schema>(&self.schemas_db)?
                .into_iter()
                .unique_by(|p| p.hash.to_owned())
                .collect::<Vec<_>>(),
        )
    }

    fn add_staged_entry(
        &self,
        path: &Path,
        entry_reader: &CommitEntryReader,
    ) -> Result<PathBuf, OxenError> {
        log::debug!("add_staged_entry {:?}", path);
        if let Some(parent) = path.parent() {
            let relative_parent = util::fs::path_relative_to_dir(parent, &self.repository.path)?;
            let staged_db: StagedDirEntryDB<MultiThreaded> =
                StagedDirEntryDB::new(&self.repository, &relative_parent)?;
            let entry_reader = CommitDirEntryReader::new(
                &self.repository,
                &entry_reader.commit_id,
                &relative_parent,
            )?;

            self.add_staged_entry_in_dir_db(path, &entry_reader, &staged_db)
        } else {
            log::error!("add_staged_entry no parent... {:?}", path);
            Err(OxenError::file_has_no_parent(path))
        }
    }

    fn add_staged_entry_in_dir_db<T: ThreadMode>(
        &self,
        path: &Path,
        entry_reader: &CommitDirEntryReader,
        staged_db: &StagedDirEntryDB<T>,
    ) -> Result<PathBuf, OxenError> {
        // We should have normalized to path past repo at this point
        log::debug!("Add file: {:?} to {:?}", path, self.repository.path);
        if !path.exists() {
            return Err(OxenError::entry_does_not_exist(path));
        }

        // compute the hash to know if it has changed
        let hash = util::hasher::hash_file_contents(path)?;

        // Key is the filename relative to the repository
        // if repository: /Users/username/Datasets/MyRepo
        //   /Users/username/Datasets/MyRepo/train -> train
        //   /Users/username/Datasets/MyRepo/annotations/train.txt -> annotations/train.txt
        let path = util::fs::path_relative_to_dir(path, &self.repository.path)?;

        let mut staged_entry = StagedEntry {
            hash: hash.to_owned(),
            status: StagedEntryStatus::Added,
        };

        // Check if it is a merge conflict, then we can add it
        if let Some(merger) = &self.merger {
            if merger.has_file(&path)? {
                log::debug!("add_staged_entry_in_dir_db merger has file! {:?}", path);
                self.add_staged_entry_to_db(&path, &staged_entry, staged_db)?;
                merger.remove_conflict_path(&path)?;
                return Ok(path);
            }
        }

        // Check if file has changed on disk
        // Since we are using a CommitDirEntryReader we need the base file name
        let basename = path.file_name().unwrap().to_str().unwrap();
        if let Ok(Some(entry)) = entry_reader.get_entry(basename) {
            log::debug!(
                "add_staged_entry_in_dir_db comparing hashes {:?} -> {:?}",
                staged_entry,
                entry
            );
            if entry.hash == hash {
                // file has not changed, don't add it
                log::debug!(
                    "add_staged_entry_in_dir_db do not add file, it hasn't changed: {:?}",
                    path
                );
                return Ok(path);
            } else {
                // Hash doesn't match, mark it as modified
                log::debug!(
                    "add_staged_entry_in_dir_db HASH DOESN'T MATCH {:?}",
                    entry.path
                );
                staged_entry.status = StagedEntryStatus::Modified;
            }
        }

        log::debug!("add_staged_entry_in_dir_db {:?} {:?}", path, staged_entry);
        self.add_staged_entry_to_db(&path, &staged_entry, staged_db)?;

        Ok(path)
    }

    fn add_staged_entry_to_db<T: ThreadMode>(
        &self,
        path: &Path,
        staged_entry: &StagedEntry,
        staged_db: &StagedDirEntryDB<T>,
    ) -> Result<(), OxenError> {
        let relative = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        if let Some(file_name) = relative.file_name() {
            // add all parents up to root
            let mut components = path.components().collect::<Vec<_>>();
            log::debug!("add_staged_entry_to_db got components {}", components.len());
            while !components.is_empty() {
                if let Some(_component) = components.pop() {
                    let parent: PathBuf = components.iter().collect();
                    log::debug!("add_staged_entry_to_db got parent {:?}", parent);
                    log::debug!("add_staged_entry_to_db adding parent {:?}", parent);
                    path_db::put(&self.dir_db, parent, &StagedEntryStatus::Added)?;
                }
            }

            // If tabular, add schema
            if util::fs::is_tabular(path) {
                log::debug!(
                    "add_staged_entry_to_db is tabular! compute schema {:?}",
                    path
                );
                let full_path = self.repository.path.join(path);

                match tabular::read_df(&full_path, DFOpts::empty()) {
                    Ok(df) => {
                        let schema = schema::Schema::from_polars(&df.schema());
                        log::debug!(
                            "add_staged_entry_to_db is tabular! got schema {:?} -> {:?}",
                            full_path,
                            schema
                        );

                        path_db::put(&self.schemas_db, path, &schema)?;
                    }
                    Err(err) => {
                        log::warn!("Could not compute schema for file: {}", err);
                    }
                }
            }

            staged_db.add_staged_entry_to_db(file_name, staged_entry)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    fn list_added_files_in_dir(&self, dir: &Path) -> Result<Vec<PathBuf>, OxenError> {
        let relative = util::fs::path_relative_to_dir(dir, &self.repository.path)?;
        let staged_dir = StagedDirEntryReader::new(&self.repository, &relative)?;
        staged_dir.list_added_paths()
    }

    pub fn list_staged_dirs(&self) -> Result<Vec<(PathBuf, StagedEntryStatus)>, OxenError> {
        path_db::list_path_entries(&self.dir_db, Path::new(""))
    }

    pub fn compute_staged_dir_stats(
        &self,
        path: &Path,
        status: &StagedEntryStatus,
    ) -> Result<StagedDirStats, OxenError> {
        let relative_path = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        log::debug!("compute_staged_dir_stats {:?} -> {:?}", relative_path, path);
        let mut stats = StagedDirStats {
            path: relative_path.to_owned(),
            num_files_staged: 0,
            total_files: 0,
            status: status.to_owned(),
        };

        // Only consider directories
        if !path.is_dir() {
            log::debug!("compute_staged_dir_stats path is not dir {:?}", path);
            return Ok(stats);
        }

        // Count in db from relative path
        let num_files_staged = self.list_added_files_in_dir(&relative_path)?.len();

        // Make sure we have some files added
        if num_files_staged == 0 {
            log::debug!("compute_staged_dir_stats num_files_staged == 0 {:?}", path);
            return Ok(stats);
        }

        // Count in fs from full path
        stats.total_files = util::fs::count_files_in_dir(path);
        stats.num_files_staged = num_files_staged;

        Ok(stats)
    }

    pub fn list_removed_files(
        &self,
        entry_reader: &CommitEntryReader,
    ) -> Result<Vec<PathBuf>, OxenError> {
        // TODO: We are looping multiple times to check whether file is added,modified,or removed, etc
        //       We should do this loop once, and check each thing
        let mut paths: Vec<PathBuf> = vec![];
        for short_path in entry_reader.list_files()? {
            let path = self.repository.path.join(&short_path);
            if !path.exists() && !self.has_entry(&short_path) {
                paths.push(short_path);
            }
        }
        Ok(paths)
    }

    pub fn list_untracked_files(
        &self,
        entry_reader: &CommitEntryReader,
    ) -> Result<Vec<PathBuf>, OxenError> {
        let dir_entries = std::fs::read_dir(&self.repository.path)?;
        // println!("Listing untracked files from {:?}", dir_entries);
        let num_in_head = entry_reader.num_entries()?;
        log::debug!(
            "stager::list_untracked_files head has {} files",
            num_in_head
        );

        let mut paths: Vec<PathBuf> = vec![];
        for entry in dir_entries {
            let local_path = entry?.path();
            if local_path.is_file() {
                // Return relative path with respect to the repo
                let relative_path =
                    util::fs::path_relative_to_dir(&local_path, &self.repository.path)?;
                log::debug!(
                    "stager::list_untracked_files considering path {:?}",
                    relative_path
                );

                // File is committed in HEAD
                if entry_reader.has_file(&relative_path) {
                    continue;
                }

                // File is staged
                if !self.has_entry(&relative_path) {
                    paths.push(relative_path);
                }
            }
        }

        Ok(paths)
    }

    pub fn remove_staged_dir<P: AsRef<Path>>(&self, short_path: P) -> Result<(), OxenError> {
        let short_path = short_path.as_ref();

        log::debug!("Remove staged dir short_path: {:?}", short_path);

        // Not most efficient to linearly scan, but we don't have pointers to parents or children
        let added_dirs = self.list_staged_dirs()?;
        for (added_dir, _) in added_dirs.iter() {
            if added_dir.starts_with(short_path) {
                log::debug!("Removing files from added_dir: {:?}", added_dir);

                // Remove all files within that dir
                let staged_dir: StagedDirEntryDB<MultiThreaded> =
                    StagedDirEntryDB::new(&self.repository, added_dir)?;
                staged_dir.unstage()?;

                // Remove from dir db
                path_db::delete(&self.dir_db, added_dir)?;
            }
        }

        Ok(())
    }

    pub fn unstage(&self) -> Result<(), OxenError> {
        let added_dirs = self.list_staged_dirs()?;
        log::debug!("Unstage dirs: {}", added_dirs.len());
        for (dir, _) in added_dirs {
            log::debug!("Unstaging dir: {:?}", dir);
            let staged_dir: StagedDirEntryDB<MultiThreaded> =
                StagedDirEntryDB::new(&self.repository, &dir)?;
            staged_dir.unstage()?;
        }
        let staged_dir_db: StagedDirEntryDB<MultiThreaded> =
            StagedDirEntryDB::new(&self.repository, Path::new(""))?;
        staged_dir_db.unstage()?;
        path_db::clear(&self.dir_db)?;
        path_db::clear(&self.schemas_db)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::index::{oxenignore, CommitEntryReader, CommitReader, CommitWriter, Stager};
    use crate::error::OxenError;
    use crate::model::StagedEntryStatus;
    use crate::test;
    use crate::util;

    use std::path::{Path, PathBuf};

    #[test]
    fn test_stager_unstage() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            // Add a file and a directory
            stager.add_file(&hello_file, &entry_reader)?;
            stager.add_dir(&sub_dir, &entry_reader)?;

            // Make sure the counts start properly
            let status = stager.status(&entry_reader)?;
            assert_eq!(status.added_files.len(), 3);
            assert_eq!(status.added_dirs.paths.len(), 1);

            // Unstage
            stager.unstage()?;

            // There should no longer be any added files
            let status = stager.status(&entry_reader)?;
            status.print_stdout();
            assert_eq!(status.added_files.len(), 0);
            assert_eq!(status.added_dirs.paths.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_twice_only_adds_once() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Make sure we have a valid file
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add it twice
            stager.add_file(&hello_file, &entry_reader)?;
            stager.add_file(&hello_file, &entry_reader)?;

            // Make sure we still only have it once
            let status = stager.status(&entry_reader)?;
            assert_eq!(status.added_files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_cannot_add_if_not_modified() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Make sure we have a valid file
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add it
            stager.add_file(&hello_file, &entry_reader)?;

            // Commit it
            let commit_writer = CommitWriter::new(&repo)?;
            let mut status = stager.status(&entry_reader)?;
            let commit = commit_writer.commit(&mut status, "Add Hello World")?;
            stager.unstage()?;

            // try to add it again
            let entry_reader = CommitEntryReader::new(&repo, &commit)?;
            stager.add_file(&hello_file, &entry_reader)?;

            // make sure we don't have it added again, because the hash hadn't changed since last commit
            let status = stager.status(&entry_reader)?;
            assert_eq!(status.added_files.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_non_existant_file() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let hello_file = PathBuf::from("non-existant.txt");
            if stager.add_file(&hello_file, &entry_reader).is_ok() {
                // we don't want to be able to add this file
                panic!("test_add_non_existant_file() Cannot stage non-existant file")
            }

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_file() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let hello_file = test::add_txt_file_to_dir(&stager.repository.path, "Hello 1")?;
            stager.add_file(&hello_file, &entry_reader)?;

            let status = stager.status(&entry_reader)?;
            assert_eq!(status.added_files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_single_add_file_in_dir() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to directories
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data").join("deeper");
            std::fs::create_dir_all(&sub_dir)?;
            let file = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            assert!(stager.add_file(&file, &entry_reader).is_ok());

            let status = stager.status(&entry_reader)?;
            assert_eq!(status.added_files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_directory() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to directories
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_dir(&sub_dir, &entry_reader)?;

            let status = stager.status(&entry_reader)?;
            status.print_stdout();
            assert_eq!(status.added_files.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_stager_get_entry() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;

            // Stage file
            stager.add_file(&hello_file, &entry_reader)?;

            // we should be able to fetch this entry json
            let entry = stager.get_entry(relative_path).unwrap().unwrap();
            assert!(!entry.hash.is_empty());
            assert_eq!(entry.status, StagedEntryStatus::Added);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;

            // Stage file
            stager.add_file(&hello_file, &entry_reader)?;

            // List files
            let status = stager.status(&entry_reader)?;
            let files = status.added_files;
            assert_eq!(files.len(), 1);
            assert!(files.get(&relative_path).is_some());

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_file_in_sub_dir() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let sub_file = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_file(&sub_file, &entry_reader)?;

            // List files
            let status = stager.status(&entry_reader)?;
            let files = status.added_files;

            // There is one file
            assert_eq!(files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(&sub_file, repo_path)?;
            assert!(files.contains_key(&relative_path));

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_all_files_in_sub_dir() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let training_data_dir = PathBuf::from("training_data");
            let sub_dir = repo_path.join(&training_data_dir);
            std::fs::create_dir_all(&sub_dir)?;

            let sub_file_1 = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let sub_file_2 = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;
            let sub_file_3 = test::add_txt_file_to_dir(&sub_dir, "Hello 3")?;

            let dirs = stager.status(&entry_reader)?.untracked_dirs;

            // There is one directory
            assert_eq!(dirs.len(), 1);

            // Then we add all three
            stager.add_file(&sub_file_1, &entry_reader)?;
            stager.add_file(&sub_file_2, &entry_reader)?;
            stager.add_file(&sub_file_3, &entry_reader)?;

            // There now there are no untracked directories
            let dirs = stager.status(&entry_reader)?.untracked_dirs;
            assert_eq!(dirs.len(), 0);

            // And there is one tracked directory
            let added_dirs = stager.status(&entry_reader)?.added_dirs;
            assert_eq!(added_dirs.len(), 1);
            let added_dir = added_dirs.get(&training_data_dir).unwrap();
            assert_eq!(added_dir.num_files_staged, 3);
            assert_eq!(added_dir.total_files, 3);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_directories() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let training_data_dir = PathBuf::from("training_data");
            let sub_dir = repo_path.join(&training_data_dir);
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_dir(&sub_dir, &entry_reader)?;

            // List files
            let dirs = stager.status(&entry_reader)?.added_dirs;

            // There is one directory
            assert_eq!(dirs.len(), 1);
            let added_dir = dirs.get(&training_data_dir).unwrap();
            assert_eq!(added_dir.path, training_data_dir);

            // With two files
            assert_eq!(added_dir.num_files_staged, 2);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello 1")?;

            // Do not add...

            // List files
            let files = stager.list_untracked_files(&entry_reader)?;
            assert_eq!(files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(hello_file, repo_path)?;
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_modified_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello 1")?;

            // add the file
            stager.add_file(&hello_file, &entry_reader)?;

            // commit the file
            let mut status = stager.status(&entry_reader)?;
            let commit_writer = CommitWriter::new(&repo)?;
            let commit = commit_writer.commit(&mut status, "added hello 1")?;
            stager.unstage()?;

            let mod_files = stager.status(&entry_reader)?.modified_files;
            assert_eq!(mod_files.len(), 0);

            // modify the file
            let hello_file = test::modify_txt_file(hello_file, "Hello 2")?;

            // List files
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;
            let status = stager.status(&entry_reader)?;
            status.print_stdout();
            let mod_files = status.modified_files;
            assert_eq!(mod_files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(hello_file, repo_path)?;
            assert_eq!(mod_files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_dirs() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            // Must have some sort of file in the dir to add it.
            test::write_txt_file_to_path(sub_dir.join("hi.txt"), "Hi")?;

            // Do not add...

            // List files
            let dirs = stager.status(&entry_reader)?.untracked_dirs;
            assert_eq!(dirs.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_one_untracked_directory() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            // Do not add...

            // List files
            let dirs = stager.status(&entry_reader)?.untracked_dirs;

            // There is one directory
            assert_eq!(dirs.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_dir_recursive() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let stager = Stager::new(&repo)?;
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&repo, &commit)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let annotations_dir = PathBuf::from("annotations");
            let full_annotations_dir = repo_path.join(&annotations_dir);

            // Add the directory which has the structure
            // annotations/
            //   README.md
            //   train/
            //     bounding_box.csv
            //     annotations.txt
            //     two_shot.txt
            //     one_shot.csv
            //   test/
            //     annotations.txt
            let ignore = oxenignore::create(&repo);
            stager.add(&full_annotations_dir, &entry_reader, &ignore)?;

            // List dirs
            let status = stager.status(&entry_reader)?;
            status.print_stdout();
            let dirs = status.added_dirs;

            // There is one directory
            assert_eq!(dirs.len(), 1);

            // With recursive files
            let added_dir = dirs.get(&annotations_dir).unwrap();
            assert_eq!(added_dir.num_files_staged, 6);

            Ok(())
        })
    }

    #[test]
    fn test_stager_modify_file_recursive() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let stager = Stager::new(&repo)?;
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&repo, &commit)?;

            let repo_path = &stager.repository.path;
            let one_shot_file = repo_path
                .join("annotations")
                .join("train")
                .join("one_shot.csv");

            // Modify the committed file
            let one_shot_file = test::modify_txt_file(one_shot_file, "new content coming in hot")?;

            // List modified
            let status = stager.status(&entry_reader)?;
            status.print_stdout();
            let files = status.modified_files;

            // There is one modified file
            assert_eq!(files.len(), 1);

            // And it is
            let relative_path = util::fs::path_relative_to_dir(one_shot_file, repo_path)?;
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_remove_file_top_level() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let stager = Stager::new(&repo)?;
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&repo, &commit)?;

            let repo_path = &stager.repository.path;
            let file_to_rm = repo_path.join("labels.txt");

            let status = stager.status(&entry_reader)?;
            status.print_stdout();

            // Remove a committed file
            util::fs::remove_file(&file_to_rm)?;

            // List removed
            let status = stager.status(&entry_reader)?;
            status.print_stdout();
            let files = status.removed_files;

            // There is one removed file, and nothing else
            assert_eq!(files.len(), 1);
            assert_eq!(status.added_dirs.len(), 0);
            assert_eq!(status.added_files.len(), 0);
            assert_eq!(status.untracked_dirs.len(), 0);
            assert_eq!(status.untracked_files.len(), 0);
            assert_eq!(status.modified_files.len(), 0);

            // And it is
            let relative_path = util::fs::path_relative_to_dir(&file_to_rm, repo_path)?;
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_remove_file_recursive() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let stager = Stager::new(&repo)?;
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&repo, &commit)?;

            let repo_path = &stager.repository.path;
            let one_shot_file = repo_path
                .join("annotations")
                .join("train")
                .join("one_shot.csv");

            // Remove a committed file
            util::fs::remove_file(&one_shot_file)?;

            // List removed
            let status = stager.status(&entry_reader)?;
            status.print_stdout();
            let files = status.removed_files;

            // There is one removed file
            assert_eq!(files.len(), 1);

            // And it is
            let relative_path = util::fs::path_relative_to_dir(&one_shot_file, repo_path)?;
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_directories_after_add() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;

            // Create 2 sub directories, one with  Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let train_dir = repo_path.join("train");
            std::fs::create_dir_all(&train_dir)?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/cat_1.jpg"))?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/dog_1.jpg"))?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/cat_2.jpg"))?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/dog_2.jpg"))?;

            let test_dir = repo_path.join("test");
            std::fs::create_dir_all(&test_dir)?;
            let _ = test::add_img_file_to_dir(&test_dir, Path::new("data/test/images/cat_3.jpg"))?;
            let _ = test::add_img_file_to_dir(&test_dir, Path::new("data/test/images/dog_3.jpg"))?;

            let valid_dir = repo_path.join("valid");
            std::fs::create_dir_all(&valid_dir)?;
            let _ = test::add_img_file_to_dir(&valid_dir, Path::new("data/test/images/dog_4.jpg"))?;

            let base_file_1 = test::add_txt_file_to_dir(repo_path, "Hello 1")?;
            let _base_file_2 = test::add_txt_file_to_dir(repo_path, "Hello 2")?;
            let _base_file_3 = test::add_txt_file_to_dir(repo_path, "Hello 3")?;

            // At first there should be 3 untracked
            let untracked_dirs = stager.status(&entry_reader)?.untracked_dirs;
            assert_eq!(untracked_dirs.len(), 3);

            // Add the directory
            stager.add_dir(&train_dir, &entry_reader)?;
            // Add one file
            let _ = stager.add_file(&base_file_1, &entry_reader)?;

            // List the files
            let added_files = stager.status(&entry_reader)?.added_files;
            let added_dirs = stager.status(&entry_reader)?.added_dirs;
            let untracked_files = stager.list_untracked_files(&entry_reader)?;
            let untracked_dirs = stager.status(&entry_reader)?.untracked_dirs;

            // There is 5 added file and 1 added dir
            assert_eq!(added_files.len(), 5);
            assert_eq!(added_dirs.len(), 1);

            // There are 2 untracked files at the top level
            assert_eq!(untracked_files.len(), 2);
            // There are 2 untracked dirs at the top level
            assert_eq!(untracked_dirs.len(), 2);

            Ok(())
        })
    }
}
