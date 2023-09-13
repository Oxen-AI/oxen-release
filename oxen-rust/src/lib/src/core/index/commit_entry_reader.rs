use crate::constants::{DIRS_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::index::{CommitDirEntryReader, CommitReader};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry};
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::core::db::path_db;
use crate::model::LocalRepository;

pub struct CommitEntryReader {
    base_path: PathBuf,
    dir_db: DBWithThreadMode<MultiThreaded>,
    pub commit_id: String,
}

impl CommitEntryReader {
    pub fn db_path(base_path: impl AsRef<Path>, commit_id: &str) -> PathBuf {
        util::fs::oxen_hidden_dir(&base_path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(DIRS_DIR)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitEntryReader, OxenError> {
        log::debug!("CommitEntryReader::new() commit_id: {}", commit.id);
        CommitEntryReader::new_from_commit_id(repository, &commit.id)
    }

    pub fn new_from_commit_id(
        repository: &LocalRepository,
        commit_id: &str,
    ) -> Result<CommitEntryReader, OxenError> {
        CommitEntryReader::new_from_path(&repository.path, commit_id)
    }

    pub fn new_from_path(
        base_path: impl AsRef<Path>,
        commit_id: &str,
    ) -> Result<CommitEntryReader, OxenError> {
        let path = Self::db_path(&base_path, commit_id);
        let opts = db::opts::default();
        log::debug!(
            "CommitEntryReader::new_from_commit_id() commit_id: {} path: {:?}",
            commit_id,
            path
        );

        if !path.exists() {
            std::fs::create_dir_all(&path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&path))?;
        }

        Ok(CommitEntryReader {
            base_path: base_path.as_ref().to_owned(),
            dir_db: DBWithThreadMode::open_for_read_only(&opts, &path, true)?,
            commit_id: commit_id.to_owned(),
        })
    }

    /// For opening the entry reader from head, so that it opens and closes the commit db within the constructor
    pub fn new_from_head(repository: &LocalRepository) -> Result<CommitEntryReader, OxenError> {
        let commit_reader = CommitReader::new(repository)?;
        let commit = commit_reader.head_commit()?;
        log::debug!(
            "CommitEntryReader::new_from_head() commit_id: {}",
            commit.id
        );
        CommitEntryReader::new(repository, &commit)
    }

    /// Lists all the directories in the commit
    pub fn list_dirs(&self) -> Result<Vec<PathBuf>, OxenError> {
        let root = PathBuf::from("");
        let mut paths = path_db::list_paths(&self.dir_db, &root)?;
        if !paths.contains(&root) {
            paths.push(root);
        }
        paths.sort();
        Ok(paths)
    }

    /// Lists all the parents of directories that are in the commit dir db
    pub fn list_dir_parents(&self, path: impl AsRef<Path>) -> Result<Vec<PathBuf>, OxenError> {
        // A little hacky, we just filter by starts_with because we aren't representing the parents in the db
        // Shouldn't be a problem unless we have repos with hundreds of thousands of directories?
        let path = path.as_ref();
        let parents = path_db::list_paths(&self.dir_db, Path::new(""))?
            .into_iter()
            .filter(|base| path.starts_with(base) && base != path)
            .collect();
        Ok(parents)
    }

    /// Lists all the child directories that are in the commit dir db
    pub fn list_dir_children(&self, path: impl AsRef<Path>) -> Result<Vec<PathBuf>, OxenError> {
        // A little hacky, we just filter by starts_with because we aren't representing the parents in the db
        // Shouldn't be a problem unless we have repos with hundreds of thousands of directories?
        let path = path.as_ref();
        let parents = path_db::list_paths(&self.dir_db, Path::new(""))?
            .into_iter()
            .filter(|dir| path == Path::new("") || (dir.starts_with(path) && dir != path))
            .collect();
        Ok(parents)
    }

    pub fn has_dir<P: AsRef<Path>>(&self, path: P) -> bool {
        path_db::has_entry(&self.dir_db, path)
    }

    pub fn num_entries(&self) -> Result<usize, OxenError> {
        let mut count = 0;
        for dir in self.list_dirs()? {
            let commit_entry_dir =
                CommitDirEntryReader::new_from_path(&self.base_path, &self.commit_id, &dir)?;
            count += commit_entry_dir.num_entries();
        }
        Ok(count)
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut paths: Vec<PathBuf> = vec![];
        for dir in self.list_dirs()? {
            let commit_dir =
                CommitDirEntryReader::new_from_path(&self.base_path, &self.commit_id, &dir)?;
            let mut files = commit_dir.list_files()?;
            paths.append(&mut files);
        }
        Ok(paths)
    }

    /// List entries in a vector when we need ordering
    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut paths: Vec<CommitEntry> = vec![];
        for dir in self.list_dirs()? {
            let commit_dir =
                CommitDirEntryReader::new_from_path(&self.base_path, &self.commit_id, &dir)?;
            let mut files = commit_dir.list_entries()?;
            paths.append(&mut files);
        }
        Ok(paths)
    }

    /// List entries in a set for quick lookup
    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        let mut paths: HashSet<CommitEntry> = HashSet::new();
        for dir in self.list_dirs()? {
            let commit_dir =
                CommitDirEntryReader::new_from_path(&self.base_path, &self.commit_id, &dir)?;
            let files = commit_dir.list_entries_set()?;
            paths.extend(files);
        }
        Ok(paths)
    }

    pub fn list_entry_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let entries = self.list_entries()?;

        let start_page = if page == 0 { 0 } else { page - 1 };
        let start_idx = start_page * page_size;

        if (start_idx + page_size) < entries.len() {
            let subset: Vec<CommitEntry> = entries[start_idx..(start_idx + page_size)].to_vec();
            Ok(subset)
        } else if (start_idx < entries.len()) && (start_idx + page_size) >= entries.len() {
            let subset: Vec<CommitEntry> = entries[start_idx..entries.len()].to_vec();
            Ok(subset)
        } else {
            Ok(vec![])
        }
    }

    pub fn list_directory(&self, dir: &Path) -> Result<Vec<CommitEntry>, OxenError> {
        log::debug!("CommitEntryReader::list_directory() dir: {:?}", dir);
        let mut entries = vec![];
        // This lists all the committed dirs
        let dirs = self.list_dirs()?;
        for committed_dir in dirs {
            // Have to make sure we are in a subset of the dir (not really a tree structure)
            // log::debug!("CommitEntryReader::list_directory() checking committed_dir: {:?}", committed_dir);
            if committed_dir.starts_with(dir) {
                let entry_reader = CommitDirEntryReader::new_from_path(
                    &self.base_path,
                    &self.commit_id,
                    &committed_dir,
                )?;
                let mut dir_entries = entry_reader.list_entries()?;
                entries.append(&mut dir_entries);
            }
        }
        Ok(entries)
    }

    /// Groups the entries by their parent directory and returns hashmap of dir -> vec<entry>
    pub fn list_entries_per_directory(
        &self,
        dir: &Path,
    ) -> Result<HashMap<PathBuf, Vec<CommitEntry>>, OxenError> {
        log::debug!("CommitEntryReader::list_directory() dir: {:?}", dir);
        let mut dir_entries: HashMap<PathBuf, Vec<CommitEntry>> = HashMap::new();
        // This lists all the committed dirs
        let dirs = self.list_dirs()?;
        for committed_dir in dirs {
            // Have to make sure we are in a subset of the dir (not really a tree structure)
            // log::debug!("CommitEntryReader::list_directory() checking committed_dir: {:?}", committed_dir);
            if committed_dir.starts_with(dir) {
                let entry_reader = CommitDirEntryReader::new_from_path(
                    &self.base_path,
                    &self.commit_id,
                    &committed_dir,
                )?;
                let entries = entry_reader.list_entries()?;
                dir_entries.insert(committed_dir, entries);
            }
        }
        Ok(dir_entries)
    }

    pub fn has_file(&self, path: &Path) -> bool {
        if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
            if let Ok(dir) =
                CommitDirEntryReader::new_from_path(&self.base_path, &self.commit_id, parent)
            {
                return dir.has_file(file_name);
            }
        }
        false
    }

    pub fn get_entry(&self, path: &Path) -> Result<Option<CommitEntry>, OxenError> {
        if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
            let dir =
                CommitDirEntryReader::new_from_path(&self.base_path, &self.commit_id, parent)?;
            // log::debug!("CommitEntryReader::get_entry() get_entry: {:?}", path);
            
            // log::debug!("CommitEntryReader::get_entry() path: {:?} result: {:?}", path, result);
            dir.get_entry(file_name)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::core::index::CommitEntryReader;
    use crate::error::OxenError;

    use crate::test;

    use std::path::Path;

    #[test]
    fn test_commit_dir_reader_check_if_file_exists() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            command::add(&repo, filepath)?;
            let commit = command::commit(&repo, "Adding labels file")?;

            let reader = CommitEntryReader::new(&repo, &commit)?;
            let path = Path::new(filename);
            assert!(reader.has_file(path));

            Ok(())
        })
    }
}
