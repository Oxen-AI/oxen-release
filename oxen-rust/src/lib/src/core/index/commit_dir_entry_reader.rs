//!

use std::time::{Duration, Instant};

use crate::constants::{self};
use crate::core::db;
use crate::core::db::path_db;
use crate::core::db::tree_db::{TreeObject, TreeObjectChild};
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository};
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use super::{CommitEntryWriter, ObjectDBReader};

/// # CommitDirEntryReader
pub struct CommitDirEntryReader {
    dir: PathBuf,
    dir_object: TreeObject,
    commit_id: String,
    object_reader: Arc<ObjectDBReader>,
}
// This was formerly for commit dir entry db, now it's just the dir hashes db
impl CommitDirEntryReader {
    pub fn dir_hash_db(base_path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(base_path, commit_id).join(constants::DIR_HASHES_DIR)
    }

    pub fn dir_hashes_db_exists(base_path: &Path, commit_id: &str) -> bool {
        let db_path = CommitDirEntryReader::dir_hash_db(base_path, commit_id);
        db_path.join("CURRENT").exists()
    }

    // Maybe offer both options
    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
        dir: &Path,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<CommitDirEntryReader, OxenError> {
        CommitDirEntryReader::new_from_path(&repository.path, commit_id, dir, object_reader)
    }

    pub fn new_from_path(
        base_path: &Path,
        commit_id: &str,
        dir: &Path,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<CommitDirEntryReader, OxenError> {
        let db_path = CommitDirEntryReader::dir_hash_db(base_path, commit_id);
        log::debug!(
            "Creating new commit dir entry reader for path: {:?}",
            base_path.join(dir)
        );

        let opts = db::opts::default();
        if !CommitDirEntryReader::dir_hashes_db_exists(base_path, commit_id) {
            // Get the current time
            let start_time = Instant::now();

            if let Err(err) = std::fs::create_dir_all(&db_path) {
                log::error!("CommitDirEntryReader could not create dir {db_path:?}\nErr: {err:?}");
            }

            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
            let end_time = Instant::now();
            let elapsed = end_time.duration_since(start_time);
            log::debug!(
                "CommitDirEntryReader took {:?} to create dir hashes db",
                elapsed
            );
        }

        let start_time = Instant::now();
        let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, db_path, false)?;
        let elapsed = start_time.elapsed();
        log::debug!(
            "CommitDirEntryReader took {:?} to open dir hashes db",
            elapsed
        );

        let dir_hash: Option<String> = path_db::get_entry(&dir_hashes_db, dir)?;

        let dir_object: TreeObject = match dir_hash {
            Some(dir_hash) => object_reader.get_dir(&dir_hash)?.unwrap(),
            None => {
                // Creating dummy dir object
                TreeObject::Dir {
                    children: Vec::new(),
                    hash: "".to_string(),
                }
            }
        };

        Ok(CommitDirEntryReader {
            dir: dir.to_path_buf(),
            dir_object,
            commit_id: commit_id.to_string(),
            object_reader,
        })
    }

    pub fn num_entries(&self) -> usize {
        log::debug!("num_entries in dir {:?}", self.dir);
        let mut count = 0;
        for vnode_child in self.dir_object.children() {
            let vnode = self
                .object_reader
                .get_vnode(vnode_child.hash())
                .unwrap()
                .unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { .. } = entry {
                    count += 1
                }
            }
        }
        count
    }

    pub fn has_file<P: AsRef<Path>>(&self, path: P) -> bool {
        let full_path = self.dir.join(path.as_ref());
        let path_hash_prefix = util::hasher::hash_path(full_path)[0..2].to_string();

        log::debug!("looking for this path hash prefix {:?}", path_hash_prefix);

        // Binary search for the appropriate vnode
        let vnode_child = self
            .dir_object
            .binary_search_on_path(&PathBuf::from(path_hash_prefix))
            .unwrap();

        if vnode_child.is_none() {
            return false;
        }

        let vnode_child = vnode_child.unwrap();
        // Get the vnode object proper
        let vnode = self
            .object_reader
            .get_vnode(vnode_child.hash())
            .unwrap()
            .unwrap();

        // Now binary search within the vnode for the appropriate file
        let full_path = self.dir.join(path.as_ref());
        let file = vnode
            .binary_search_on_path(&full_path.to_path_buf())
            .unwrap();
        if file.is_none() {
            return false;
        }

        matches!(file.unwrap(), TreeObjectChild::File { .. })
    }

    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<CommitEntry>, OxenError> {
        let full_path = self.dir.join(path.as_ref());
        let path_hash_prefix = util::hasher::hash_path(full_path)[0..2].to_string();

        // Binary search for the appropriate vnode
        let vnode_child = self
            .dir_object
            .binary_search_on_path(&PathBuf::from(path_hash_prefix.clone()))?;

        if vnode_child.is_none() {
            log::debug!("could not find vnode child for path {:?}", path.as_ref());
            return Ok(None);
        }

        let vnode_child = vnode_child.unwrap();

        // Get parent vnode
        let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();

        // Now binary search within the vnode for the appropriate file
        let full_path = self.dir.join(path.as_ref());
        let file = vnode.binary_search_on_path(&full_path)?;

        if file.is_none() {
            log::debug!("could not find file for path {:?}", path.as_ref());
            return Ok(None);
        }

        let file = file.unwrap();

        match file.clone() {
            TreeObjectChild::File { hash, .. } => {
                // Get file object by hash
                let file_object = self.object_reader.get_file(&hash)?.unwrap();
                // Get commit entry from file object
                let entry = file_object.to_commit_entry(file.path(), &self.commit_id);
                Ok(Some(entry))
            }
            _ => {
                log::debug!("wrong type of file node for path {:?}", path.as_ref());
                Ok(None)
            }
        }
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut files = Vec::new();
        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { path, .. } = entry {
                    files.push(path.to_owned())
                }
            }
        }
        Ok(files)
    }

    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut entries = Vec::new();
        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { path, .. } = entry {
                    // Get file object by hash
                    let file_object = self.object_reader.get_file(entry.hash())?.unwrap();
                    // Get commit entry from file object
                    let entry = file_object.to_commit_entry(path, &self.commit_id);
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        let mut entries = HashSet::new();
        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { path, .. } = entry {
                    // Get file object by hash
                    let file_object = self.object_reader.get_file(entry.hash())?.unwrap();
                    // Get commit entry from file object
                    let entry = file_object.to_commit_entry(path, &self.commit_id);
                    entries.insert(entry);
                }
            }
        }
        Ok(entries)
    }

    pub fn list_entry_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        log::debug!("deleteme calling list_entry_page");
        // Don't have a skip here....
        let mut entries: Vec<CommitEntry> = Vec::new();
        let mut entry_i = 0;

        let start_page = if page == 0 { 0 } else { page - 1 };
        let start_idx = start_page * page_size;

        // For every vnode, get the vnode and add its children to a list
        // TODO: possible optimization - these will all be sorted by path coming out,
        // so if this is slow we can treat it as merging n sorted lists instead of concatenating and then sorting

        let mut file_children: Vec<TreeObjectChild> = Vec::new();

        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();

            for entry in vnode.children() {
                if let TreeObjectChild::File { .. } = entry {
                    file_children.push(entry.to_owned());
                }
            }
        }

        // Now sort these all by path
        file_children.sort_by(|a, b| {
            let a_path = a.path();
            let b_path = b.path();
            a_path.cmp(b_path)
        });

        // Apply pagination logic to the file_children list
        for entry in file_children {
            if entries.len() >= page_size {
                break;
            }

            if entry_i >= start_idx {
                // Get file object by hash
                let file_object = self.object_reader.get_file(entry.hash())?.unwrap();
                // Get commit entry from file object
                let entry = file_object.to_commit_entry(entry.path(), &self.commit_id);
                entries.push(entry);
            }
            entry_i += 1;
        }

        Ok(entries)
    }

    pub fn list_entry_page_with_offset(
        &self,
        page: usize,
        page_size: usize,
        offset: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        log::debug!("deleteme calling list_entry_page_with_offset");

        // Apply logic from above here
        let mut entries: Vec<CommitEntry> = Vec::new();
        let start_page = if page == 0 { 0 } else { page - 1 };
        let mut start_idx = start_page * page_size;
        let mut entry_i = 0;

        if start_idx >= offset {
            start_idx -= offset;
        }

        // Get all vnode chidlren
        let mut file_children: Vec<TreeObjectChild> = Vec::new();

        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();

            for entry in vnode.children() {
                if let TreeObjectChild::File { .. } = entry {
                    file_children.push(entry.to_owned());
                }
            }
        }

        // Now sort these all by path
        file_children.sort_by(|a, b| {
            let a_path = a.path();
            let b_path = b.path();
            a_path.cmp(b_path)
        });

        // Apply pagination logic to the file_children list
        for entry in file_children {
            if entries.len() >= page_size {
                break;
            }

            if entry_i >= start_idx {
                // Get file object by hash
                let file_object = self.object_reader.get_file(entry.hash())?.unwrap();
                // Get commit entry from file object
                let entry = file_object.to_commit_entry(entry.path(), &self.commit_id);
                entries.push(entry);
            }
            entry_i += 1;
        }

        Ok(entries)
    }
}
