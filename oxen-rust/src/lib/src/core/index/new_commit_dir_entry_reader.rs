//!

use crate::constants::{FILES_DIR, HISTORY_DIR, self};
use crate::core::db;
use crate::core::db::path_db;
use crate::core::db::tree_db::{TreeObject, TreeObjectChild};
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository};
use crate::util;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;

use super::CommitEntryWriter;

/// # CommitDirEntryReader
/// We could index files by path here for qui
pub struct NewCommitDirEntryReader {
    dir: PathBuf,
    dir_hash: String,
    dir_object: TreeObject,
    base_path: PathBuf,
    vnodes_db: DBWithThreadMode<MultiThreaded>,
    files_db: DBWithThreadMode<MultiThreaded>,
    commit_id: String,
}

// TODONOW: Is it worth indexing the vnodes up front? 
// there will only ever be 700 or whatever of them

// This was formerly for commit dir entry db, now it's just the dir hashes db
impl NewCommitDirEntryReader {
    pub fn dir_hash_db(base_path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(base_path, commit_id)
            .join(constants::DIR_HASHES_DIR)
    }

    pub fn dir_db(base_path: &Path) -> PathBuf {
        base_path.join(constants::OXEN_HIDDEN_DIR).join(constants::DIR_HASHES_DIR)
    }

    pub fn vnodes_db(base_path: &Path) -> PathBuf {
        base_path.join(constants::OXEN_HIDDEN_DIR).join(constants::OBJECTS_DIR).join(constants::OBJECT_VNODES_DIR)
    }

    pub fn files_db(base_path: &Path) -> PathBuf {
        base_path.join(constants::OXEN_HIDDEN_DIR).join(constants::OBJECTS_DIR).join(constants::OBJECT_FILES_DIR)
    }

    // Probably don't need to do db_exists stuff - we're in 1 db now...
    // TODONOW: do we want to load the children of a dir into a hashset 
    // on load? how much is this reused - O(n) loading + O(1) lookup vs
    // O(1) loading + O(log(n)) lookup...

    // Maybe offer both options
    pub fn new(
        repository: &LocalRepository, 
        commit_id: &str, 
        dir: &Path
    ) -> Result<NewCommitDirEntryReader, OxenError> {
        NewCommitDirEntryReader::new_from_path(&repository.path, commit_id, dir)
    }

    // TODONOW: Error handling for when we can't find entries.
    pub fn new_from_path(
        base_path: &Path, 
        commit_id: &str, 
        dir: &Path
    ) -> Result<NewCommitDirEntryReader, OxenError> {
        let db_path = NewCommitDirEntryReader::dir_hash_db(base_path, commit_id);
        log::debug!(
            "Creating new commit dir entry reader for path: {:?}",
            db_path
        );
        let opts = db::opts::default();
        let dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, db_path, false)?;
        let dirs_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, NewCommitDirEntryReader::dir_db(base_path), false)?;
        // Get hash for dir 
        let dir_hash: String = path_db::get_entry(&dir_hashes_db, dir)?.unwrap();
        // Get object for dir 
        let dir_object: TreeObject = path_db::get_entry(&dirs_db, dir_hash.clone())?.unwrap();

        let vnodes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, NewCommitDirEntryReader::vnodes_db(base_path), false)?;
        let files_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, NewCommitDirEntryReader::files_db(base_path), false)?;


        Ok(NewCommitDirEntryReader {
            dir_hash: dir_hash,
            dir: dir.to_path_buf(),
            base_path: base_path.to_path_buf(),
            dir_object: dir_object,
            vnodes_db: vnodes_db,
            files_db: files_db,
            commit_id: commit_id.to_string(),
        })
    }

    pub fn num_entries(&self) -> Result<usize, OxenError> {
        // TODONOW: assuming we only care about `File` type entries here, not schemas. 
        let mut count = 0;
        for vnode_child in self.dir_object.children() {
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo? 
            let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                match entry {
                    TreeObjectChild::File {..} => count += 1,
                    _ => (),
                }
            }
        }
        Ok(count)
    }

    pub fn has_file<P: AsRef<Path>>(&self, path: P) -> bool {
        let path_hash_prefix = util::hasher::hash_path(path.as_ref().clone())[0..2].to_string();

        // TODONOW: maybe make binary search not return a result?
        // Binary search for the appropriate vnode 
        let vnode_child = self.dir_object.binary_search_on_path(&PathBuf::from(path_hash_prefix)).unwrap();
        
        if vnode_child.is_none() {
            return false;
        }

        let vnode_child = vnode_child.unwrap();
        // Get the vnode object proper
        // TODONOW error handling 
        let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash()).unwrap().unwrap();

        // Now binary search within the vnode for the appropriate file 
        let file = vnode.binary_search_on_path(&path.as_ref().to_path_buf()).unwrap();

        if file.is_none() {
            return false;
        }

        match file.unwrap() {
            TreeObjectChild::File {..} => true,
            _ => false,
        }
        
    }

    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<CommitEntry>, OxenError> {
        let path_hash_prefix = util::hasher::hash_path(path.as_ref().clone())[0..2].to_string();

        // Binary search for the appropriate vnode 
        let vnode = self.dir_object.binary_search_on_path(&PathBuf::from(path_hash_prefix))?;
        
        if vnode.is_none() {
            return Ok(None);
        }

        let vnode = vnode.unwrap();

        // Now binary search within the vnode for the appropriate file 
        let file = self.dir_object.binary_search_on_path(&path.as_ref().to_path_buf())?;

        if file.is_none() {
            return Ok(None);
        }

        let file = file.unwrap();

        match file.clone() {
            TreeObjectChild::File {hash, ..} => {
                // Get file object by hash 
                let file_object: TreeObject = path_db::get_entry(&self.files_db, hash)?.unwrap();
                // Get commit entry from file object
                let entry = file_object.to_commit_entry(file.path(), &self.commit_id); 
                Ok(Some(entry))
            },
            _ => Ok(None),
        }
    }
    
    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut files = Vec::new();
        for vnode_child in self.dir_object.children() {
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo? 
            let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                match entry {
                    TreeObjectChild::File {path, ..} => files.push(path.to_owned()),
                    _ => (),
                }
            }
        }
        Ok(files)
    }

    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut entries = Vec::new();
        for vnode_child in self.dir_object.children() {
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo? 
            let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                match entry {
                    TreeObjectChild::File {path, ..} => {
                        // Get file object by hash 
                        let file_object: TreeObject = path_db::get_entry(&self.files_db, entry.hash())?.unwrap();
                        // Get commit entry from file object
                        let entry = file_object.to_commit_entry(path, &self.commit_id); 
                        entries.push(entry);
                    },
                    _ => (),
                }
            }
        }
        Ok(entries)
    }

    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        let mut entries = HashSet::new();
        for vnode_child in self.dir_object.children() {
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo? 
            let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                match entry {
                    TreeObjectChild::File {path, ..} => {
                        // Get file object by hash 
                        let file_object: TreeObject = path_db::get_entry(&self.files_db, entry.hash())?.unwrap();
                        // Get commit entry from file object
                        let entry = file_object.to_commit_entry(path, &self.commit_id); 
                        entries.insert(entry);
                    },
                    _ => (),
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
        // Don't have a skip here....
        let mut entries: Vec<CommitEntry> = Vec::new();
        let mut entry_i = 0;

        let start_page = if page == 0 { 0 } else { page - 1 };
        let start_idx = start_page * page_size;

        for vnode_child in self.dir_object.children() {
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo? 
            let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                match entry {
                    TreeObjectChild::File {path, ..} => {
                        if entries.len() >= page_size {
                            break;
                        }
                        if entry_i >= start_idx {
                            // Get file object by hash 
                            let file_object: TreeObject = path_db::get_entry(&self.files_db, entry.hash())?.unwrap();
                            // Get commit entry from file object
                            let entry = file_object.to_commit_entry(path, &self.commit_id); 
                            entries.push(entry);
                        }
                        entry_i += 1;
                    },
                    _ => (),
                }
            }
        }

        Ok(entries)
        

    }

    pub fn list_entry_page_with_offset(
        &self,
        page: usize,
        page_size: usize,
        offset: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let mut entries: Vec<CommitEntry> = Vec::new();
        let start_page = if page == 0 { 0 } else { page - 1 };
        let mut start_idx = start_page * page_size;
        let mut entry_i = 0;
        log::debug!("list_entry_page_with_offset(1) page: {page}, page_size: {page_size}, offset: {offset} start_idx: {start_idx} start_page: {start_page}");

        if start_idx >= offset {
            start_idx -= offset;
        }
        log::debug!("list_entry_page_with_offset(2) page: {page}, page_size: {page_size}, offset: {offset} start_idx: {start_idx} start_page: {start_page}");

        for vnode_child in self.dir_object.children() {
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo? 
            let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                match entry {
                    TreeObjectChild::File {path, ..} => {

                        if entries.len() >= page_size {
                            break;
                        }

                        if entry_i >= start_idx {
                            // Get file object by hash 
                            let file_object: TreeObject = path_db::get_entry(&self.files_db, entry.hash())?.unwrap();
                            // Get commit entry from file object
                            let entry = file_object.to_commit_entry(path, &self.commit_id); 
                            entries.push(entry);
                        }
                    },
                    _ => (),
                }
            }
        }
        Ok(entries)
    }


}
