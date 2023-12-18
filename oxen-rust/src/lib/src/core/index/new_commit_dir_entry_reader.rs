//!

use crate::constants::{FILES_DIR, HISTORY_DIR, OBJECT_DIRS_DIR, self, OBJECTS_DIR};
use crate::core::db;
use crate::core::db::path_db;
use crate::core::db::tree_db::{TreeObject, TreeObjectChild};
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository};
use crate::{util, api};

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


    pub fn dir_hashes_db_exists(base_path: &Path, commit_id: &str) -> bool {
        let db_path = NewCommitDirEntryReader::dir_hash_db(base_path, commit_id);
        db_path.join("CURRENT").exists()
    }

    pub fn dir_db(base_path: &Path) -> PathBuf {
        base_path.join(constants::OXEN_HIDDEN_DIR).join(OBJECTS_DIR).join(OBJECT_DIRS_DIR)
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
            base_path.join(dir)
        );
        // TODONOW: maybe not with create_if_missing here - this should probably already be getting created for this commit at commit time, but
        // let's see what happens 

        // Create the dir hashes db if it doesn't exist? 

        let opts = db::opts::default();
        if !NewCommitDirEntryReader::dir_hashes_db_exists(base_path, commit_id) {
            // log::debug!("dir hashes db not exists");
            if let Err(err) = std::fs::create_dir_all(&db_path) {
                // log::error!("CommitDirEntryReader could not create dir {db_path:?}\nErr: {err:?}");
            }
            // log::debug!("about to open");
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
            // log::debug!("successfully opened");
        } else {
            // log::debug!("dir hashes db exists, allegedly");
        }

        // DO the same with the dirs db  - TODONOW, this is not great 
        let dirs_db_path = NewCommitDirEntryReader::dir_db(base_path);
        if !dirs_db_path.join("CURRENT").exists() {
            // log::debug!("dirs db not exists");
            if let Err(err) = std::fs::create_dir_all(&dirs_db_path) {
                log::error!("CommitDirEntryReader could not create dir {dirs_db_path:?}\nErr: {err:?}");
            }
            // log::debug!("about to open");
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&dirs_db_path))?;
            // log::debug!("successfully opened");
        } else {
            // log::debug!("dirs db exists, allegedly");
        }


        // TODONOW REALLY SERIOUSLY CONSOLIDATE THIS GUYS 
        // do the same with vnodes and files dbs. 

        let vnodes_db_path = NewCommitDirEntryReader::vnodes_db(base_path);
        if !vnodes_db_path.join("CURRENT").exists() {
            log::debug!("vnodes db not exists");
            if let Err(err) = std::fs::create_dir_all(&vnodes_db_path) {
                log::error!("CommitDirEntryReader could not create dir {vnodes_db_path:?}\nErr: {err:?}");
            }
            log::debug!("about to open");
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&vnodes_db_path))?;
            // log::debug!("successfully opened");
        } else {
            // log::debug!("vnodes db exists, allegedly");
        }

        let files_db_path = NewCommitDirEntryReader::files_db(base_path);
        if !files_db_path.join("CURRENT").exists() {
            log::debug!("files db not exists");
            if let Err(err) = std::fs::create_dir_all(&files_db_path) {
                log::error!("CommitDirEntryReader could not create dir {files_db_path:?}\nErr: {err:?}");
            }
            log::debug!("about to open");
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&files_db_path))?;
            // log::debug!("successfully opened");
        } else {
            // log::debug!("files db exists, allegedly");
        }


        let dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, db_path, false)?;
        let dirs_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, NewCommitDirEntryReader::dir_db(base_path), false)?;
        let vnodes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, NewCommitDirEntryReader::vnodes_db(base_path), false)?;
        let files_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, NewCommitDirEntryReader::files_db(base_path), false)?;


        // Same 
        // TODONOW: buckle down functionality for opening a direntryreader when one doesn't exist. 
        // slightly undefined, but seems like we could create a dummy node for the dir and save that off
        // probably don't need to put a dummy entry in the dir hashes db 

        // Get hash for dir 

        // // Print all entries in the dir_hashes_db - each is a string 
        // let iter = dir_hashes_db.iterator(IteratorMode::Start);
        // for item in iter {
        //     match item {
        //         Ok((key, value)) => {
        //             match str::from_utf8(&key) {
        //                 Ok(key_str) => {
        //                     match String::from_utf8(value.to_vec()) {
        //                         Ok(value_str) => {
        //                             // return full path
        //                             log::debug!("hey dir_hashes_db key: {:?}, value: {}", key_str, value_str);
        //                         }
        //                         Err(_) => {
        //                             log::error!("Could not decode value as UTF-8");
        //                         }
        //                     }
        //                 }
        //                 Err(_) => {
        //                     log::error!("Could not decode key as UTF-8");
        //                 }
        //             }
        //         }
        //         _ => {
        //             return Err(OxenError::basic_str(
        //                 "Could not read iterate over db values",
        //             ));
        //         }
        //     }
        // }
        
// 
        // log::debug!("Hey looking for dir {:?}", dir);
        let dir_hash: Option<String> = path_db::get_entry(&dir_hashes_db, dir)?;

        let dir_object: TreeObject = match dir_hash {
            Some(dir_hash) => {
                // log::debug!("Found dir hash {:?}", dir_hash);
                // log::debug!("so here's the status of the dirs db");
                // NewCommitDirEntryReader::print_all_entries_in_dirs_db(base_path)?;
                path_db::get_entry(&dirs_db, dir_hash)?.unwrap()
            },
            None => {
                log::debug!("Did not find dir hash");
                // Creating dummy dir object 
                TreeObject::Dir {
                    children: Vec::new(),
                    hash: "".to_string(),
                }
            }
        };

        // get commit by id 

        log::debug!("got root hash {:?} at path {:?} for commit id {:?}", dir_object.hash().clone(), dir, commit_id);


        Ok(NewCommitDirEntryReader {
            dir_hash: dir_object.hash().to_string(),
            dir: dir.to_path_buf(),
            base_path: base_path.to_path_buf(),
            dir_object: dir_object,
            vnodes_db: vnodes_db,
            files_db: files_db,
            commit_id: commit_id.to_string(),
        })
    }

    pub fn num_entries(&self) -> usize {
        // TODONOW: assuming we only care about `File` type entries here, not schemas. 
        let mut count = 0;
        for vnode_child in self.dir_object.children() {
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo? 
            let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash()).unwrap().unwrap();
            for entry in vnode.children() {
                match entry {
                    TreeObjectChild::File {..} => count += 1,
                    _ => (),
                }
            }
        }
        count
    }


    pub fn print_all_entries_in_dirs_db(
        base_path: &Path,
    ) -> Result<(), OxenError> {

        let opts = db::opts::default();
        log::debug!("looking in dirs db path {:?}", NewCommitDirEntryReader::dir_db(base_path));
        let dirs_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, NewCommitDirEntryReader::dir_db(&base_path), false)?;
        let iter = dirs_db.iterator(IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, value)) => {
                    match str::from_utf8(&key) {
                        Ok(key_str) => {
                            match String::from_utf8(value.to_vec()) {
                                Ok(value_str) => {
                                    // return full path
                                    log::debug!("PRINTING DIR_DB ENTRY key: {:?}, value: {}: base_path: {:?}", key_str, value_str, base_path);
                                }
                                Err(_) => {
                                    log::error!("Could not decode value as UTF-8");
                                }
                            }
                        }
                        Err(_) => {
                            log::error!("Could not decode key as UTF-8");
                        }
                    }
                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Could not read iterate over db values",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn has_file<P: AsRef<Path>>(&self, path: P) -> bool {
        let full_path = self.dir.join(path.as_ref());
        let path_hash_prefix = util::hasher::hash_path(full_path)[0..2].to_string();

        log::debug!("looking for this path hash prefix {:?}", path_hash_prefix);

        // TODONOW: maybe make binary search not return a result?
        // Binary search for the appropriate vnode 
        let vnode_child = self.dir_object.binary_search_on_path(&PathBuf::from(path_hash_prefix)).unwrap();
        

        // log::debug!("got this vnode ")

        if vnode_child.is_none() {
            return false;
        }

        let vnode_child = vnode_child.unwrap();
        // Get the vnode object proper
        // TODONOW error handling 
        let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash()).unwrap().unwrap();

        // Now binary search within the vnode for the appropriate file 
        let full_path = self.dir.join(path.as_ref());
        log::debug!("checking has_file at path {:?}", full_path);
        let file = vnode.binary_search_on_path(&full_path.to_path_buf()).unwrap();
        log::debug!("got file {:?}", file);
        if file.is_none() {
            return false;
        }

        match file.unwrap() {
            TreeObjectChild::File {..} => true,
            _ => false,
        }
        
    }

    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<CommitEntry>, OxenError> {
        let full_path = self.dir.join(path.as_ref());
        // log::debug!("got the full_path {:?}, path {:?}", full_path, path.as_ref());
        let path_hash_prefix = util::hasher::hash_path(full_path)[0..2].to_string();
        // log::debug!("we are looking for a vnode with path hash prefix {:?}", path_hash_prefix);


        // for vnode_child in self.dir_object.children() {
        //     // log::debug!("vnode child {:?}", vnode_child);
        //     // Get the parent vnode entry 
        //     let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash())?.unwrap();
        //     // log::debug!("vnode yoyo {:?}", vnode);
        // }

        // Binary search for the appropriate vnode 
        let vnode_child = self.dir_object.binary_search_on_path(&PathBuf::from(path_hash_prefix))?;
        
        // log::debug!("here's the vnode child... {:?}", vnode_child);

        // log::debug!("here are all children of our dir object {:?}", self.dir_object.children());
        // log::debug!("and our dir object specifically is {:?}", self.dir_object);
        // log::debug!("here is our dir object {:?}", self.dir_object);
        
        if vnode_child.is_none() {
            return Ok(None);
        }

        let vnode = vnode_child.unwrap();

        // Get parent vnode 
        let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode.hash())?.unwrap();

        // Now binary search within the vnode for the appropriate file 
        let full_path = self.dir.join(path.as_ref());

        // log::debug!("searching in path from root {:?}", full_path);
        let file = vnode.binary_search_on_path(&full_path)?;

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

                        log::debug!("considering entry {:?} with entry_i {:?} and start_idx {:?}", entry, entry_i, start_idx);

                        if entry_i >= start_idx {
                            // Get file object by hash 
                            let file_object: TreeObject = path_db::get_entry(&self.files_db, entry.hash())?.unwrap();
                            // Get commit entry from file object
                            let entry = file_object.to_commit_entry(path, &self.commit_id); 
                            log::debug!("adding entry to results");
                            entries.push(entry);
                        }
                        entry_i += 1;
                    },
                    _ => {}
                }
                
            }
        }
        Ok(entries)
    }


}
