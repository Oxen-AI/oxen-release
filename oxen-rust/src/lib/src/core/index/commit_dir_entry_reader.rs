//!

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
/// We could index files by path here for qui
pub struct CommitDirEntryReader {
    dir: PathBuf,
    dir_object: TreeObject,
    commit_id: String,
    object_reader: Arc<ObjectDBReader>,
}

// TODONOW: Is it worth indexing the vnodes up front?
// there will only ever be 700 or whatever of them

// This was formerly for commit dir entry db, now it's just the dir hashes db
impl CommitDirEntryReader {
    pub fn dir_hash_db(base_path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(base_path, commit_id).join(constants::DIR_HASHES_DIR)
    }

    pub fn dir_hashes_db_exists(base_path: &Path, commit_id: &str) -> bool {
        let db_path = CommitDirEntryReader::dir_hash_db(base_path, commit_id);
        db_path.join("CURRENT").exists()
    }

    // pub fn dir_db(base_path: &Path) -> PathBuf {
    //     base_path.join(constants::OXEN_HIDDEN_DIR).join(OBJECTS_DIR).join(OBJECT_DIRS_DIR)
    // }

    // pub fn vnodes_db(base_path: &Path) -> PathBuf {
    //     base_path.join(constants::OXEN_HIDDEN_DIR).join(constants::OBJECTS_DIR).join(constants::OBJECT_VNODES_DIR)
    // }

    // pub fn files_db(base_path: &Path) -> PathBuf {
    //     base_path.join(constants::OXEN_HIDDEN_DIR).join(constants::OBJECTS_DIR).join(constants::OBJECT_FILES_DIR)
    // }

    // Probably don't need to do db_exists stuff - we're in 1 db now...
    // TODONOW: do we want to load the children of a dir into a hashset
    // on load? how much is this reused - O(n) loading + O(1) lookup vs
    // O(1) loading + O(log(n)) lookup...

    // Maybe offer both options
    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
        dir: &Path,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<CommitDirEntryReader, OxenError> {
        CommitDirEntryReader::new_from_path(&repository.path, commit_id, dir, object_reader)
    }

    // TODONOW: Error handling for when we can't find entries.
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
            // log::debug!("dir hashes db not exists");
            if let Err(err) = std::fs::create_dir_all(&db_path) {
                log::error!("CommitDirEntryReader could not create dir {db_path:?}\nErr: {err:?}");
            }
            // log::debug!("about to open");
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
            // log::debug!("successfully opened");
        }

        // let start = std::time::Instant::now();
        // let dirs_db_path = CommitDirEntryReader::dir_db(base_path);
        // if !dirs_db_path.join("CURRENT").exists() {
        //     // log::debug!("dirs db not exists");
        //     if let Err(err) = std::fs::create_dir_all(&dirs_db_path) {
        //         log::error!("CommitDirEntryReader could not create dir {dirs_db_path:?}\nErr: {err:?}");
        //     }

        //     let _db: DBWithThreadMode<MultiThreaded> =
        //         DBWithThreadMode::open(&opts, dunce::simplified(&dirs_db_path))?;
        // }

        // let vnodes_db_path = CommitDirEntryReader::vnodes_db(base_path);
        // if !vnodes_db_path.join("CURRENT").exists() {
        //     log::debug!("vnodes db not exists");
        //     if let Err(err) = std::fs::create_dir_all(&vnodes_db_path) {
        //         log::error!("CommitDirEntryReader could not create dir {vnodes_db_path:?}\nErr: {err:?}");
        //     }
        //     log::debug!("about to open");
        //     // open it then lose scope to close it
        //     let _db: DBWithThreadMode<MultiThreaded> =
        //         DBWithThreadMode::open(&opts, dunce::simplified(&vnodes_db_path))?;
        // }

        // let files_db_path = CommitDirEntryReader::files_db(base_path);
        // if !files_db_path.join("CURRENT").exists() {
        //     log::debug!("files db not exists");
        //     if let Err(err) = std::fs::create_dir_all(&files_db_path) {
        //         log::error!("CommitDirEntryReader could not create dir {files_db_path:?}\nErr: {err:?}");
        //     }
        //     // open it then lose scope to close it
        //     let _db: DBWithThreadMode<MultiThreaded> =
        //         DBWithThreadMode::open(&opts, dunce::simplified(&files_db_path))?;
        // }

        // let elapsed = start.elapsed();
        // log::debug!("open-created thesse other dbs in {:?}", elapsed.as_millis());

        let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, db_path, false)?;
        // let time = std::time::Instant::now();
        // let dirs_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, CommitDirEntryReader::dir_db(base_path), false)?;
        // let vnodes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, CommitDirEntryReader::vnodes_db(base_path), false)?;
        // let files_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(&opts, CommitDirEntryReader::files_db(base_path), false)?;
        // let elapsed = time.elapsed();
        // log::debug!("opened all objects dbs in {:?}", elapsed.as_millis());

        //
        // log::debug!("Hey looking for dir {:?}", dir);
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
        // TODONOW: assuming we only care about `File` type entries here, not schemas.
        log::debug!("num_entries in dir {:?}", self.dir);
        let mut count = 0;
        for vnode_child in self.dir_object.children() {
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo?
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

        // TODONOW: maybe make binary search not return a result?
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
        // TODONOW error handling
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
        log::debug!(
            "checking in dir {:?} for path {:?}",
            self.dir,
            path.as_ref()
        );
        let full_path = self.dir.join(path.as_ref());
        log::debug!(
            "got_entry the full_path {:?}, path {:?}",
            full_path,
            path.as_ref()
        );
        let path_hash_prefix = util::hasher::hash_path(full_path)[0..2].to_string();
        // log::debug!("we are looking for a vnode with path hash prefix {:?}", path_hash_prefix);

        // for vnode_child in self.dir_object.children() {
        //     // log::debug!("vnode child {:?}", vnode_child);
        //     // Get the parent vnode entry
        //     let vnode: TreeObject = path_db::get_entry(&self.vnodes_db, vnode_child.hash())?.unwrap();
        //     // log::debug!("vnode yoyo {:?}", vnode);
        // }

        // Binary search for the appropriate vnode
        let vnode_child = self
            .dir_object
            .binary_search_on_path(&PathBuf::from(path_hash_prefix.clone()))?;

        // log::debug!("here are all children of our dir object {:?}", self.dir_object.children());
        // log::debug!("and our dir object specifically is {:?}", self.dir_object);
        // log::debug!("here is our dir object {:?}", self.dir_object);

        log::debug!("looking for path_hash_prefix {:?}", path_hash_prefix);
        log::debug!(
            "path_hash_prefix of just the short path is {:?}",
            util::hasher::hash_path(path.as_ref())[0..2].to_string()
        );
        log::debug!("children of dir object {:?}", self.dir_object.children());

        if vnode_child.is_none() {
            log::debug!("could not find vnode child for path {:?}", path.as_ref());
            return Ok(None);
        }

        let vnode = vnode_child.unwrap();

        log::debug!("here is our vnode {:?}", vnode);

        // Get parent vnode
        let vnode = self.object_reader.get_vnode(vnode.hash())?.unwrap();

        // Now binary search within the vnode for the appropriate file
        let full_path = self.dir.join(path.as_ref());

        // log::debug!("searching in path from root {:?}", full_path);

        log::debug!(
            "binary searching for path {:?} on these children {:?}",
            full_path,
            vnode.children()
        );

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
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo?
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
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo?
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
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo?
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
        // Don't have a skip here....
        let mut entries: Vec<CommitEntry> = Vec::new();
        let mut entry_i = 0;

        let start_page = if page == 0 { 0 } else { page - 1 };
        let start_idx = start_page * page_size;

        for vnode_child in self.dir_object.children() {
            // Get vnode entry - TODONOW: method here to get the object given a ChildObject and repo?
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { path, .. } = entry {
                    if entries.len() >= page_size {
                        break;
                    }

                    log::debug!(
                        "considering entry {:?} with entry_i {:?} and start_idx {:?}",
                        entry,
                        entry_i,
                        start_idx
                    );

                    if entry_i >= start_idx {
                        // Get file object by hash
                        let file_object = self.object_reader.get_file(entry.hash())?.unwrap();
                        // Get commit entry from file object
                        let entry = file_object.to_commit_entry(path, &self.commit_id);
                        log::debug!("adding entry to results");
                        entries.push(entry);
                    }
                    entry_i += 1;
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
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { path, .. } = entry {
                    if entries.len() >= page_size {
                        break;
                    }

                    log::debug!(
                        "considering entry {:?} with entry_i {:?} and start_idx {:?}",
                        entry,
                        entry_i,
                        start_idx
                    );

                    if entry_i >= start_idx {
                        // Get file object by hash
                        let file_object = self.object_reader.get_file(entry.hash())?.unwrap();
                        // Get commit entry from file object
                        let entry = file_object.to_commit_entry(path, &self.commit_id);
                        log::debug!("adding entry to results");
                        entries.push(entry);
                    }
                    entry_i += 1;
                }
            }
        }
        Ok(entries)
    }
}
