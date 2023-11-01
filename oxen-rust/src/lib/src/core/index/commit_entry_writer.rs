use crate::api;
use crate::constants::{self, DEFAULT_BRANCH_NAME, HISTORY_DIR, VERSIONS_DIR};
use crate::core::db;
use crate::core::db::tree_db::{TreeChild, TreeNode, TreeDB};
use crate::core::db::{kv_db, path_db, tree_db};
use crate::core::index::oxenignore;
use crate::core::index::{CommitDirEntryWriter, RefWriter, SchemaWriter};
use crate::error::OxenError;
use crate::model::schema::Schema;
use crate::model::{
    Commit, CommitEntry, LocalRepository, StagedData, StagedEntry, StagedEntryStatus,
};
use crate::util;
use crate::util::fs::path_relative_to_dir;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};

use filetime::FileTime;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded, IteratorMode};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::{CommitDirEntryReader, CommitEntryReader, TreeDBReader};

pub struct CommitEntryWriter {
    repository: LocalRepository,
    dir_db: DBWithThreadMode<MultiThreaded>,
    tree_db: DBWithThreadMode<MultiThreaded>,
    commit: Commit,
}

impl CommitEntryWriter {
    pub fn versions_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(VERSIONS_DIR))
    }

    pub fn commit_dir(path: &Path, commit_id: &str) -> PathBuf {
        util::fs::oxen_hidden_dir(path)
            .join(Path::new(HISTORY_DIR))
            .join(commit_id)
    }

    pub fn commit_dir_db(path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(path, commit_id).join(constants::DIRS_DIR)
    }

    pub fn commit_tree_db(path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(path, commit_id).join(constants::TREE_DIR)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitEntryWriter, OxenError> {
        log::debug!("CommitEntryWriter::new() commit_id: {}", commit.id);
        let db_path = CommitEntryWriter::commit_dir_db(&repository.path, &commit.id);
        if !db_path.exists() {
            util::fs::create_dir_all(&db_path)?;
        }

        let tree_db_path = CommitEntryWriter::commit_tree_db(&repository.path, &commit.id);

        let opts = db::opts::default();
        Ok(CommitEntryWriter {
            repository: repository.clone(),
            dir_db: DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?,
            tree_db: DBWithThreadMode::open(&opts, dunce::simplified(&tree_db_path))?,
            commit: commit.to_owned(),
        })
    }

    pub fn copy_parent_dbs(
        &self,
        repo: &LocalRepository,
        parent_ids: &Vec<String>,
    ) -> Result<(), OxenError> {
        if parent_ids.is_empty() {
            // We are creating initial commit, no parent
            let ref_writer = RefWriter::new(repo)?;
            // Set head to default name -> first commit
            ref_writer.create_branch(DEFAULT_BRANCH_NAME, &self.commit.id)?;
            // Make sure head is pointing to that branch
            ref_writer.set_head(DEFAULT_BRANCH_NAME);
        }

        // merge parent dbs
        log::debug!(
            "copy_parent_dbs {} -> '{}'",
            self.commit.id,
            self.commit.message
        );
        for parent_id in parent_ids {
            let parent_commit = api::local::commits::get_by_id(repo, parent_id)?
                .ok_or(OxenError::revision_not_found(parent_id.to_owned().into()))?;
            log::debug!(
                "copy parent {} -> '{}'",
                parent_commit.id,
                parent_commit.message
            );

            let reader = CommitEntryReader::new(repo, &parent_commit)?;
            self.write_entries_from_reader(&reader)?;
        }

        Ok(())
    }

    fn write_entries_from_reader(&self, reader: &CommitEntryReader) -> Result<(), OxenError> {
        let dirs = reader.list_dirs()?;
        for dir in dirs {
            // Write entries per dir
            let writer = CommitDirEntryWriter::new(&self.repository, &self.commit.id, &dir)?;
            path_db::put(&self.dir_db, &dir, &0)?;

            let dir_reader = CommitDirEntryReader::new(&self.repository, &reader.commit_id, &dir)?;
            let entries = dir_reader.list_entries()?;
            log::debug!(
                "write_entries_from_reader got {} entries for dir {:?}",
                entries.len(),
                dir
            );

            // Commit entries data
            entries.par_iter().for_each(|entry| {
                log::debug!("copy entry {:?} -> {:?}", dir, entry.path);

                // Write to db
                match writer.add_commit_entry(entry) {
                    Ok(_) => {}
                    Err(err) => {
                        log::error!("write_entries_from_reader {err:?}");
                    }
                }
            });
        }

        Ok(())
    }

    pub fn set_file_timestamps(
        &self,
        entry: &CommitEntry,
        time: &FileTime,
    ) -> Result<(), OxenError> {
        if let Some(parent) = entry.path.parent() {
            let writer = CommitDirEntryWriter::new(&self.repository, &self.commit.id, parent)?;
            writer.set_file_timestamps(entry, time)
        } else {
            Err(OxenError::file_has_no_parent(&entry.path))
        }
    }

    fn add_staged_entry_to_db(
        &self,
        writer: &CommitDirEntryWriter,
        new_commit: &Commit,
        staged_entry: &StagedEntry, // TODONOW drop
        origin_path: &Path,
        file_path: &Path,
    ) -> Result<(), OxenError> {
        // log::debug!("Commit [{}] add file {:?}", new_commit.id, path);

        // then metadata from the full file path
        let full_path = origin_path.join(file_path);

        // Get last modified time
        let metadata = fs::metadata(&full_path).unwrap();
        let mtime = FileTime::from_last_modification_time(&metadata);

        let metadata = fs::metadata(&full_path)?;

        // Re-hash for issues w/ adding - TODONOW deprecated
        let hash = util::hasher::hash_file_contents(&full_path)?;

        // Create entry object to as json
        let entry = CommitEntry {
            commit_id: new_commit.id.to_owned(),
            path: file_path.to_path_buf(),
            hash: hash.to_owned(),
            num_bytes: metadata.len(),
            last_modified_seconds: mtime.unix_seconds(),
            last_modified_nanoseconds: mtime.nanoseconds(),
        };

        // Write to db & backup
        self.add_commit_entry(origin_path, writer, entry)?;
        Ok(())
    }

    fn add_commit_entry(
        &self,
        origin_path: &Path,
        writer: &CommitDirEntryWriter,
        commit_entry: CommitEntry,
    ) -> Result<(), OxenError> {
        let entry = self.backup_file_to_versions_dir(origin_path, commit_entry)?;
        log::debug!(
            "add_commit_entry with hash {:?} -> {}",
            entry.path,
            entry.hash
        );

        writer.add_commit_entry(&entry)
    }

    fn backup_file_to_versions_dir(
        &self,
        origin_path: &Path, // could be copying from a different base directory
        commit_entry: CommitEntry,
    ) -> Result<CommitEntry, OxenError> {
        let full_path = origin_path.join(&commit_entry.path);

        log::debug!(
            "backup_file_to_versions_dir {:?} -> {:?}",
            commit_entry.path,
            full_path
        );

        // create a copy to our versions directory
        // .oxen/versions/ENTRY_HASH/COMMIT_ID.ext
        // where ENTRY_HASH is something like subdirs: 59/E029D4812AEBF0
        let versions_entry_path = util::fs::version_path(&self.repository, &commit_entry);
        let versions_entry_dir = versions_entry_path.parent().unwrap();

        log::debug!(
            "Copying commit entry for file: {:?} -> {:?}",
            commit_entry.path,
            versions_entry_path
        );

        // Create dir if not exists
        if !versions_entry_dir.exists() {
            std::fs::create_dir_all(versions_entry_dir)?;
        }

        util::fs::copy(full_path, versions_entry_path)?;

        Ok(commit_entry)
    }

    pub fn commit_staged_entries(
        &self,
        commit: &Commit,
        staged_data: &StagedData,
        origin_path: &Path,
    ) -> Result<(), OxenError> {
        self.copy_parent_dbs(&self.repository, &commit.parent_ids.clone())?;
        self.commit_staged_entries_with_prog(commit, staged_data, origin_path)?;
        self.commit_schemas(commit, &staged_data.staged_schemas)
    }

    fn commit_schemas(
        &self,
        commit: &Commit,
        schemas: &HashMap<PathBuf, Schema>,
    ) -> Result<(), OxenError> {
        log::debug!("commit_schemas got {} schemas", schemas.len());

        let schema_writer = SchemaWriter::new(&self.repository, &commit.id)?;
        for (path, schema) in schemas.iter() {
            // Add schema if it does not exist
            if !schema_writer.has_schema(schema) {
                schema_writer.put_schema(schema)?;
            }

            // Map the file to the schema
            schema_writer.put_schema_for_file(path, schema)?;
        }

        Ok(())
    }

    fn group_staged_files_to_dirs(
        &self,
        files: &HashMap<PathBuf, StagedEntry>,
    ) -> HashMap<PathBuf, Vec<(PathBuf, StagedEntry)>> {
        let mut results: HashMap<PathBuf, Vec<(PathBuf, StagedEntry)>> = HashMap::new();

        for (path, entry) in files.iter() {
            if let Some(parent) = path.parent() {
                results
                    .entry(parent.to_path_buf())
                    .or_default()
                    .push((path.clone(), entry.clone()));
            }
        }

        results
    }

    // TODONOW: this is duplicated in stager.rs
    fn should_ignore_path(&self, path: &Path) -> bool {
        let ignore = oxenignore::create(&self.repository);
        let should_ignore = if let Some(ignore) = ignore {
            ignore.matched(path, path.is_dir()).is_ignore()
        } else {
            false
        };

        should_ignore || util::fs::is_in_oxen_hidden_dir(path)
    }

    pub fn construct_merkle_tree_from_parent(&self, staged_data: &StagedData) -> Result<(), OxenError> {
        // Step 1: Establish link to previous commit's tree
        if self.commit.parent_ids.len() != 1 {
            panic!("Merkle tree construction not yet implemented for multiple parents")
        }; // TODONOW: merge commits..

        let parent_tree_path = CommitEntryWriter::commit_tree_db(&self.repository.path, &self.commit.parent_ids[0]);
        let parent_tree = TreeDBReader::new(&self.repository, parent_tree_path)?;

        // Step 1.25: Copy over all parent tree stuff to new tree...

        for result in parent_tree.db.iterator(IteratorMode::Start) {
            match result {
                Ok((key, value)) => {
                    // Try parsing the key as a UTF-8 string
                    let path_str = match String::from_utf8(key.to_vec()) {
                        Ok(s) => s,
                        Err(e) => {
                            log::error!("Failed to convert key to UTF-8 string: {:?}", e);
                            continue;
                        }
                    };
                    
                    // Parse the `value` into a TreeNode
                    let node = match serde_json::from_slice::<TreeNode>(&value) {
                        Ok(n) => n,
                        Err(e) => {
                            log::error!("Failed to parse value as TreeNode: {:?}", e);
                            continue;
                        }
                    };
                    
                    let path = PathBuf::from(path_str);
        
                    // Copy entire old db to new tree, then modify
                    log::debug!("Copying over tree path {:?} from parent tree", path);
                    path_db::put(&self.tree_db, &path, &node)?;

                    // Get the path we just put to see what's going on 
                    let entry: TreeNode = path_db::get_entry(&self.tree_db, &path)?.unwrap();
                    log::debug!("here's the entry we jus got...{:?}", entry);
                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Could not iterate over db values"
                    ))
                }
            }
        }
        

        // Step 1.5: Get all dirs and put into hash set representing the commit tree 
    
        // TODONOW: factor out code = dirgetting

        let mut dir_paths = path_db::list_paths(&self.dir_db, &PathBuf::from(""))?;
        dir_paths.sort_by(|a, b| {
            let a_count = a.components().count();
            let b_count = b.components().count();
            b_count.cmp(&a_count)
        });

        // Build a map of dir to children 
        let mut dir_map: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();

        log::debug!("inserting stuff into dir_map now for commit with message {:?} and id {:?}", self.commit.message, self.commit.id);
        for dir in &dir_paths {
            let parent = dir.parent().unwrap_or(Path::new("")).to_path_buf();
            // Insert the dir itself 
            dir_map.entry(dir.to_path_buf()).or_default();
            log::debug!("inserting {:?} into dir_map", dir);
            if &parent != dir {
                log::debug!("inserting {:?} into dir_map", dir);
                dir_map.entry(parent).or_default().push(dir.to_path_buf());
            }
            
        }

        // Print every dir in dir map 
        for (dir, children) in &dir_map {
            log::debug!("dir_map dir: {:?} children: {:?}", dir, children);
        }


        // Step 2: Get all DIRS implicated in the changes for this commit via stageddata
        //...IF THEY ARE A DIR aka if they are in dirs db
        // TODONOW - schemas?
        // TODONOW - extrac tthis out probably

        log::debug!("Here's our dir map: {:?}", dir_map);

        let mut dirs_to_recompute: HashSet<PathBuf> = HashSet::new();
        for (path, entry) in staged_data.staged_files.iter() {
            let mut current_path = PathBuf::new(); 
            for component in path.iter() {
                current_path = current_path.join(component); 
                log::debug!("checking to see if we can insert component {:?} into dirs_to_recompute", current_path);
                log::debug!("dir_map contains {:?}: {:?}", current_path.clone(), dir_map.contains_key(&current_path));
                if dir_map.contains_key(&current_path) {
                    log::debug!("inserting {:?} into dirs_to_recompute", current_path);
                    dirs_to_recompute.insert(current_path.clone());
                } else {
                    log::debug!("unable to insert {:?} into dirs_to_recompute", current_path);
                }
            }
        }
        // Always need to recompute the root dir, but gets missed by this logic
        dirs_to_recompute.insert(PathBuf::from(""));
        // Sort them by descending component count to work bottom-up
        
        let mut modified_dirs_vec: Vec<PathBuf> = dirs_to_recompute.into_iter().collect();
        modified_dirs_vec.sort_by(|a, b| {
            let a_count = a.components().count();
            let b_count = b.components().count();
            b_count.cmp(&a_count)
        });

        

        for dir in &modified_dirs_vec {            
            log::debug!("Processing dir... {:?}", dir);
            let dir_entry_reader = CommitDirEntryReader::new(&self.repository, &self.commit.id, &dir)?;
            
            // Get all file children
            let children_entries = dir_entry_reader.list_entries()?;
            log::debug!("children_entries {:?}", children_entries);

            let child_entry_nodes: Vec<TreeChild> = children_entries.iter().map(|entry| CommitEntryWriter::entry_to_treechild(entry)).collect::<Vec<_>>();

            // Get all directory children as treenodes
            let children_dirs = dir_map.get(dir).unwrap();
            log::debug!("children_dirs: {:?}", children_dirs);

            // Read each dir into a vec of nodes
            // unwrapping both here bc if the dir doesn't exist in the db yet, something has gone wrong in treebuilding
            // let dir_entry_nodes: Vec<TreeChild> = children_dirs.iter().map(|path| path_db::get_entry(&self.tree_db, path).unwrap().unwrap()).collect::<Vec<_>>();
            let mut dir_entry_nodes: Vec<TreeChild> = Vec::new();

            log::debug!("Here's the state of the db right where it's about to fail.");
            // Iterate over self.tree_db and print everything out 
            self.temp_print_tree_db();

            for path in children_dirs {
                println!("Processing path: {:?}", path);

                match path_db::get_entry(&self.tree_db, path) {
                    Ok(Some(entry)) => {
                        dir_entry_nodes.push(entry);
                    },
                    Ok(None) => {
                        println!("Warning: No entry found for path: {:?}", path);
                        panic!();
                    },
                    Err(e) => {
                        println!("Error fetching entry for path {:?}: {:?}", path, e);
                        panic!();
                    }
                }
            }

            

            // Create tree_db nodes for these children. 
            for file_child in &child_entry_nodes {
                let file_node = TreeNode::File {
                    path: file_child.path().to_path_buf(),
                    hash: file_child.hash().to_string(),
                };
                path_db::put(&self.tree_db, &file_node.path(), &file_node)?;
            }

            // Combine the child and dir nodes into a list sorted by path() 
            let mut all_children: Vec<TreeChild> = child_entry_nodes;
            all_children.extend(dir_entry_nodes);
            all_children.sort_by(|a, b| a.path().cmp(b.path()));


            let node_hash = util::hasher::compute_subtree_hash(&all_children);

            // Create a Directory style TreeNode of the children in the pathdb 
            let dir_node = TreeNode::Directory {
                path: dir.to_path_buf(),
                children: all_children,
                hash: node_hash.to_string(),
            };

            // Put this node into the db 
            path_db::put(&self.tree_db, dir, &dir_node)?;
        }

        



        Ok(())

    }

    // pub fn construct_merkle_tree_from_parent(&self, staged_data: &StagedData) -> Result<(), OxenError> {
    //     // Parent commit 
    //     // TODONOW: handling for merge commits? 
    //     if self.commit.parent_ids.len() != 1 {
    //         panic!("Merkle tree construction not yet implemented for multiple parents")
    //     }

    //     let parent_tree_path = CommitEntryWriter::commit_tree_db(&self.repository.path, &self.commit.parent_ids[0]);
    //     let parent_tree = TreeDBReader::new(&self.repository, parent_tree_path)?;

    //     // Get all paths implicated in the changes via stageddata 
    //     // TODONOW - schemas?
    //     // TODONOW - extrac tthis out probably
    //     let mut affected_paths: HashSet<PathBuf> = HashSet::new();
    //     for (path, entry) in staged_data.staged_files.iter() {
    //         let mut current_path = PathBuf::new(); 
    //         for component in path.iter() {
    //             current_path = current_path.join(component); 
    //             affected_paths.insert(current_path.clone());
    //         }
    //     }

    //     // Handle affected paths from bottom up. Sorting by affected component count to ensure 
    //     // every child is processed before its parent
    //     let mut affected_paths_vec: Vec<PathBuf> = affected_paths.into_iter().collect();
    //     affected_paths_vec.sort_by(|a, b| {
    //         let a_count = a.components().count();
    //         let b_count = b.components().count();
    //         b_count.cmp(&a_count)
    //     });

    //     // Copy all paths from parent tree to new tree 
    //     for result in parent_tree.db.iterator(IteratorMode::Start) {
    //         match result {
    //             Ok((key, value)) => {
    //                 // Parse key as a path  
    //                 let path_str = String::from_utf8_lossy(&key).into_owned();
    //                 let path = PathBuf::from(path_str); // TODONOW lossy?

    //                 // Copy entire old db to new tree, then modifiy - // TODONOW copy whole
    //                 // if !affected_paths.contains(&path) {
    //                 path_db::put(&self.tree_db, &path, &value)?;
    //                 // }

    //             }
    //             _ => {
    //                 return Err(OxenError::basic_str(
    //                     "Could not iterate over db values"
    //                 ))
    //             }
    //         }
    //     }

    //     // Iterate through staged entries, making in-place modifications of the duplicate tree according to 
    //     // ADDED / MODIFIED / REMOVED status
    //     // TODONOW: Group these by dir to avoid multiple db ops / sorts as we go
    //     // TODONOW schemas
    //     for (path, entry) in &staged_data.staged_files {
    //         // Match on the StagedEntryStatus of the entry 
    //         match entry.status {
    //             StagedEntryStatus::Added => {
    //                 self.modify_tree_added(&path, &entry)?;
    //             }
    //             StagedEntryStatus::Modified => {
    //                 self.modify_tree_modified(&path,  &entry)?;
    //             }
    //             StagedEntryStatus::Removed => {
    //                 self.modify_tree_deleted(&path,  &entry)?;
    //             }
    //         }
    //     }

    //     // Re-hash upwards - affected paths are ordered by desc number of path components 
    //     for path in affected_paths_vec {
    //         log::debug!("Recomputing hashes for path {:?}", path);
    //         // Get node 
    //         let mut node: TreeNode = path_db::get_entry(&self.tree_db, &path)?.unwrap();
    //         // If node has no children after all ops, delete it and remove from its parent
    //         if node.children().is_empty() {
    //             // Delete the node itself
    //             path_db::delete(&self.tree_db, &path)?;

    //             let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
    //             let mut parent_node: TreeNode = path_db::get_entry(&self.tree_db, &parent)?.unwrap();
    //             parent_node.delete_child(&path.to_path_buf())?;
    //             path_db::put(&self.tree_db, &parent_node.path(), &parent_node)?;
    //             continue;
    //         }
    //         // If there are still children, sort them, then update the hash 
    //         node.sort_children()?;

    //         // Get hash of new children
    //         node.set_hash(util::hasher::compute_subtree_hash(&node.children()));
    //         path_db::put(&self.tree_db, &node.path(), &node)?;

    //     }


    //     Ok(())
    // }

    fn modify_tree_added(&self, path: &Path, entry: &StagedEntry) -> Result<(), OxenError> {
        log::debug!("modify_tree adding file {:?}", path);
        // Create a treenode and treechild for this entry 
        let new_child = TreeChild::File {
            path: path.to_path_buf(),
            hash: entry.hash.clone(),
        };
        let new_node = TreeNode::File {
            path: path.to_path_buf(),
            hash: entry.hash.clone(),
        };

        // Add the new file node to the db 
        path_db::put(&self.tree_db, &path, &new_node)?;

        // Get parent path // TODONOW what to do about "" directory with no parent
        let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
        // Get parent node from tree_db
        log::debug!("Trying to get parent node at path {:?}", parent);
        let mut parent_node: TreeNode = path_db::get_entry(&self.tree_db, &parent)?.unwrap();
        parent_node.add_child(new_child)?;

        // Put the new parent node into the tree_db - re-hashing occurs later to avoid it happening duplicatively
        path_db::put(&self.tree_db, &parent_node.path(), &parent_node)?;

        Ok(())
    }

    fn modify_tree_modified(&self, path: &Path, entry: &StagedEntry) -> Result<(), OxenError> {
        log::debug!("modify_tree modifying file {:?}", path);

        // Create a treenode and treechild for this entry 
        let new_child = TreeChild::File {
            path: path.to_path_buf(),
            hash: entry.hash.clone(),
        };
        let new_node = TreeNode::File {
            path: path.to_path_buf(),
            hash: entry.hash.clone(),
        };

        // Overwrite the existing file in the db 
        path_db::put(&self.tree_db, &path, &new_node)?;
        

        let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
        // Get parent node from tree_db
        let mut parent_node: TreeNode = path_db::get_entry(&self.tree_db, &parent)?.unwrap();
        parent_node.update_child(new_child)?;
        // Reinsert the parent node 
        path_db::put(&self.tree_db, &parent_node.path(), &parent_node)?;

        Ok(())
        
    }

    fn modify_tree_deleted(&self, path: &Path, entry: &StagedEntry) -> Result<(), OxenError> {
        // Delete the file node from the db 
        path_db::delete(&self.tree_db, &path)?;

        // Get the parent node from the db 
        let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
        let mut parent_node: TreeNode = path_db::get_entry(&self.tree_db, &parent)?.unwrap();
        parent_node.delete_child(&path.to_path_buf())?; // TODONOW error handling
        // Reinsert the parent node
        path_db::put(&self.tree_db, &parent_node.path(), &parent_node)?;

        Ok(())
    }



    pub fn construct_merkle_tree_new(&self) -> Result<(), OxenError> {
        // Get all directory paths...
        let mut dir_paths = path_db::list_paths(&self.dir_db, &PathBuf::from(""))?;

        // Sort all paths by descending component count so that we can build the tree from the bottom up
        dir_paths.sort_by(|a, b| {
            let a_count = a.components().count();
            let b_count = b.components().count();
            b_count.cmp(&a_count)
        });

         // todonow factor out code = dirgetting
        // Build a map of dir to children 
        let mut dir_map: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();

        for dir in &dir_paths {
            let parent = dir.parent().unwrap_or(Path::new("")).to_path_buf();
            // Insert the dir itself 
            dir_map.entry(dir.to_path_buf()).or_default();
            if &parent != dir {
                dir_map.entry(parent).or_default().push(dir.to_path_buf());
            }
            
        }

        // Iterate over the dir map to debug print 
        for (dir, children) in dir_map.iter() {
            log::debug!("here is dir: {:?} children: {:?}", dir, children);
        }

        // Treedb reader 
        // let tree_db_reader = TreeDBReader::new_from_db(&self.repository, self.tree_db)?;

        // Iterate over these dirs, getting all the children, both file and dir 
        // TODONOW extract with the other one
        for dir in &dir_paths {

            
            log::debug!("Processing dir... {:?}", dir);
            let dir_entry_reader = CommitDirEntryReader::new(&self.repository, &self.commit.id, &dir)?;
            
            // Get all file children
            let children_entries = dir_entry_reader.list_entries()?;
            log::debug!("children_entries {:?}", children_entries);

            let child_entry_nodes: Vec<TreeChild> = children_entries.iter().map(|entry| CommitEntryWriter::entry_to_treechild(entry)).collect::<Vec<_>>();

            // Get all directory children as treenodes
            let children_dirs = dir_map.get(dir).unwrap();
            log::debug!("children_dirs: {:?}", children_dirs);

            // Read each dir into a vec of nodes
            // unwrapping both here bc if the dir doesn't exist in the db yet, something has gone wrong in treebuilding
            let dir_entry_nodes: Vec<TreeChild> = children_dirs.iter().map(|path| path_db::get_entry(&self.tree_db, path).unwrap().unwrap()).collect::<Vec<_>>();

            // Create tree_db nodes for these children. 
            for file_child in &child_entry_nodes {
                let file_node = TreeNode::File {
                    path: file_child.path().to_path_buf(),
                    hash: file_child.hash().to_string(),
                };
                path_db::put(&self.tree_db, &file_node.path(), &file_node)?;
            }

            // Combine the child and dir nodes into a list sorted by path() 
            let mut all_children: Vec<TreeChild> = child_entry_nodes;
            all_children.extend(dir_entry_nodes);
            all_children.sort_by(|a, b| a.path().cmp(b.path()));


            let node_hash = util::hasher::compute_subtree_hash(&all_children);

            // Create a Directory style TreeNode of the children in the pathdb 
            let dir_node = TreeNode::Directory {
                path: dir.to_path_buf(),
                children: all_children,
                hash: node_hash.to_string(),
            };

            // Put this node into the db 
            path_db::put(&self.tree_db, dir, &dir_node)?;
        }

        Ok(())

    }

    fn entry_to_treechild(entry: &CommitEntry) -> TreeChild {
        TreeChild::File {
            path: entry.path.clone(),
            hash: entry.hash.clone(),
        }
    }

    




    // TODONOW delete or handle
    // fn construct_commit_merkle_tree(
    //     &self, 
    //     staged_data: &StagedData, 
    // ) -> Result<(), OxenError> {
    //     // Check if tree from previous commit exists
    //     // TODONOW: What if this commit has two parents...
    //     if &self.commit.parent_ids != 1 {
    //         panic!("Merkle tree construction not yet implemented for duplicate parents!")
    //     }

    //     // Get previous commit merkle tree

    //     let prev_tree = CommitEntryWriter::commit_tree_db(&self.repository.path, &self.commit.parent_ids[0]);
    //     // Check if there is anything at prev_tree path 
    //     if prev_tree.exists() {

    //         // self.r_update_tree_for_dir(&self.repository.path, &prev_tree, staged_data)
    //         self.update_tree
    //     } else { // TODONOW why are we passing paths in both these branches that are part of `self`
    //         self.r_build_tree_for_dir( &self.repository.path)
    //     }



    // }

    // TODONOW branch and recreat

    // TODONOW delete 
    pub fn temp_print_tree_db(&self) -> () {
        let iter = self.tree_db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key_bytes, value_bytes)) => {
                    match String::from_utf8(key_bytes.to_vec()) {
                        Ok(key_str) => {
                            let key_path = PathBuf::from(key_str);

                            // Attempting to deserialize the value into TreeNode
                            let deserialized_value: Result<TreeNode, _> =
                                serde_json::from_slice(&value_bytes);
                            match deserialized_value {
                                Ok(tree_node) => {
                                    log::debug!(
                                        "\n\ntree_db entry: {:?} -> {:?}\n\n",
                                        key_path,
                                        tree_node
                                    );
                                }
                                Err(e) => {
                                    log::error!("tree_db error deserializing value: {:?}", e);
                                }
                            }
                        }
                        Err(_) => {
                            log::error!("tree_db Could not decode key {:?}", key_bytes);
                        }
                    }
                }
                Err(e) => {
                    log::error!("tree_db error: {:?}", e);
                }
            }
        }
    }

    fn commit_staged_entries_with_prog(
        &self,
        commit: &Commit,
        staged_data: &StagedData,
        origin_path: &Path,
    ) -> Result<(), OxenError> {
        let size: u64 = unsafe { std::mem::transmute(staged_data.staged_files.len()) };
        if size == 0 {
            return Ok(());
        }
        let bar = oxen_progress_bar(size, ProgressBarType::Counter);
        let mut grouped = self.group_staged_files_to_dirs(&staged_data.staged_files);
        log::debug!(
            "commit_staged_entries_with_prog got groups {}",
            grouped.len()
        );

        // MERKLE
        // TODONOW - constructor?
        // let mut root_node = TreeNode {
        //     path: PathBuf::from("/"),
        //     children: vec![],
        //     hash: "".to_string(),
        // };

        for (dir, files) in grouped.iter() {
            log::debug!("doing dir: {:?}", dir);
            log::debug!("doing file: {:?}", files);
        }

        // Track entries in commit
        for (dir, files) in grouped.iter_mut() {
            // TODONOW likely error source
            let mut tree_dir_node: TreeNode =
                path_db::get_entry(&self.tree_db, dir)?.unwrap_or_default();
            tree_dir_node.set_path(dir.to_path_buf());
            // Write entries per dir
            //TODONOW: tree_db?
            let entry_writer = CommitDirEntryWriter::new(&self.repository, &self.commit.id, dir)?;
            path_db::put(&self.dir_db, dir, &0)?;

            // TODONOW parallelize or fold in
            // TODONOW figure out the duplciate hash issue with rbuild_tree_for_dir
            log::debug!(
                "commit_staged_entries_with_prog got files {} for dir {:?}",
                files.len(),
                dir
            );
            // Re-hash all entries except Removed
            // for (path, entry) in files.iter_mut() {
            //     // Re-hash here, for Merkle + for files changed after addition.
            //     if entry.status == StagedEntryStatus::Added  || entry.status == StagedEntryStatus::Modified {
            //         entry.hash = util::hasher::hash_file_contents(&path)?;
            //     }

            //     tree_dir_node.children.push(TreeChild::File {
            //         path: path.to_path_buf(),
            //         hash: entry.hash.to_owned(),
            //     });
            // }

            // TODONOW: how can we avoid having to store all the hashes up front...

            // Commit entries data
            files.par_iter().for_each(|(path, entry)| {
                self.commit_staged_entry(&entry_writer, commit, origin_path, path, entry);
                bar.inc(1);
            });

            // Get root path

            // TODONOW delete
            // log::debug!("commit_staged_entries_with_prog sorting children for reinsert");
            // tree_dir_node.children.sort_by(|a, b| a.path().cmp(&b.path()));

            // // Reinsert
            // log::debug!("commit_staged_entries_with_prog reinserting dir {:?} -> {:?}", dir, tree_dir_node);
            // path_db::put(&self.tree_db, dir, &tree_dir_node)?;
        }

        // Rebuild tree, temporarily without reference to `StagedEntries`

        // TODONOW: whats up with these paths...
        // match self.construct_merkle_tree() {
        //     Ok(root_node) => {
        //         log::debug!(
        //             "commit_staged_entries_with_prog got root node {:?}",
        //             root_node
        //         );
        //     }
        //     Err(e) => {
        //         log::error!(
        //             "commit_staged_entries_with_prog error rebuilding tree {:?}",
        //             e
        //         );
        //     }
        // }

        // TODONOW: are we sure we're re-hashing? 
        // TODONOW: make sure all hashes are correctly updated at this point.
        

        // Check if the merkle tree for the previous commit exists - TODONOW - merge commits w/ multi parent


        // self.construct_merkle_tree_new()?;
        self.temp_print_tree_db(); // TODONOW delete

        

        // self.construct_commit_merkle_tree(&staged_data)?;

        // Show all entries of the tree db.
        // TODO remove this debug

        // TODONOW remove
        let hello = util::fs::rlist_paths_in_dir(&self.repository.path);
        // Log out all of these paths
        for path in hello {
            log::debug!("\n\ncommit_staged_entries_with_prog path: {:?}", path);
        }

        // TODONOW debug print remove
        self.temp_print_tree_db();

        //

        // Track dirs in commit
        for (_path, staged_dirs) in staged_data.staged_dirs.paths.iter() {
            for staged_dir in staged_dirs.iter() {
                log::debug!(
                    "commit_staged_entries_with_prog adding dir {:?} -> {:?}",
                    staged_dir.path,
                    staged_dir.status
                );
                if staged_dir.status == StagedEntryStatus::Removed {
                    let entry_writer = CommitDirEntryWriter::new(
                        &self.repository,
                        &self.commit.id,
                        &staged_dir.path,
                    )?;
                    let num_entries = kv_db::count(&entry_writer.db)?;
                    if num_entries == 0 {
                        path_db::delete(&self.dir_db, &staged_dir.path)?;
                        continue;
                    }
                }
                path_db::put(&self.dir_db, &staged_dir.path, &0)?;
            }
        }

        // This needs to happen after both the commit dirs db and the entries dbs are fully updated - hence here 

        
        // TODONOW: Merge commit logic? 
        if self.commit.parent_ids.len() == 1 {
            let prev_tree_path = CommitEntryWriter::commit_tree_db(&self.repository.path, &self.commit.parent_ids[0]);
            if prev_tree_path.exists() {
                log::debug!("constructing merkle tree from parent commit");
                self.construct_merkle_tree_from_parent(staged_data)?;
            }
        } else {
            // Merge commit, initial commit, or no previous tree
            log::debug!("constructing new merkle tree");
            self.construct_merkle_tree_new()?;
        }

        bar.finish_and_clear();

        Ok(())
    }

    // TODONOW use oxen stuff instead of built in fs?
    // TODONOW: does this need to be iterative

    fn r_build_tree_for_dir(&self, dir_path: &PathBuf) -> Result<TreeNode, OxenError> {
        log::debug!("rbuild_tree_for_dir called on, {:?}", dir_path);
        let mut children: Vec<TreeChild> = Vec::new();
        let entries = fs::read_dir(dir_path)?; // TODONOW: oxenignore?
        let dir_path = util::fs::path_relative_to_dir(dir_path, &self.repository.path)?;
        for entry in entries {
            let path = entry?.path();
            if self.should_ignore_path(&path) {
                continue;
            }

            if path.is_file() {
                let hash = util::hasher::hash_file_contents(&path)?;
                children.push(TreeChild::File {
                    path: util::fs::path_relative_to_dir(path.clone(), &self.repository.path)?,
                    hash: hash.clone(),
                });
                let file_node = TreeNode::File {
                    path: util::fs::path_relative_to_dir(path, &self.repository.path)?,
                    hash: hash,
                };
                path_db::put(&self.tree_db, &file_node.path(), &file_node)?;
            } else if path.is_dir() {
                log::debug!("traversing on {:?}", path);
                let subtree_node = self.r_build_tree_for_dir(&path)?;
                children.push(TreeChild::Directory {
                    path: util::fs::path_relative_to_dir(path, &self.repository.path)?,
                    hash: subtree_node.hash().clone(),
                });
            }
        }

        children.sort_by(|a, b| a.path().cmp(&b.path()));
        let hash = util::hasher::compute_subtree_hash(&children);

        let mut subtree_node = TreeNode::Directory {
            path: dir_path.to_path_buf(),
            children: children,
            hash: hash.to_owned(),
        };

        // Write to db
        // TODONOW: parse out list of affected paths
        let relative_path = util::fs::path_relative_to_dir(dir_path, &self.repository.path)?;
        path_db::put(&self.tree_db, relative_path, &subtree_node)?;

        Ok(subtree_node)
    }

    // fn update_tree(&self, prev_tree_path: PathBuf, staged_data: &StagedData) -> Result<(), OxenError> {
    //     // Open a TreeDB at this path 

    //     let new_tree_path = CommitEntryWriter::commit_tree_db(&self.repository.path, &self.commit.id);

    //     let prev_tree_db: TreeDB<SingleThreaded> = TreeDB::new(&self.repository, &prev_tree_path)?;
    //     let new_tree_db: TreeDB<SingleThreaded> = TreeDB::new(&self.repository, &new_tree_path)?;
    //     // Get affected paths from the StagedData 
    //     // TODONOW schemas...
    //     // TODONOW probably extract this out.
    //     let mut affected_paths: HashSet<PathBuf> = HashSet::new();
    //     for (path, entry) in staged_data.staged_files.iter() {
    //         let mut current_path = PathBuf::new(); 
    //         for component in path.iter() {
    //             current_path = current_path.join(component); 
    //             affected_paths.insert(current_path);
    //         }
    //     }

    //     // Copy all unaffected nodes from old tree to new - iterate over the rocksdb 
    //     for result in prev_tree_db.db.iterator(rocksdb::IteratorMode::Start) {
    //         let (key, value) = match result {
    //             Ok((k, v)) => (k, v),
    //             Err(e) => return Err(OxenError::from(e))
    //         };

    //         let path = PathBuf::from(String::from_utf8_lossy(&key).to_string());
    //         if !affected_paths.contains(&path) {
    //             // Copy this node to the new tree
    //             new_tree_db.db.put(&key, &value)?;
    //         }
    //     }


    //     // Hanlde affected nodes 
    //     for path in &affected_paths {

    //     }




    //     Ok(())

    // }


    fn commit_staged_entry(
        &self,
        writer: &CommitDirEntryWriter,
        commit: &Commit,
        origin_path: &Path,
        path: &Path,
        entry: &StagedEntry,
    ) {
        match entry.status {
            StagedEntryStatus::Removed => match writer.remove_path_from_db(path) {
                Ok(_) => {}
                Err(err) => {
                    let err = format!("Failed to remove file: {err}");
                    panic!("{}", err)
                }
            },
            StagedEntryStatus::Modified => {
                match self.add_staged_entry_to_db(writer, commit, entry, origin_path, path) {
                    Ok(_) => {}
                    Err(err) => {
                        let err = format!("Failed to commit MODIFIED file: {err}");
                        panic!("{}", err)
                    }
                }
            }
            StagedEntryStatus::Added => {
                match self.add_staged_entry_to_db(writer, commit, entry, origin_path, path) {
                    Ok(_) => {}
                    Err(err) => {
                        let err = format!("Failed to ADD file: {err}");
                        panic!("{}", err)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {}
