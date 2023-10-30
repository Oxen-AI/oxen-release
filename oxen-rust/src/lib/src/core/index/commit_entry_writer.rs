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
use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::{CommitDirEntryReader, CommitEntryReader};

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

    // MERKLE - here's where we get dirs
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
        staged_entry: &StagedEntry,
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
            hash: staged_entry.hash.to_owned(),
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

    // TODONOW: issue with .oxenignore being modified at weird times possible?

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
        match self.r_build_tree_for_dir(&self.repository.path) {
            Ok(root_node) => {
                log::debug!(
                    "commit_staged_entries_with_prog got root node {:?}",
                    root_node
                );
            }
            Err(e) => {
                log::error!(
                    "commit_staged_entries_with_prog error rebuilding tree {:?}",
                    e
                );
            }
        }

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
            log::debug!("the entry is {:?}", &entry);
            // let path = util::fs::path_relative_to_dir(entry?.path(), &self.repository.path)?;
            let path = entry?.path();
            log::debug!("the path is {:?}", path);
            if self.should_ignore_path(&path) {
                log::debug!("ignoring / skipping the path");
                continue;
            }
            log::debug!("not skipping the path");

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
    //     // for result in prev_tree_db.db.iterator(rocksdb::IteratorMode::Start) {
    //     //     let (key, value) = match result {
    //     //         Ok((k, v)) => (k, v),
    //     //         Err(e) => return Err(OxenError::from(e))
    //     //     };

    //     //     let path = PathBuf::from(String::from_utf8_lossy(&key).to_string());
    //     //     if !affected_paths.contains(&path) {
    //     //         // Copy this node to the new tree
    //     //         new_tree_db.db.put(&key, &value)?;
    //     //     }
    //     // }






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
