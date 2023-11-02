use crate::api;
use crate::constants::{self, DEFAULT_BRANCH_NAME, HISTORY_DIR, VERSIONS_DIR, SCHEMAS_TREE_PREFIX};
use crate::core::db;
use crate::core::db::tree_db::{TreeChild, TreeNode};
use crate::core::db::{kv_db, path_db};
use crate::core::index::{CommitDirEntryWriter, RefWriter, SchemaWriter};
use crate::error::OxenError;
use crate::model::schema::Schema;
use crate::model::{
    Commit, CommitEntry, LocalRepository, StagedData, StagedEntry, StagedEntryStatus,
};
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};

use filetime::FileTime;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded, IteratorMode};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::{CommitDirEntryReader, CommitEntryReader, TreeDBReader, SchemaReader};

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
        self.commit_schemas(commit, &staged_data.staged_schemas)?;
        self.construct_commit_merkle_tree(staged_data)
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

    fn construct_commit_merkle_tree(
        &self, 
        staged_data: &StagedData,
    ) -> Result<(), OxenError> {
        if self.commit.parent_ids.len() == 1 {
            let prev_tree_path = CommitEntryWriter::commit_tree_db(&self.repository.path, &self.commit.parent_ids[0]);
            if prev_tree_path.exists() {
                self.construct_merkle_tree_from_parent(staged_data)?;
            }
        } else {
            // Merge commit, initial commit, or no previous tree
            self.construct_merkle_tree_new()?;
        }
        self.temp_print_tree_db();
        Ok(())
    }

    fn merkelize_schemas(
        &self
    ) -> Result<(), OxenError> {
        // Add all schemas
        let schema_reader = SchemaReader::new(&self.repository, &self.commit.id)?;
        let schemas = schema_reader.list_schemas()?;

        if schemas.is_empty() {
            path_db::delete(&self.tree_db, &PathBuf::from(SCHEMAS_TREE_PREFIX))?;
            if let Some(mut root) = path_db::get_entry(&self.tree_db, &PathBuf::from(""))? {
                if let TreeNode::Directory { .. } = &mut root {
                    root.delete_child(&PathBuf::from(SCHEMAS_TREE_PREFIX))?;
                    root.set_hash(util::hasher::compute_subtree_hash(&root.children()));
                    path_db::put(&self.tree_db, &root.path(), &root)?;
                }
            }
            return Ok(());
        }

        let mut schema_children: Vec<TreeChild> = Vec::new();
        for (path, schema) in schemas {
            let node = TreeNode::File {
                path: Path::new(SCHEMAS_TREE_PREFIX).join(path.clone()),
                hash: schema.hash.clone(),
            };
            let child = TreeChild::File {
                path: Path::new(SCHEMAS_TREE_PREFIX).join(path.clone()),
                hash: schema.hash.clone(),
            };

            schema_children.push(child);
            path_db::put(&self.tree_db, &node.path(), &node)?;
        }

        // Sort lexically by path 
        schema_children.sort_by(|a, b| a.path().cmp(b.path()));
        let schemas_hash = util::hasher::compute_subtree_hash(&schema_children);
        let schemas_node = TreeNode::Directory {
            path: Path::new(SCHEMAS_TREE_PREFIX).to_path_buf(),
            children: schema_children,
            hash: schemas_hash.clone()
        };
        let schemas_child = TreeChild::Directory {
            path: Path::new(SCHEMAS_TREE_PREFIX).to_path_buf(),
            hash: schemas_hash
        };

        path_db::put(&self.tree_db, &schemas_node.path(), &schemas_node)?;

        // Add the schemas db as a child to the root node 
        let mut root: TreeNode = path_db::get_entry(&self.tree_db, &PathBuf::from(""))?.unwrap();
        root.upsert_child(schemas_child)?;
        root.set_hash(util::hasher::compute_subtree_hash(&root.children()));
        path_db::put(&self.tree_db, &root.path(), &root)?;
        
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

    pub fn construct_merkle_tree_from_parent(&self, staged_data: &StagedData) -> Result<(), OxenError> {
        let parent_tree = TreeDBReader::new(&self.repository, &self.commit.parent_ids[0])?;

        // Step 1: Copy over all entries from parent tree to new tree
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
                    path_db::put(&self.tree_db, &path, &node)?;

                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Could not iterate over db values"
                    ))
                }
            }
        }
        

        // Step 2: Get all dirs and put into hash set representing the commit tree 
        // TODONOW: Factor out the dirgetting here
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
            // self.temp_print_tree_db();

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
        
        self.merkelize_schemas()
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

        // Iterate over these dirs, getting all the children, both file and dir 
        // TODONOW extract with the other one
        for dir in &dir_paths {

            let dir_entry_reader = CommitDirEntryReader::new(&self.repository, &self.commit.id, &dir)?;
            
            // Get all file children
            let children_entries = dir_entry_reader.list_entries()?;

            let child_entry_nodes: Vec<TreeChild> = children_entries.iter().map(|entry| CommitEntryWriter::entry_to_treechild(entry)).collect::<Vec<_>>();

            // Get all directory children as treenodes
            let children_dirs = dir_map.get(dir).unwrap();

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
        self.merkelize_schemas()
    }

    fn entry_to_treechild(entry: &CommitEntry) -> TreeChild {
        TreeChild::File {
            path: entry.path.clone(),
            hash: entry.hash.clone(),
        }
    }


    // TODONOW delete 
    pub fn temp_print_tree_db(&self) -> () {
        log::debug!("PRINTING COMMIT TREE FOR COMMIT {:?}", self.commit.id);
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

            // Commit entries data
            files.par_iter().for_each(|(path, entry)| {
                self.commit_staged_entry(&entry_writer, commit, origin_path, path, entry);
                bar.inc(1);
            });
        }

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
                match self.add_staged_entry_to_db(writer, commit, origin_path, path) {
                    Ok(_) => {}
                    Err(err) => {
                        let err = format!("Failed to commit MODIFIED file: {err}");
                        panic!("{}", err)
                    }
                }
            }
            StagedEntryStatus::Added => {
                match self.add_staged_entry_to_db(writer, commit, origin_path, path) {
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
mod tests {
    use std::path::PathBuf;
    use crate::error::OxenError;
    use crate::command;
    use crate::test;
    use crate::util;
    use serde_json::json;
    use crate::core::index::TreeDBReader;


    #[tokio::test]
    async fn test_merkle_tree_tracks_schemas() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("large_files");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_200k_csv();
            util::fs::copy(from_file, &csv_file)?;

            command::add(&local_repo, &csv_file)?;
            let first_commit = command::commit(&local_repo, "add test.csv")?;

            // Get commit merkle hash for root level of repo for the most recent commit
          

            // Add a schema for the csv file
            let schema_ref = "large_files/test.csv";
            let schema_metadata = json!({
                "description": "A dataset of faces",
                "task": "gen_faces"
            });

            let column_name = "image_id".to_string();
            let column_metadata = json!({
                "root": "images"
            });
            command::schemas::add_column_metadata(
                &local_repo,
                schema_ref,
                &column_name,
                &column_metadata,
            )?;

            command::schemas::add_schema_metadata(&local_repo, schema_ref, &schema_metadata)?;
            let second_commit = command::commit(&local_repo, "add test.csv schema metadata")?;

            // Add column-level schema details

            // Get merkle root hashes for all 3 commits and compare. All should be different 

            let db = TreeDBReader::new(&local_repo, &first_commit.id)?;
            let root_node = db.get_entry(&PathBuf::from(""))?.unwrap();
            let first_root_hash = root_node.hash().to_string();

            let db = TreeDBReader::new(&local_repo, &second_commit.id)?;
            let root_node = db.get_entry(&PathBuf::from(""))?.unwrap();
            let second_root_hash = root_node.hash().to_string();
            assert_ne!(first_root_hash, second_root_hash);

            Ok(())
        }).await
    }
}