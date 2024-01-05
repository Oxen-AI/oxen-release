use crate::api;
use crate::constants::{
    self, DEFAULT_BRANCH_NAME, HISTORY_DIR, SCHEMAS_TREE_PREFIX, TMP_DIR, VERSIONS_DIR,
};
use crate::core::db;
use crate::core::db::path_db;
use crate::core::db::tree_db::{TreeObject, TreeObjectChild, TreeObjectChildWithStatus};
use crate::core::index::{
    CommitDirEntryWriter, LegacyCommitDirEntryReader, RefWriter, SchemaReader, SchemaWriter,
};
use crate::error::OxenError;

use crate::model::{
    Commit, CommitEntry, LocalRepository, StagedData, StagedEntry, StagedEntryStatus, StagedSchema,
};
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::view::schema::SchemaWithPath;

use filetime::FileTime;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::{CommitDirEntryReader, CommitEntryReader, ObjectDBReader};

pub struct CommitEntryWriter {
    repository: LocalRepository,
    pub dir_db: DBWithThreadMode<MultiThreaded>,
    pub dir_hashes_db: DBWithThreadMode<MultiThreaded>,
    files_db: DBWithThreadMode<MultiThreaded>,
    schemas_db: DBWithThreadMode<MultiThreaded>,
    pub dirs_db: DBWithThreadMode<MultiThreaded>,
    vnodes_db: DBWithThreadMode<MultiThreaded>,
    commit: Commit,
}

impl CommitEntryWriter {
    pub fn versions_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(VERSIONS_DIR))
    }

    pub fn objects_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(constants::OBJECTS_DIR))
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

    // Let dir hash db
    pub fn commit_dir_hash_db(path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(path, commit_id).join(constants::DIR_HASHES_DIR)
    }

    pub fn files_db_dir(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_FILES_DIR)
    }

    pub fn schemas_db_dir(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_SCHEMAS_DIR)
    }

    pub fn dirs_db_dir(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_DIRS_DIR)
    }

    pub fn vnodes_db_dir(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_VNODES_DIR)
    }

    // pub fn temp_commit_hashes_db_dir(repo: &LocalRepository) -> PathBuf {
    //     util::fs::oxen_hidden_dir(&repo.path)
    //         .join(constants::OBJECTS_DIR)
    //         .join("commit-hashes")
    // }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitEntryWriter, OxenError> {
        log::debug!("CommitEntryWriter::new() commit_id: {}", commit.id);
        let db_path = CommitEntryWriter::commit_dir_db(&repository.path, &commit.id);
        let tree_db_path = CommitEntryWriter::commit_tree_db(&repository.path, &commit.id);
        let files_db_path = CommitEntryWriter::files_db_dir(repository);
        let schemas_db_path = CommitEntryWriter::schemas_db_dir(repository);
        let dirs_db_path = CommitEntryWriter::dirs_db_dir(repository);
        let vnodes_db_path = CommitEntryWriter::vnodes_db_dir(repository);
        let dir_hashes_db_path =
            CommitEntryWriter::commit_dir_hash_db(&repository.path, &commit.id);
        // let temp_commit_hashes_db_path = CommitEntryWriter::temp_commit_hashes_db_dir(&repository);

        for path in &[
            &db_path,
            &tree_db_path,
            &files_db_path,
            &schemas_db_path,
            &dirs_db_path,
            &vnodes_db_path,
            &dir_hashes_db_path,
            // &temp_commit_hashes_db_path,
        ] {
            if !path.exists() {
                util::fs::create_dir_all(path)?;
            }
        }

        let opts = db::opts::default();
        Ok(CommitEntryWriter {
            repository: repository.clone(),
            dir_db: DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?,
            files_db: DBWithThreadMode::open(&opts, dunce::simplified(&files_db_path))?,
            schemas_db: DBWithThreadMode::open(&opts, dunce::simplified(&schemas_db_path))?,
            dirs_db: DBWithThreadMode::open(&opts, dunce::simplified(&dirs_db_path))?,
            vnodes_db: DBWithThreadMode::open(&opts, dunce::simplified(&vnodes_db_path))?,
            dir_hashes_db: DBWithThreadMode::open(&opts, dunce::simplified(&dir_hashes_db_path))?,
            // temp_commit_hashes_db: DBWithThreadMode::open(
            //     &opts,
            //     dunce::simplified(&temp_commit_hashes_db_path),
            // )?,
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

            // Copy parent dirs into our new dirs db
            let reader = CommitEntryReader::new(repo, &parent_commit)?;
            let dirs = reader.list_dirs()?;
            for dir in dirs {
                path_db::put(&self.dir_db, &dir, &0)?;
            }

            // Copy parent schemas
            let schemas = {
                let schema_reader = SchemaReader::new(repo, &parent_commit.id)?;
                schema_reader.list_schemas()?
            };
            let schema_writer = SchemaWriter::new(repo, &self.commit.id)?;
            for (path, schema) in schemas {
                schema_writer.put_schema_for_file(&path, &schema)?;
            }
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

        // // Get last modified time
        let metadata = fs::metadata(&full_path).unwrap();
        let mtime = FileTime::from_last_modification_time(&metadata);

        let metadata = fs::metadata(&full_path)?;

        // // Re-hash for issues w/ adding
        let hash = util::hasher::hash_file_contents(&full_path)?;

        // // Create entry object to as json
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
        _writer: &CommitDirEntryWriter,
        commit_entry: CommitEntry,
    ) -> Result<(), OxenError> {
        let entry = self.backup_file_to_versions_dir(origin_path, commit_entry)?;
        log::debug!(
            "add_commit_entry with hash {:?} -> {}",
            entry.path,
            entry.hash
        );

        // writer.add_commit_entry(&entry)?;
        Ok(())
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
        log::debug!("here's the status for commit {:#?}", staged_data);
        self.copy_parent_dbs(&self.repository, &commit.parent_ids.clone())?;
        self.commit_staged_entries_with_prog(commit, staged_data, origin_path)?;
        self.commit_schemas(commit, &staged_data.staged_schemas)?;
        self.construct_commit_merkle_tree(staged_data, origin_path)?;
        // self.new_temp_print_tree_db()?;
        Ok(())
    }

    fn commit_schemas(
        &self,
        commit: &Commit,
        staged_schemas: &HashMap<PathBuf, StagedSchema>,
    ) -> Result<(), OxenError> {
        log::debug!("commit_schemas got {} schemas", staged_schemas.len());

        let schema_writer = SchemaWriter::new(&self.repository, &commit.id)?;
        for (path, staged_schema) in staged_schemas.iter() {
            if staged_schema.status == StagedEntryStatus::Removed {
                schema_writer.delete_schema(&staged_schema.schema)?;
                schema_writer.delete_schema_for_file(path, &staged_schema.schema)?;
            } else {
                if !schema_writer.has_schema(&staged_schema.schema) {
                    schema_writer.put_schema(&staged_schema.schema)?;
                }
                // Map the file to the schema
                schema_writer.put_schema_for_file(path, &staged_schema.schema)?;
            }
        }

        Ok(())
    }

    fn construct_commit_merkle_tree(
        &self,
        staged_data: &StagedData,
        origin_path: &Path,
    ) -> Result<(), OxenError> {
        if self.commit.parent_ids.is_empty() {
            self.construct_merkle_tree_empty(origin_path)
        } else {
            self.construct_merkle_tree_from_parent(staged_data, origin_path)
        }
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
    pub fn construct_merkle_tree_from_parent(
        &self,
        staged_data: &StagedData,
        origin_path: &Path,
    ) -> Result<(), OxenError> {
        // Get all dirs so we can find which changed
        // Get last commit id
        let parent_commit_id = match &self.commit.parent_ids.len() {
            1 => &self.commit.parent_ids[0],
            2 => &self.commit.parent_ids[1],
            _ => panic!("Unexpected number of parent commit ids"),
        };

        log::debug!(
            "writing tree for commit {:?} with parent {:?}",
            self.commit.message,
            parent_commit_id
        );

        // Get parent
        let _parent = api::local::commits::get_by_id(&self.repository, parent_commit_id);

        let mut dir_map: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let dir_paths = path_db::list_paths(&self.dir_db, &PathBuf::from(""))?;

        for dir in &dir_paths {
            let parent = dir.parent().unwrap_or(Path::new("")).to_path_buf();
            dir_map.entry(dir.to_path_buf()).or_default();
            if &parent != dir {
                dir_map.entry(parent).or_default().push(dir.to_path_buf());
            }
        }

        // Step 2: parse StagedEntry paths to save time by only recomputing dirs that saw changes in this commit
        let mut dirs_to_recompute: HashSet<PathBuf> = HashSet::new();
        for (path, _entry) in staged_data.staged_files.iter() {
            let mut current_path = PathBuf::new();
            for component in path.iter() {
                current_path = current_path.join(component);
                if dir_map.contains_key(&current_path) {
                    dirs_to_recompute.insert(current_path.clone());
                }
            }
        }

        // Also recomepute for staged schemas
        for (path, _schema) in staged_data.staged_schemas.iter() {
            let mut current_path = PathBuf::new();
            for component in path.iter() {
                current_path = current_path.join(component);
                if dir_map.contains_key(&current_path) {
                    dirs_to_recompute.insert(current_path.clone());
                }
            }
        }

        // No more need for step 3. We're not deleting stuff because we're not copying our treedb anymore,
        // just building up a new reference head when needed.

        // Always need to recompute the root dir, this case was getting missed
        dirs_to_recompute.insert(PathBuf::from(""));
        let mut modified_dirs_vec: Vec<PathBuf> = dirs_to_recompute.into_iter().collect();

        log::debug!(
            "affected dirs are {:#?} for commit {:?}",
            modified_dirs_vec,
            self.commit
        );

        self.create_tree_nodes_from_affected_dirs(
            &mut modified_dirs_vec,
            dir_map,
            staged_data,
            parent_commit_id.to_string(),
            origin_path,
        )?;

        let root_hash: String =
            path_db::get_entry(&self.dir_hashes_db, PathBuf::from(""))?.unwrap();

        log::debug!(
            "client got root hash {:?} for commit with message {:?} and id {:?}",
            root_hash,
            self.commit.message,
            self.commit.id
        );

        // Insert into the commit hashes db
        // path_db::put(&self.temp_commit_hashes_db, &self.commit.id, &root_hash)?;
        Ok(())
    }

    pub fn construct_merkle_tree_from_legacy_commit(
        &self,
        _origin_path: &Path,
    ) -> Result<(), OxenError> {
        log::debug!("constructing new merkle tree");
        // Operate on all dirs to make the tree from scratch...
        let mut dir_paths = path_db::list_paths(&self.dir_db, &PathBuf::from(""))?;

        log::debug!("server got dirs {:?}", dir_paths);
        // So this is probably getting transferred over properly, meaning we're getting all the stuff from the dirs db.

        if dir_paths.is_empty() {
            // Initial commit - we want to create the root node as empty, then return
            let root_node = TreeObject::Dir {
                // path: PathBuf::from(""),
                children: Vec::new(),
                hash: util::hasher::compute_children_hash(&Vec::new()),
            };
            path_db::put(&self.dirs_db, root_node.hash(), &root_node)?;
            path_db::put(&self.dir_hashes_db, PathBuf::from(""), &root_node.hash())?;

            return Ok(());
        }

        // Build a map of dir to children
        let mut dir_map: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for dir in &dir_paths {
            let parent = dir.parent().unwrap_or(Path::new("")).to_path_buf();
            dir_map.entry(dir.to_path_buf()).or_default();
            if &parent != dir {
                dir_map.entry(parent).or_default().push(dir.to_path_buf());
            }
        }

        self.create_tree_nodes_from_dirs(&mut dir_paths, dir_map)?;

        // If dir path
        let _root_hash: String =
            path_db::get_entry(&self.dir_hashes_db, PathBuf::from(""))?.unwrap();

        Ok(())
    }

    pub fn construct_merkle_tree_empty(&self, _origin_path: &Path) -> Result<(), OxenError> {
        // Initial commits will never have entries - just need to populate the root node

        let empty_root = TreeObject::Dir {
            children: Vec::new(),
            hash: util::hasher::compute_children_hash(&Vec::new()),
        };

        path_db::put(&self.dirs_db, empty_root.hash(), &empty_root)?;
        path_db::put(&self.dir_hashes_db, PathBuf::from(""), &empty_root.hash())?;

        Ok(())
    }

    pub fn get_node_from_child(
        &self,
        child: &TreeObjectChild,
    ) -> Result<Option<TreeObject>, OxenError> {
        match child {
            TreeObjectChild::File { hash, .. } => path_db::get_entry(&self.files_db, hash),
            TreeObjectChild::Schema { hash, .. } => path_db::get_entry(&self.schemas_db, hash),
            TreeObjectChild::Dir { hash, .. } => path_db::get_entry(&self.dirs_db, hash),
            TreeObjectChild::VNode { hash, .. } => path_db::get_entry(&self.vnodes_db, hash),
        }
    }

    pub fn get_root_node(&self) -> Result<Option<TreeObject>, OxenError> {
        let root_hash: String =
            path_db::get_entry(&self.dir_hashes_db, PathBuf::from(""))?.unwrap();
        path_db::get_entry(&self.dirs_db, root_hash)
    }

    fn process_affected_dir(
        &self,
        dir: PathBuf,
        parent_hash_db: &DBWithThreadMode<MultiThreaded>,
        staged_entries_map: &HashMap<PathBuf, Vec<TreeObjectChildWithStatus>>,
    ) -> Result<(), OxenError> {
        log::debug!("processing affected dir... {:?}", dir);
        // STEP 1: If this dir has a hash in the previous commit, grab its node as the starting point
        let prev_dir_hash: Option<String> = path_db::get_entry(parent_hash_db, dir.clone())?;
        let prev_dir_object: TreeObject = if let Some(prev_hash) = prev_dir_hash {
            path_db::get_entry(&self.dirs_db, prev_hash)?.unwrap()
        } else {
            TreeObject::Dir {
                children: Vec::new(),
                hash: util::hasher::compute_children_hash(&Vec::new()),
            }
        };

        // STEP 2: Get all the new children from the staged data map for this dir
        let new_children: Vec<TreeObjectChildWithStatus> = staged_entries_map
            .get(&dir)
            .unwrap_or(&Vec::new())
            .clone()
            .to_vec();

        // STEP 3: Get vnodes for this dir, including new ones (not on the previous dir object's child attr)
        log::debug!(
            "here's the new children for dir {:?}: {:#?} and commit {:#?}",
            dir,
            new_children,
            self.commit
        );
        let affected_vnodes = self.get_affected_vnodes(&new_children)?;
        log::debug!(
            "here affected_vnodes are {:#?} for commit {:#?} and dir {:#?}",
            affected_vnodes,
            self.commit,
            dir
        );
        let prev_vnode_children = prev_dir_object.children();
        let mut prev_vnode_map: HashMap<String, String> = HashMap::new();
        for vnode in prev_vnode_children {
            prev_vnode_map.insert(
                vnode.path().to_string_lossy().to_string(),
                vnode.hash().to_string(),
            );
        }

        // Get a set of all unique vnode children (those in either the prev or new vnode children lists)
        let mut all_vnodes: HashSet<String> = HashSet::new();
        for vnode in prev_vnode_children {
            all_vnodes.insert(vnode.path().to_string_lossy().to_string());
        }

        log::debug!(
            "affected_vnodes for commit with message {:?} and dir {:?} are {:?}",
            self.commit.message,
            dir,
            affected_vnodes
        );
        for vnode in affected_vnodes.keys() {
            all_vnodes.insert(vnode.to_string());
        }

        log::debug!(
            "vnodes we're processing here are {:?} for heya commit with message {:?}",
            all_vnodes,
            self.commit.message
        );

        log::debug!(
            "prev_vnode_children are {:?} for commit with message {:?}",
            prev_vnode_children,
            self.commit.message
        );

        let updated_dir_children =
            self.update_dir_vnode_children(all_vnodes, &prev_vnode_map, &affected_vnodes)?;

        // Set the dir node's children

        // lexically sort vnode children
        if updated_dir_children.is_empty() {
            path_db::delete(&self.dir_db, dir.clone())?;
            path_db::delete(&self.dir_hashes_db, dir.clone())?;
        }

        let dir_hash = util::hasher::compute_children_hash(&updated_dir_children);
        let updated_dir_object = TreeObject::Dir {
            children: updated_dir_children,
            hash: dir_hash,
        };

        path_db::put(
            &self.dirs_db,
            updated_dir_object.hash(),
            &updated_dir_object,
        )?;

        path_db::put(
            &self.dir_hashes_db,
            dir.clone(),
            &updated_dir_object.hash().to_string(),
        )?;

        Ok(())
    }

    fn update_dir_vnode_children(
        &self,
        all_vnodes: HashSet<String>,
        prev_vnode_map: &HashMap<String, String>,
        affected_vnode_map: &HashMap<String, Vec<TreeObjectChildWithStatus>>,
    ) -> Result<Vec<TreeObjectChild>, OxenError> {
        let mut result: Vec<TreeObjectChild> = Vec::new();
        for vnode_name in all_vnodes {
            let prev_vnode: TreeObject = if prev_vnode_map.contains_key(&vnode_name) {
                path_db::get_entry(&self.vnodes_db, &prev_vnode_map[&vnode_name])?.unwrap()
            } else {
                TreeObject::VNode {
                    children: Vec::new(),
                    hash: util::hasher::compute_children_hash(&Vec::new()),
                    name: vnode_name.clone(),
                }
            };

            let children = prev_vnode.children().clone();

            // Step 2: Map the children by path to avoid duplicates when merging old with new
            let mut old_children_map = HashMap::new();
            for child in children {
                old_children_map.insert(child.path().to_string_lossy().to_string(), child);
            }

            // Step 3: Get the new children from the staged data map for this vnode
            let new_children = affected_vnode_map
                .get(&vnode_name)
                .unwrap_or(&Vec::new())
                .clone();

            let merged_children =
                self.update_vnode_children(&mut old_children_map, new_children)?;

            // These are now the merged and sorted children we want to hash and insert

            let updated_vnode_hash = util::hasher::compute_children_hash(&merged_children);

            let updated_vnode = TreeObject::VNode {
                children: merged_children,
                hash: updated_vnode_hash,
                name: vnode_name.clone(),
            };

            let updated_vnode_child = TreeObjectChild::VNode {
                path: PathBuf::from(vnode_name),
                hash: updated_vnode.hash().to_string(),
            };

            // TODONOW: if broken, "&& !children_path_map.is_empty()"
            if !updated_vnode.children().is_empty() {
                path_db::put(&self.vnodes_db, updated_vnode.hash(), &updated_vnode)?;
                // Add the vnode
                result.push(updated_vnode_child);
            }
        }

        // Lexically sort result by path to allow later binary searching + consistent hashing
        result.sort_by(|a, b| a.path().cmp(b.path()));
        Ok(result)
    }

    // Merges the vnode children in the last commit with the children in this commit
    // according to stagedentry status, sorts, and returns.
    fn update_vnode_children(
        &self,
        prev_children_map: &mut HashMap<String, TreeObjectChild>,
        new_children: Vec<TreeObjectChildWithStatus>,
    ) -> Result<Vec<TreeObjectChild>, OxenError> {
        for child_with_status in new_children {
            match child_with_status.status {
                StagedEntryStatus::Added | StagedEntryStatus::Modified => {
                    // Add or replace the child in the map
                    prev_children_map.insert(
                        child_with_status.child.path_as_str().to_string(),
                        child_with_status.child.clone(),
                    );
                }
                StagedEntryStatus::Removed => {
                    // Remove the child from the map
                    prev_children_map.remove(&child_with_status.child.path_as_str().to_string());
                }
            }
        }
        let mut updated_children: Vec<TreeObjectChild> =
            prev_children_map.values().cloned().collect();
        // Sort lexically
        updated_children.sort_by(|a, b| a.path().cmp(b.path()));
        Ok(updated_children)
    }

    fn write_gather_vnode_children(
        &self,
        children: Vec<TreeObjectChild>,
    ) -> Result<Vec<TreeObjectChild>, OxenError> {
        let mut groups: HashMap<String, Vec<TreeObjectChild>> = HashMap::new();

        // Group by first two letters of hash
        for child in children {
            let hash_prefix = &util::hasher::hash_str(child.path_as_str())[..2];
            groups
                .entry(hash_prefix.to_string())
                .or_default()
                .push(child);
        }

        // Sort each group and create VNodes
        let mut vnodes: Vec<TreeObjectChild> = Vec::new();
        for (name, mut group_children) in groups {
            group_children.sort_by(|a, b| a.path().cmp(b.path()));

            // Here you can compute a combined hash for the group if needed
            let combined_hash = util::hasher::compute_children_hash(&group_children);

            let vnode_object = TreeObject::VNode {
                hash: combined_hash.to_string(),
                children: group_children,
                name: name.clone(),
            };

            log::debug!(
                "putting vnode {:#?} into vnodes_db write gather",
                vnode_object
            );
            path_db::put(&self.vnodes_db, vnode_object.hash(), &vnode_object)?;

            vnodes.push(TreeObjectChild::VNode {
                hash: combined_hash,
                path: PathBuf::from(name),
            });
        }
        Ok(vnodes)
    }

    fn create_tree_nodes_from_dirs(
        &self,
        dirs: &mut Vec<PathBuf>,
        dir_map: HashMap<PathBuf, Vec<PathBuf>>,
    ) -> Result<(), OxenError> {
        // Sort dirs by descending component count to work bottom up
        dirs.sort_by(|a, b| {
            let a_count = a.components().count();
            let b_count = b.components().count();
            b_count.cmp(&a_count)
        });

        let schema_reader = SchemaReader::new(&self.repository, &self.commit.id)?;
        let schemas = schema_reader.list_schemas()?;

        // Map parent dirs to schemas
        let mut schema_map: HashMap<PathBuf, Vec<SchemaWithPath>> = HashMap::new();
        for (path, schema) in schemas {
            let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
            let schema_with_path = SchemaWithPath {
                path: PathBuf::from(SCHEMAS_TREE_PREFIX)
                    .join(path.clone())
                    .to_string_lossy()
                    .to_string(),
                schema,
            };
            schema_map.entry(parent).or_default().push(schema_with_path);
        }

        // Starting with the lowest-down dirs...
        for dir in dirs {
            log::debug!("new merkle constructor processing dir {:?}", dir);
            let file_child_objs = self.write_file_objects_for_dir(dir.to_path_buf())?;
            log::debug!("got file_child_objs {:?}", file_child_objs);
            let schema_child_objs =
                self.write_schema_objects_for_dir(dir.to_path_buf(), &schema_map)?;
            log::debug!("got schema_child_objs {:?}", schema_child_objs);
            let dir_child_objs = self.gather_dir_children_for_dir(dir.to_path_buf(), &dir_map)?;
            log::debug!("got dir_child_objs {:?}", dir_child_objs);

            let mut all_children: Vec<TreeObjectChild> = file_child_objs;
            all_children.extend(schema_child_objs);
            all_children.extend(dir_child_objs);

            // Lexically sort the children

            let mut vnode_child_objs = self.write_gather_vnode_children(all_children)?;

            // Lexically sort the vnode_child_objs by path
            vnode_child_objs.sort_by(|a, b| a.path().cmp(b.path()));

            // Hash them
            let dir_hash = util::hasher::compute_children_hash(&vnode_child_objs);

            // Create a Dir TreeObject
            let dir_object = TreeObject::Dir {
                hash: dir_hash.to_string(),
                children: vnode_child_objs,
            };

            // Insert the dir into both the dir objects db and the dir hashes db
            log::debug!("putting dir {:?} into dir_hashes_db from new", dir);
            path_db::put(&self.dirs_db, dir_object.hash(), &dir_object)?;
            path_db::put(&self.dir_hashes_db, dir, &dir_object.hash().to_string())?;
        }
        Ok(())
    }

    fn create_tree_nodes_from_affected_dirs(
        &self,
        dirs: &mut Vec<PathBuf>,
        dir_map: HashMap<PathBuf, Vec<PathBuf>>,
        status: &StagedData,
        parent_commit_id: String,
        origin_path: &Path,
    ) -> Result<(), OxenError> {
        // Step 1: Sort affected dirs by their component count
        dirs.sort_by(|a, b| {
            let a_count = a.components().count();
            let b_count = b.components().count();
            b_count.cmp(&a_count)
        });

        let _schemas_map = self.map_schemas_to_parent_dirs()?;

        let parent_hash_db_dir =
            CommitEntryWriter::commit_dir_hash_db(&self.repository.path, &parent_commit_id);
        let parent_hash_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open(
            &rocksdb::Options::default(),
            dunce::simplified(&parent_hash_db_dir),
        )?;

        // TODONOW: streamline this - but for now, let's make a mapping of staged entries (as TreeObjects) to their parent directories

        let mut staged_entries_map: HashMap<PathBuf, Vec<TreeObjectChildWithStatus>> =
            HashMap::new();

        staged_entries_map =
            self.group_staged_files_to_dirs_with_status(staged_entries_map, status, origin_path)?;
        staged_entries_map =
            self.group_staged_schemas_to_dirs_with_status(staged_entries_map, status)?;
        staged_entries_map =
            self.group_staged_dirs_to_dirs_with_status(staged_entries_map, status)?;

        // Get affected dirs as a set
        let mut affected_dirs: HashSet<PathBuf> = HashSet::new();
        for dir in dirs.clone() {
            affected_dirs.insert(dir.clone());
        }
        log::debug!("affected_dirs are {:#?}", affected_dirs);

        // Now get the unaffected dirs: aka, iterate over dirs_map, and if the dir isn't in affected_dirs, add it to unaffected_dirs
        let mut unaffected_dirs: Vec<PathBuf> = Vec::new();
        for (dir, _) in dir_map.iter() {
            if !affected_dirs.contains(dir) {
                unaffected_dirs.push(dir.clone());
            }
        }

        // iterate over unaffected dirs and get their hashes from the parent commit and copy them over to the new commit
        for dir in unaffected_dirs {
            log::debug!("checking unaffected dir {:?}", dir);
            let prev_dir_hash: Option<String> = path_db::get_entry(&parent_hash_db, dir.clone())?;
            if let Some(prev_hash) = prev_dir_hash {
                log::debug!("Found some prev_dir_hash");
                path_db::put(&self.dir_hashes_db, dir.clone(), &prev_hash)?;
            } else {
                panic!("Somehow we have an unaffected dir that doesn't exist in the parent commit")
                // TODONOW error hadnling
            }
        }

        // These dirs are sorted by descending component count, so we can work bottom up
        for dir in dirs {
            self.process_affected_dir(dir.to_path_buf(), &parent_hash_db, &staged_entries_map)?;
        }
        Ok(())
    }

    fn write_file_objects_for_dir(&self, dir: PathBuf) -> Result<Vec<TreeObjectChild>, OxenError> {
        log::debug!("in write file objects from dir for dir {:?}", dir);
        let dir_entry_reader =
            LegacyCommitDirEntryReader::new(&self.repository, &self.commit.id, &dir)?;

        // Get all file children
        let files = dir_entry_reader.list_entries()?;

        // let mut file_children_map: HashMap<PathBuf, TreeObject> = HashMap::new();
        let mut file_children: Vec<TreeObjectChild> = Vec::new();
        // Process into TreeChildObject for TreeO
        for file in &files {
            let file_object = TreeObject::File {
                num_bytes: file.num_bytes,
                last_modified_seconds: file.last_modified_seconds,
                last_modified_nanoseconds: file.last_modified_nanoseconds,
                hash: file.hash.clone(),
            };
            path_db::put(&self.files_db, file_object.hash(), &file_object)?;

            let file_child = TreeObjectChild::File {
                path: file.path.clone(),
                hash: file.hash.clone(),
            };

            file_children.push(file_child);
        }

        Ok(file_children)
    }

    fn write_schema_objects_for_dir(
        &self,
        dir: PathBuf,
        schema_map: &HashMap<PathBuf, Vec<SchemaWithPath>>,
    ) -> Result<Vec<TreeObjectChild>, OxenError> {
        let schema_nodes: Vec<SchemaWithPath> = match schema_map.get(&dir) {
            Some(nodes) => nodes.clone(),
            None => Vec::new(),
        };

        let mut schema_objects_map: HashMap<PathBuf, TreeObject> = HashMap::new();
        for schema_node in schema_nodes {
            let schema_object = TreeObject::Schema {
                hash: schema_node.schema.hash.clone(),
            };
            path_db::put(&self.schemas_db, schema_object.hash(), &schema_object)?;
            // schema_object.write(&self.repository)?;
            schema_objects_map.insert(PathBuf::from(schema_node.path.clone()), schema_object);
        }

        let mut schema_children: Vec<TreeObjectChild> = Vec::new();
        for (path, schema_object) in schema_objects_map {
            let schema_child = TreeObjectChild::Schema {
                path,
                hash: schema_object.hash().to_string(),
            };
            schema_children.push(schema_child);
        }

        Ok(schema_children)
    }

    fn gather_dir_children_for_dir(
        &self,
        dir: PathBuf,
        dir_map: &HashMap<PathBuf, Vec<PathBuf>>,
    ) -> Result<Vec<TreeObjectChild>, OxenError> {
        // Dir nodes have already been written to the dir objects and dir hashes dbs
        let child_dirs = dir_map.get(&dir).unwrap();
        let mut dir_children: Vec<TreeObjectChild> = Vec::new();

        for path in child_dirs {
            let maybe_hash: Option<String> = path_db::get_entry(&self.dir_hashes_db, path)?;
            if let Some(hash) = maybe_hash {
                let dir_child = TreeObjectChild::Dir {
                    path: path.clone(),
                    hash,
                };
                dir_children.push(dir_child);
            }
        }

        Ok(dir_children)
    }

    // Traverse the tree, saving it locally to a tmp path that will be deleted after transmission to the server
    pub fn save_temp_commit_tree(&self) -> Result<PathBuf, OxenError> {
        // Get hash of this commit
        let temp_db_path = self
            .repository
            .path
            .join(TMP_DIR)
            .join("trees")
            .join(&self.commit.id);
        if !temp_db_path.exists() {
            std::fs::create_dir_all(&temp_db_path)?;
        }

        // Print whether or not that exists
        log::debug!(
            "Does the temp db path exist? {:?}",
            std::fs::metadata(&temp_db_path).is_ok()
        );

        let opts = db::opts::default();
        let temp_tree_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open(&opts, dunce::simplified(&temp_db_path))?;
        let commit_hash: &String = &self.commit.root_hash.clone().unwrap();

        let root_dir_node: TreeObject = path_db::get_entry(&self.dirs_db, commit_hash)?.unwrap(); // TODONOW: error handling here

        for child in root_dir_node.children() {
            self.r_save_temp_commit_tree(child, &temp_tree_db)?;
        }

        // Plug the root hash in here at "" to give the server a starting point for traversal.
        // Safe because an empty hash will not collide w/ any in xxhash
        path_db::put(&temp_tree_db, PathBuf::from(""), &root_dir_node)?;

        Ok(temp_db_path)
    }

    pub fn r_save_temp_commit_tree(
        &self,
        child_node: &TreeObjectChild,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        // Get parent node
        let node: TreeObject = match child_node {
            TreeObjectChild::Dir { .. } => {
                path_db::get_entry(&self.dirs_db, child_node.hash())?.unwrap()
            }
            TreeObjectChild::File { .. } => {
                path_db::get_entry(&self.files_db, child_node.hash())?.unwrap()
            }
            TreeObjectChild::Schema { .. } => {
                path_db::get_entry(&self.schemas_db, child_node.hash())?.unwrap()
            }
            TreeObjectChild::VNode { .. } => {
                path_db::get_entry(&self.vnodes_db, child_node.hash())?.unwrap()
            }
        };

        match node.clone() {
            TreeObject::Dir { children, hash, .. } => {
                // Add parent to db
                log::debug!("yo adding dir {:?} to db", node);
                path_db::put(db, hash, &node)?;
                for child in children {
                    self.r_save_temp_commit_tree(&child, db)?;
                }
                Ok(())
            }
            TreeObject::VNode { children, hash, .. } => {
                log::debug!("yo adding vnode {:?} to db", node);
                path_db::put(db, hash, &node)?;
                // let vnode_node: TreeObject = path_db::get_entry(&self.vnodes_db, &node.hash())?.unwrap();
                for child in children {
                    self.r_save_temp_commit_tree(&child, db)?;
                }
                Ok(())
            }
            TreeObject::File { hash, .. } | TreeObject::Schema { hash } => {
                log::debug!("yo adding leaf {:?} to db", node);
                path_db::put(db, hash, &node)?;
                // We're at a leaf node, so we're done
                Ok(())
            }
        }
    }

    pub fn new_temp_print_tree_db(&self) -> Result<(), OxenError> {
        // Get the hash of this commit
        let commit_hash = &self.commit.root_hash.clone().unwrap();
        // let commit_hash: String = path_db::get_entry(&self.dirs_, &self.commit.id)?.unwrap();

        // Get the root dir node (this hash)
        let root_dir_node: TreeObject = path_db::get_entry(&self.dirs_db, commit_hash)?.unwrap();

        log::debug!(
            "\n\nPRINTING TREE DB FOR COMMIT WITH ID {:?} MESSAGE {:?}",
            self.commit.id,
            self.commit.message
        );
        log::debug!("\n\nnew merkle root dir node is: {:?}", root_dir_node);

        // Get the children of the root dir node
        let root_dir_children: &Vec<TreeObjectChild> = root_dir_node.children();
        for child in root_dir_children {
            self.r_temp_print_tree_db(child)?;
        }

        log::debug!("and here's the entries");
        let commit_reader = CommitEntryReader::new(&self.repository, &self.commit)?;
        let entries = commit_reader.list_entries()?;

        log::debug!(
            "\n\nPRINTING ENTRIES FOR COMMIT WITH ID {:?} MESSAGE {:?}",
            self.commit.id,
            self.commit.message
        );
        for entry in entries {
            log::debug!("entry is {:?}", entry);
        }

        Ok(())
    }

    pub fn r_temp_print_tree_db(&self, child_node: &TreeObjectChild) -> Result<(), OxenError> {
        // Get parent node
        let node: TreeObject = match child_node {
            TreeObjectChild::Dir { .. } => {
                path_db::get_entry(&self.dirs_db, child_node.hash())?.unwrap()
            }
            TreeObjectChild::File { .. } => {
                path_db::get_entry(&self.files_db, child_node.hash())?.unwrap()
            }
            TreeObjectChild::Schema { .. } => {
                path_db::get_entry(&self.schemas_db, child_node.hash())?.unwrap()
            }
            TreeObjectChild::VNode { .. } => {
                path_db::get_entry(&self.vnodes_db, child_node.hash())?.unwrap()
            }
        };

        log::debug!("\n\nnew merkle node is: {:?}\n", node);

        match node {
            TreeObject::Dir { children, .. } => {
                // let dir_node: TreeObject = path_db::get_entry(&self.dirs_db, hash)?.unwrap();
                for child in children {
                    self.r_temp_print_tree_db(&child)?;
                }
                Ok(())
            }
            TreeObject::VNode { children, .. } => {
                // let vnode_node: TreeObject = path_db::get_entry(&self.vnodes_db, &node.hash())?.unwrap();
                for child in children {
                    self.r_temp_print_tree_db(&child)?;
                }
                Ok(())
            }
            _ => {
                // We're at a leaf node, so we're done
                Ok(())
            }
        }
    }

    pub fn new_temp_print_tree_db_all(&self) {
        for db in &[
            &self.files_db,
            &self.schemas_db,
            &self.dirs_db,
            &self.vnodes_db,
        ] {
            let iter = db.iterator(rocksdb::IteratorMode::Start);
            for item in iter {
                match item {
                    Ok((key_bytes, value_bytes)) => {
                        match String::from_utf8(key_bytes.to_vec()) {
                            Ok(key_str) => {
                                let key_path = PathBuf::from(key_str);

                                // Attempting to deserialize the value into TreeNode
                                let deserialized_value: Result<TreeObject, _> =
                                    serde_json::from_slice(&value_bytes);
                                match deserialized_value {
                                    Ok(tree_object) => {
                                        log::debug!(
                                            "\n\nnew tree_db entry: {:?} -> {:?}\n\n",
                                            key_path,
                                            tree_object
                                        );
                                    }
                                    Err(e) => {
                                        log::error!("new error deserializing value: {:?}", e);
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

        // // Track entries in commit
        for (dir, files) in grouped.iter_mut() {
            // Write entries per dir
            let entry_writer = CommitDirEntryWriter::new(&self.repository, &self.commit.id, dir)?;
            path_db::put(&self.dir_db, dir, &0)?;

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

        let object_reader = ObjectDBReader::new(&self.repository)?;
        // Track dirs in commit
        for (_path, staged_dirs) in staged_data.staged_dirs.paths.iter() {
            for staged_dir in staged_dirs.iter() {
                log::debug!(
                    "commit_staged_entries_with_prog adding dir {:?} -> {:?}",
                    staged_dir.path,
                    staged_dir.status
                );
                if staged_dir.status == StagedEntryStatus::Removed {
                    let entry_reader = CommitDirEntryReader::new(
                        &self.repository,
                        &self.commit.id,
                        &staged_dir.path,
                        object_reader.clone(),
                    )?;
                    let num_entries = entry_reader.num_entries();
                    log::debug!(
                        "got num_entries {:?} for dir {:?}",
                        num_entries,
                        staged_dir.path
                    );
                    // if num_entries == 0 {
                    //     path_db::delete(&self.dir_db, &staged_dir.path)?;
                    //     continue;
                    // }
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
                    Ok(_) => {
                        log::debug!(
                            "in the status adder adding full path {:?}",
                            origin_path.join(path)
                        );
                    }
                    Err(err) => {
                        let err = format!("Failed to ADD file: {err}");
                        panic!("{}", err)
                    }
                }
            }
        }
    }

    // Functions below here are all tree-y and should probably be moved to their own module
    fn map_schemas_to_parent_dirs(
        &self,
    ) -> Result<HashMap<PathBuf, Vec<SchemaWithPath>>, OxenError> {
        let schema_reader = SchemaReader::new(&self.repository, &self.commit.id)?;
        let schemas = schema_reader.list_schemas()?;

        let mut schema_map: HashMap<PathBuf, Vec<SchemaWithPath>> = HashMap::new();
        for (path, schema) in schemas {
            let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
            let schema_with_path = SchemaWithPath {
                path: PathBuf::from(SCHEMAS_TREE_PREFIX)
                    .join(path.clone())
                    .to_string_lossy()
                    .to_string(),
                schema,
            };
            schema_map.entry(parent).or_default().push(schema_with_path);
        }

        Ok(schema_map)
    }

    fn group_staged_files_to_dirs_with_status(
        &self,
        _staged_map: HashMap<PathBuf, Vec<TreeObjectChildWithStatus>>,
        staged_data: &StagedData,
        origin_path: &Path,
    ) -> Result<HashMap<PathBuf, Vec<TreeObjectChildWithStatus>>, OxenError> {
        let mut staged_entries_map: HashMap<PathBuf, Vec<TreeObjectChildWithStatus>> =
            HashMap::new();

        // Get parent dir for this staged file

        // TODONOW: m,aybe make a FROM method here?
        // Collect staged FILES into a map of dir -> TreeChildWithStatus
        for (path, entry) in staged_data.staged_files.iter() {
            let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
            // Add commit entry metadata to this file node
            let file_object = match entry.status {
                StagedEntryStatus::Added | StagedEntryStatus::Modified => {
                    let full_path = origin_path.join(path);
                    let metadata = fs::metadata(&full_path).unwrap();
                    let mtime = FileTime::from_last_modification_time(&metadata);

                    // Re-hash in case modified after adding
                    let hash = util::hasher::hash_file_contents(&full_path)?;

                    let file_res = TreeObject::File {
                        num_bytes: metadata.len(),
                        last_modified_seconds: mtime.unix_seconds(),
                        last_modified_nanoseconds: mtime.nanoseconds(),
                        hash,
                    };

                    // Put the full file object into the files objects db by hash
                    path_db::put(&self.files_db, file_res.hash(), &file_res)?;
                    file_res
                }
                StagedEntryStatus::Removed => {
                    // Return a dummy entry with valid hash - only using this to remove the file from
                    // all its parents, does not need insertion into db
                    TreeObject::File {
                        num_bytes: 0,
                        last_modified_seconds: 0,
                        last_modified_nanoseconds: 0,
                        hash: entry.hash.clone(),
                    }
                }
            };

            // Combine object with status so we know how to handle it in its parents later
            let file_child_with_status = TreeObjectChildWithStatus {
                child: TreeObjectChild::File {
                    path: path.to_path_buf(),
                    hash: file_object.hash().to_string(),
                },
                status: entry.status.clone(),
            };

            staged_entries_map
                .entry(parent)
                .or_default()
                .push(file_child_with_status);
        }

        Ok(staged_entries_map)
    }

    fn group_staged_schemas_to_dirs_with_status(
        &self,
        mut staged_map: HashMap<PathBuf, Vec<TreeObjectChildWithStatus>>,
        staged_data: &StagedData,
    ) -> Result<HashMap<PathBuf, Vec<TreeObjectChildWithStatus>>, OxenError> {
        log::debug!(
            "staged schemas for commit {:#?} are {:#?}",
            self.commit,
            staged_data
        );
        for (path, staged_schema) in staged_data.staged_schemas.iter() {
            let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
            log::debug!("parent dir for schema {:?} is {:?}", path, parent);
            let schema_child_with_status =
                TreeObjectChildWithStatus::from_staged_schema(path.to_path_buf(), staged_schema);

            let schema_object = TreeObject::Schema {
                hash: staged_schema.schema.hash.clone(),
            };

            log::debug!("putting schema {:?} into schemas_db", schema_object);
            path_db::put(&self.schemas_db, schema_object.hash(), &schema_object)?;
            staged_map
                .entry(parent)
                .or_default()
                .push(schema_child_with_status);
        }
        log::debug!("...giving us schema staged map {:#?}", staged_map);
        Ok(staged_map)
    }

    fn group_staged_dirs_to_dirs_with_status(
        &self,
        mut staged_map: HashMap<PathBuf, Vec<TreeObjectChildWithStatus>>,
        staged_data: &StagedData,
    ) -> Result<HashMap<PathBuf, Vec<TreeObjectChildWithStatus>>, OxenError> {
        log::debug!(
            "staged_dirs are {:#?} for commit {:#?}",
            staged_data.staged_dirs,
            self.commit
        );
        for (_path, staged_dirs) in staged_data.staged_dirs.paths.iter() {
            for dir_stats in staged_dirs.iter() {
                let parent = dir_stats
                    .path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_path_buf();
                let dir_child_with_status = TreeObjectChildWithStatus::from_staged_dir(dir_stats);
                staged_map
                    .entry(parent)
                    .or_default()
                    .push(dir_child_with_status);
            }
        }

        Ok(staged_map)
    }

    fn get_affected_vnodes(
        &self,
        new_dir_children: &Vec<TreeObjectChildWithStatus>,
    ) -> Result<HashMap<String, Vec<TreeObjectChildWithStatus>>, OxenError> {
        // For each new or modified child in this dir, associate it with its parent vnode
        // by hashing its path and getting the hash prefix.
        let mut affected_vnodes: HashMap<String, Vec<TreeObjectChildWithStatus>> = HashMap::new();

        for child_with_status in new_dir_children {
            // If we have a dir, we need to get the updated hash for it
            // which we've previously saved in the dir_hashes_db since this operates bottom-up

            // We can't do this for removed dirs since they're not reinserted into the dir dbs map
            let child_object = match &child_with_status.child {
                TreeObjectChild::Dir { path, .. } => {
                    if child_with_status.status != StagedEntryStatus::Removed {
                        let dir_hash: String =
                            path_db::get_entry(&self.dir_hashes_db, path)?.unwrap();
                        TreeObjectChild::Dir {
                            path: path.clone(),
                            hash: dir_hash,
                        }
                    } else {
                        child_with_status.child.clone()
                    }
                }
                _ => child_with_status.child.clone(),
            };

            let path_hash = util::hasher::hash_pathbuf(child_object.path());
            let prefix = path_hash[0..2].to_string();

            let updated_child_with_status = TreeObjectChildWithStatus {
                child: child_object,
                status: child_with_status.status.clone(),
            };

            affected_vnodes
                .entry(prefix)
                .or_default()
                .push(updated_child_with_status);
        }

        Ok(affected_vnodes)
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::core::index::CommitEntryReader;
    use crate::error::OxenError;
    use crate::test;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_merkle_two_files_same_hash() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let p1 = "hi.txt";
            let p2 = "bye.txt";
            let path_1 = local_repo.path.join(&p1);
            let path_2 = local_repo.path.join(&p2);

            let common_contents = "the same file";

            test::write_txt_file_to_path(&path_1, common_contents)?;
            test::write_txt_file_to_path(&path_2, common_contents)?;

            command::add(&local_repo, &path_1)?;
            command::add(&local_repo, &path_2)?;

            let status = command::status(&local_repo)?;

            log::debug!("staged files here are {:?}", status.staged_files);

            assert_eq!(status.staged_files.len(), 2);

            assert!(status.staged_files.contains_key(&PathBuf::from(p1)));
            assert!(status.staged_files.contains_key(&PathBuf::from(p2)));

            let commit = command::commit(&local_repo, "add two files")?;

            let commit_entry_reader = CommitEntryReader::new(&local_repo, &commit)?;

            // List all the files
            let files = commit_entry_reader.list_entries()?;

            assert!(commit_entry_reader.has_file(&PathBuf::from(p1)));
            assert!(commit_entry_reader.has_file(&PathBuf::from(p2)));

            Ok(())
        })
        .await
    }
}
