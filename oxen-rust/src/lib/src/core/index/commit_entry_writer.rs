use crate::api;
use crate::constants::{self, DEFAULT_BRANCH_NAME, HISTORY_DIR, SCHEMAS_TREE_PREFIX, VERSIONS_DIR};
use crate::core::db;
use crate::core::db::tree_db::{TreeChild, TreeNode};
use crate::core::db::{kv_db, path_db};
use crate::core::index::{CommitDirEntryWriter, RefWriter, SchemaReader, SchemaWriter};
use crate::error::OxenError;
use crate::model::schema::staged_schema::StagedSchemaStatus;
use crate::model::{
    Commit, CommitEntry, LocalRepository, StagedData, StagedEntry, StagedEntryStatus, StagedSchema,
};
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};

use filetime::FileTime;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
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

            // Copy parent entries
            let reader = CommitEntryReader::new(repo, &parent_commit)?;
            self.write_entries_from_reader(&reader)?;

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

        // Re-hash for issues w/ adding
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
        staged_schemas: &HashMap<PathBuf, StagedSchema>,
    ) -> Result<(), OxenError> {
        log::debug!("commit_schemas got {} schemas", staged_schemas.len());

        let schema_writer = SchemaWriter::new(&self.repository, &commit.id)?;
        for (path, staged_schema) in staged_schemas.iter() {
            if staged_schema.status == StagedSchemaStatus::Removed {
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

    fn construct_commit_merkle_tree(&self, staged_data: &StagedData) -> Result<(), OxenError> {
        if self.commit.parent_ids.len() == 1 {
            let prev_tree_path = CommitEntryWriter::commit_tree_db(
                &self.repository.path,
                &self.commit.parent_ids[0],
            );
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
    ) -> Result<(), OxenError> {
        let parent_tree = TreeDBReader::new(&self.repository, &self.commit.parent_ids[0])?;

        // Step 1: Copy over all entries from parent tree to new tree
        for result in parent_tree.db.iterator(IteratorMode::Start) {
            match result {
                Ok((key, value)) => {
                    let path_str = String::from_utf8(key.to_vec()).unwrap();
                    let node = serde_json::from_slice::<TreeNode>(&value).unwrap();

                    let path = PathBuf::from(path_str);
                    path_db::put(&self.tree_db, &path, &node)?;
                }
                _ => return Err(OxenError::basic_str("Could not iterate over db values")),
            }
        }

        // Step 2: Get all dirs and put into hash set representing the commit tree
        let dir_paths = path_db::list_paths(&self.dir_db, &PathBuf::from(""))?;

        // Build a map of dirs to children for lookup in tree construction
        let mut dir_map: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for dir in &dir_paths {
            let parent = dir.parent().unwrap_or(Path::new("")).to_path_buf();
            // Insert the dir itself
            dir_map.entry(dir.to_path_buf()).or_default();
            if &parent != dir {
                dir_map.entry(parent).or_default().push(dir.to_path_buf());
            }
        }

        // Step 2: Parse StagedEntry paths to save time by only recomputing dirs that saw changes in this commit
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

        // Step 3: Delete path db entries for files that were removed in this commit
        for (path, entry) in staged_data.staged_files.iter() {
            if entry.status == StagedEntryStatus::Removed {
                log::debug!("construct_merkle_tree_from_parent removing {:?}", path);
                path_db::delete(&self.tree_db, path)?;
                // Get the parent path
                let parent = path.parent().unwrap().to_path_buf();
                // If the parent isn't in the `dirs_map`, it's been deleted in this commit - delete it from the tree
                if !dir_map.contains_key(&parent) {
                    path_db::delete(&self.tree_db, &parent)?;
                }
            }
        }

        // Same with schemas
        for (path, staged_schema) in staged_data.staged_schemas.iter() {
            if staged_schema.status == StagedSchemaStatus::Removed {
                let schema_tree_path = PathBuf::from(SCHEMAS_TREE_PREFIX).join(path.clone());
                log::debug!(
                    "construct_merkle_tree_from_parent removing {:?}",
                    schema_tree_path
                );
                path_db::delete(&self.tree_db, schema_tree_path)?;
                let parent = path.parent().unwrap().to_path_buf();
                if !dir_map.contains_key(&parent) {
                    path_db::delete(&self.tree_db, &parent)?;
                }
            }
        }

        // Always need to recompute the root dir, but can get missed by this logic
        dirs_to_recompute.insert(PathBuf::from(""));
        let mut modified_dirs_vec: Vec<PathBuf> = dirs_to_recompute.into_iter().collect();

        self.create_tree_nodes_from_dirs(&mut modified_dirs_vec, dir_map)
    }

    pub fn construct_merkle_tree_new(&self) -> Result<(), OxenError> {
        // Operates on ALL directories to make a merkle tree from scratch
        let mut dir_paths = path_db::list_paths(&self.dir_db, &PathBuf::from(""))?;

        // Build a map of dir to children
        let mut dir_map: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for dir in &dir_paths {
            let parent = dir.parent().unwrap_or(Path::new("")).to_path_buf();
            dir_map.entry(dir.to_path_buf()).or_default();
            if &parent != dir {
                dir_map.entry(parent).or_default().push(dir.to_path_buf());
            }
        }
        self.create_tree_nodes_from_dirs(&mut dir_paths, dir_map)
    }

    fn entry_to_treechild(entry: &CommitEntry) -> TreeChild {
        TreeChild::File {
            path: entry.path.clone(),
            hash: entry.hash.clone(),
        }
    }

    fn entry_to_treenode(entry: &CommitEntry) -> TreeNode {
        TreeNode::File {
            path: entry.path.clone(),
            hash: entry.hash.clone(),
        }
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
        let mut schema_map: HashMap<PathBuf, Vec<TreeChild>> = HashMap::new();
        for (path, schema) in schemas {
            let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
            let schema_child = TreeChild::Schema {
                path: PathBuf::from(SCHEMAS_TREE_PREFIX).join(path.clone()),
                hash: schema.hash.clone(),
            };
            schema_map.entry(parent).or_default().push(schema_child);
        }

        for dir in dirs {
            let dir_entry_reader =
                CommitDirEntryReader::new(&self.repository, &self.commit.id, dir)?;

            // Get all file children
            let children_entries = dir_entry_reader.list_entries()?;
            let child_entry_nodes: Vec<TreeChild> = children_entries
                .iter()
                .map(CommitEntryWriter::entry_to_treechild)
                .collect::<Vec<_>>();
            let child_tree_nodes: Vec<TreeNode> = children_entries
                .iter()
                .map(CommitEntryWriter::entry_to_treenode)
                .collect::<Vec<_>>();

            // Get all schema children
            let schema_entry_nodes: Vec<TreeChild> = match schema_map.get(dir) {
                Some(nodes) => nodes.clone(),
                None => Vec::new(),
            };

            // Get all directory children. Since we work bottom up, these are already updated in tree
            let children_dirs = dir_map.get(dir).unwrap();
            let mut dir_entry_nodes: Vec<TreeChild> = Vec::new();

            // Collect dir children as treechildren
            for path in children_dirs {
                if let Some(entry) = path_db::get_entry(&self.tree_db, path)? {
                    dir_entry_nodes.push(entry)
                }
            }

            // Insert updated file nodes into the db
            for file_child in &child_tree_nodes {
                let file_node = TreeNode::File {
                    path: file_child.path().to_path_buf(),
                    hash: file_child.hash().to_string(),
                };
                path_db::put(&self.tree_db, file_node.path(), &file_node)?;
            }

            // Insert updated schema nodes into the db
            for schema_child in &schema_entry_nodes {
                let schema_node = TreeNode::Schema {
                    path: schema_child.path().to_path_buf(),
                    hash: schema_child.hash().to_string(),
                };
                path_db::put(&self.tree_db, schema_node.path(), &schema_node)?;
            }

            // Get a combined, lexically sorted list of all children
            let mut all_children: Vec<TreeChild> = child_entry_nodes;
            all_children.extend(dir_entry_nodes);
            all_children.extend(schema_entry_nodes);

            // Lexically sort the children
            all_children.sort_by(|a, b| a.path().cmp(b.path()));

            let node_hash = util::hasher::compute_subtree_hash(&all_children);

            // Update or a Directory TreeNode of the children in the pathdb
            let dir_node = TreeNode::Directory {
                path: dir.to_path_buf(),
                children: all_children,
                hash: node_hash.to_string(),
            };

            if dir_node.children()?.is_empty() {
                path_db::delete(&self.tree_db, dir)?;
            } else {
                path_db::put(&self.tree_db, dir, &dir_node)?;
            }
        }

        // If there's no root node in the db (if it was deleted), recreate it empty
        // to avoid tree traversal errors on server - TODONOW - should be a better way

        let maybe_root: Option<TreeNode> = path_db::get_entry(&self.tree_db, PathBuf::from(""))?;
        if maybe_root.is_none() {
            log::debug!("maybe_root is none!");
            let empty_root = TreeNode::Directory {
                path: PathBuf::from(""),
                children: Vec::new(),
                hash: util::hasher::compute_subtree_hash(&Vec::new()),
            };
            path_db::put(&self.tree_db, PathBuf::from(""), &empty_root)?;
        }

        Ok(())
    }

    // TODONOW delete after testing
    pub fn temp_print_tree_db(&self) {
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

        // Track entries in commit
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
    use crate::command;
    use crate::constants::SCHEMAS_TREE_PREFIX;
    use crate::core::index::TreeDBReader;
    use crate::error::OxenError;
    use crate::test;
    use crate::util;
    use serde_json::json;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_merkle_tree_tracks_schemas() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
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
        })
        .await
    }

    #[tokio::test]
    async fn test_merkle_tree_deletes_schemas() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("large_files");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_200k_csv();
            util::fs::copy(from_file, &csv_file)?;

            command::add(&local_repo, &csv_file)?;

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

            // Second commit merkle db
            let second_merkle_reader = TreeDBReader::new(&local_repo, &second_commit.id)?;
            let merkle_schema_path = PathBuf::from(SCHEMAS_TREE_PREFIX).join(schema_ref);

            // The schema should be in the merkle tree
            let schema_node = second_merkle_reader.get_entry(&merkle_schema_path)?;
            assert!(schema_node.is_some());

            // Delete the file for the schema, add, recommit.
            // TODONOW: add a status checker here on commit
            std::fs::remove_file(&csv_file)?;
            command::add(&local_repo, &csv_file)?;
            let third_commit = command::commit(&local_repo, "delete test.csv")?;

            let merkle_schema_path = PathBuf::from(SCHEMAS_TREE_PREFIX).join(schema_ref);
            let third_merkle_reader = TreeDBReader::new(&local_repo, &third_commit.id)?;
            let schema_node = third_merkle_reader.get_entry(&merkle_schema_path)?;
            assert!(schema_node.is_none());

            Ok(())
        })
        .await
    }
}
