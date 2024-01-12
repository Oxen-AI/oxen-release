use crate::api;
use crate::constants::{self, DEFAULT_BRANCH_NAME, HISTORY_DIR, SCHEMAS_TREE_PREFIX, VERSIONS_DIR};
use crate::core::db;
use crate::core::db::tree_db::{TreeObject, TreeObjectChild, TreeObjectChildWithStatus};
use crate::core::db::{kv_db, path_db};
use crate::core::index::{CommitDirEntryWriter, RefWriter, SchemaReader, SchemaWriter};
use crate::error::OxenError;
use crate::model::schema::Schema;
use crate::model::{
    Commit, CommitEntry, LocalRepository, StagedData, StagedEntry, StagedEntryStatus,
};
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::view::schema::SchemaWithPath;

use filetime::FileTime;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::{versioner, CommitDirEntryReader, CommitEntryReader};

pub struct CommitEntryWriter {
    pub repository: LocalRepository,
    pub dir_db: DBWithThreadMode<MultiThreaded>,
    pub dir_hashes_db: DBWithThreadMode<MultiThreaded>,
    pub files_db: DBWithThreadMode<MultiThreaded>,
    pub schemas_db: DBWithThreadMode<MultiThreaded>,
    pub dirs_db: DBWithThreadMode<MultiThreaded>,
    pub vnodes_db: DBWithThreadMode<MultiThreaded>,
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
        let grouped = self.group_staged_files_to_dirs(&staged_data.staged_files);
        log::debug!(
            "commit_staged_entries_with_prog got groups {}",
            grouped.len()
        );

        // Track entries in commit
        for (dir, files) in grouped.iter() {
            // Write entries per dir
            let entry_writer = CommitDirEntryWriter::new(&self.repository, &self.commit.id, dir)?;
            path_db::put(&self.dir_db, dir, &0)?;

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
    pub fn construct_merkle_tree_from_legacy_commit(
        &self,
        _origin_path: &Path,
    ) -> Result<(), OxenError> {
        // Operate on all dirs to make the tree from scratch...
        let mut dir_paths = path_db::list_paths(&self.dir_db, &PathBuf::from(""))?;

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

    // For migration only, can remove once old format is fully deprecated
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
            // Backup the schema to the versions dir as a part of the migration
            versioner::backup_schema(&self.repository, &schema)?;
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

    fn write_file_objects_for_dir(&self, dir: PathBuf) -> Result<Vec<TreeObjectChild>, OxenError> {
        log::debug!("in write file objects from dir for dir {:?}", dir);
        let dir_entry_reader = CommitDirEntryReader::new(&self.repository, &self.commit.id, &dir)?;

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
                num_bytes: schema_node.schema.num_bytes(),
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
}

#[cfg(test)]
mod tests {}
