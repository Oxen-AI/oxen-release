use crate::constants::{FILES_DIR, HISTORY_DIR, SCHEMAS_DIR, SCHEMAS_TREE_PREFIX};
use crate::core::db;
use crate::core::db::key_val::path_db;
use crate::core::db::key_val::tree_db::{TreeObject, TreeObjectChild};

use crate::core::v0_10_0::index::CommitEntryWriter;
use crate::error::OxenError;
use crate::model::entries::commit_entry::SchemaEntry;
use crate::model::Schema;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use crate::model::LocalRepository;

use super::{CommitReader, ObjectDBReader};

pub struct SchemaReader {
    object_reader: Arc<ObjectDBReader>,
    dir_hashes_db: DBWithThreadMode<MultiThreaded>,
    repository: LocalRepository,
    commit_id: String,
}

impl SchemaReader {
    pub fn schemas_db_dir(repo: &LocalRepository, commit_id: &str) -> PathBuf {
        // .oxen/history/COMMIT_ID/schemas/schemas
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(SCHEMAS_DIR) // double schemas/schemas is intentional because we have multiple dirs at this level
            .join(SCHEMAS_DIR)
    }

    pub fn schema_files_db_dir(repo: &LocalRepository, commit_id: &str) -> PathBuf {
        // .oxen/history/COMMIT_ID/schemas/files
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(SCHEMAS_DIR)
            .join(FILES_DIR)
    }

    pub fn new(repository: &LocalRepository, commit_id: &str) -> Result<SchemaReader, OxenError> {
        let dir_hashes_db_path = CommitEntryWriter::commit_dir_hash_db(&repository.path, commit_id);

        let opts = db::key_val::opts::default();

        if !dir_hashes_db_path.exists() {
            log::debug!("creating dir hashes db at path {:?}", dir_hashes_db_path);
            std::fs::create_dir_all(&dir_hashes_db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&dir_hashes_db_path))?;
        } else {
            log::debug!("dir hashes db exists at path {:?}", dir_hashes_db_path)
        }

        let object_reader = ObjectDBReader::new(repository, commit_id)?;

        Ok(SchemaReader {
            dir_hashes_db: DBWithThreadMode::open_for_read_only(&opts, &dir_hashes_db_path, false)?,
            object_reader,
            repository: repository.clone(),
            commit_id: commit_id.to_owned(),
        })
    }

    pub fn new_from_head(repository: &LocalRepository) -> Result<SchemaReader, OxenError> {
        let commit_reader = CommitReader::new(repository)?;
        let commit = commit_reader.head_commit()?;
        SchemaReader::new(repository, &commit.id)
    }

    pub fn get_schema_for_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Option<Schema>, OxenError> {
        log::debug!("in get_schema_for_file path {:?}", path.as_ref());
        let schema_path = Path::new(SCHEMAS_TREE_PREFIX).join(&path);
        let path_parent = path.as_ref().parent().unwrap_or(Path::new(""));

        // Get the parent dir hash in which the schema is stored
        let parent_dir_hash: Option<String> = path_db::get_entry(
            &self.dir_hashes_db,
            path_parent.to_str().unwrap().replace('\\', "/"),
        )?;

        if parent_dir_hash.is_none() {
            return Ok(None);
        }

        let parent_dir_hash = parent_dir_hash.unwrap();

        let parent_dir_obj: TreeObject = self.object_reader.get_dir(&parent_dir_hash)?.unwrap();

        // Get the hash of the schema's path
        let full_path_str = schema_path.to_str().unwrap().replace('\\', "/");
        let schema_path_hash_prefix = util::hasher::hash_path_name(full_path_str)[0..2].to_string();

        // Binary search for the appropriate vnode
        let vnode_child: Option<TreeObjectChild> = parent_dir_obj
            .binary_search_on_path(&PathBuf::from(schema_path_hash_prefix.clone()))?;

        if vnode_child.is_none() {
            return Ok(None);
        }

        let vnode_child = vnode_child.unwrap();
        let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();

        log::debug!("got vnode");
        log::debug!("here's the vnode {:?}", vnode);
        // Binary search for the appropriate schema
        let schema_child: Option<TreeObjectChild> =
            vnode.binary_search_on_path(&PathBuf::from(SCHEMAS_TREE_PREFIX).join(path))?;

        if schema_child.is_none() {
            return Ok(None);
        }

        let schema_child = schema_child.unwrap();
        log::debug!("got this schema child {:?}", schema_child);

        // Get the schema from the versions directory by hash
        let version_path = util::fs::version_path_from_schema_hash(
            &self.repository.path,
            schema_child.hash().to_string(),
        );

        log::debug!("got version path {:?}", version_path);

        let schema: Result<Schema, serde_json::Error> =
            serde_json::from_reader(std::fs::File::open(version_path)?);

        log::debug!("get_schema_for_file() got schema {:?}", schema);

        match schema {
            Ok(schema) => Ok(Some(schema)),
            Err(_) => Ok(None),
        }
    }

    pub fn list_schemas(&self) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        log::debug!("calling list schemas");
        let root_hash: String = path_db::get_entry(&self.dir_hashes_db, "")?.unwrap();
        // log::debug!("list_schemas got root hash {:?}", root_hash);
        let root_node: TreeObject = self.object_reader.get_dir(&root_hash)?.unwrap();
        // log::debug!("list_schemas got root node {:?}", root_node);
        let mut path_vals: HashMap<PathBuf, Schema> = HashMap::new();

        self.r_list_schemas(root_node, &mut path_vals)?;

        Ok(path_vals)
    }

    fn r_list_schemas(
        &self,
        dir_node: TreeObject,
        path_vals: &mut HashMap<PathBuf, Schema>,
    ) -> Result<(), OxenError> {
        // log::debug!("calling r_list_schemas on dir_node {:?}", dir_node);
        for vnode in dir_node.children() {
            let vnode = self.object_reader.get_vnode(vnode.hash())?.unwrap();
            for child in vnode.children() {
                // log::debug!("got vnode child {:?}", child);
                match child {
                    TreeObjectChild::Dir { hash, .. } => {
                        let dir_node = self.object_reader.get_dir(hash)?.unwrap();
                        self.r_list_schemas(dir_node, path_vals)?;
                    }
                    TreeObjectChild::Schema { path, hash, .. } => {
                        let stripped_path = path.strip_prefix(SCHEMAS_TREE_PREFIX).unwrap();
                        // log::debug!("got stripped path {:?} and hash {:?}", stripped_path, hash);
                        let found_schema = self.get_schema_by_hash(hash)?;
                        // log::debug!("got found schema {:?}", found_schema);
                        path_vals.insert(stripped_path.to_path_buf(), found_schema);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    pub fn list_schema_entries(&self) -> Result<Vec<SchemaEntry>, OxenError> {
        // Root hash is stored on the commit though
        let commit_reader = CommitReader::new(&self.repository)?;
        let commit =
            commit_reader
                .get_commit_by_id(&self.commit_id)?
                .ok_or(OxenError::basic_str(format!(
                    "Could not find commit {}",
                    self.commit_id
                )))?;

        let root_hash = commit
            .root_hash
            .ok_or(format!("Root hash not found for commit {}", self.commit_id))?;

        let root_node: TreeObject =
            self.object_reader
                .get_dir(&root_hash)?
                .ok_or(OxenError::basic_str(
                    "Could not find root node in object db",
                ))?;

        let mut entries: Vec<SchemaEntry> = Vec::new();

        self.r_list_schema_entries(root_node, &mut entries)?;

        Ok(entries)
    }

    fn r_list_schema_entries(
        &self,
        dir_node: TreeObject,
        entries: &mut Vec<SchemaEntry>,
    ) -> Result<(), OxenError> {
        for vnode in dir_node.children() {
            let vnode = self.object_reader.get_vnode(vnode.hash())?.unwrap();
            for child in vnode.children() {
                match child {
                    TreeObjectChild::Dir { hash, .. } => {
                        let dir_node = self.object_reader.get_dir(hash)?.unwrap();
                        self.r_list_schema_entries(dir_node, entries)?;
                    }
                    TreeObjectChild::Schema { path, hash, .. } => {
                        let stripped_path = path.strip_prefix(SCHEMAS_TREE_PREFIX).unwrap();
                        log::debug!("got stripped path {:?} and hash {:?}", stripped_path, hash);
                        let found_schema = self.object_reader.get_schema(hash)?.unwrap();
                        log::debug!("got found schema {:?}", found_schema);
                        let found_entry = SchemaEntry {
                            commit_id: self.commit_id.clone(),
                            path: stripped_path.to_path_buf(),
                            hash: found_schema.hash().clone(),
                            num_bytes: found_schema.num_bytes(),
                        };
                        entries.push(found_entry);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    pub fn list_schemas_for_ref(
        &self,
        schema_ref: impl AsRef<str>,
    ) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        let all_schemas = self.list_schemas()?;

        let mut found_schemas: HashMap<PathBuf, Schema> = HashMap::new();

        for (path, schema) in all_schemas.iter() {
            if path.to_string_lossy() == schema_ref.as_ref()
                || schema.hash == schema_ref.as_ref()
                || schema.name == Some(schema_ref.as_ref().to_string())
            {
                found_schemas.insert(path.clone(), schema.clone());
            }
        }
        Ok(found_schemas)
    }

    fn get_schema_by_hash(&self, hash: &str) -> Result<Schema, OxenError> {
        let version_path =
            util::fs::version_path_from_schema_hash(&self.repository.path, hash.to_string());
        let schema = serde_json::from_reader(std::fs::File::open(version_path)?)?;
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::core::v0_10_0::index::SchemaReader;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;

    #[test]
    fn test_schema_reader_list_empty_schemas() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let history = repositories::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;
            let schemas = schema_reader.list_schemas()?;

            assert_eq!(schemas.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_schema_reader_list_committed_schemas() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = repositories::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;
            let schemas = schema_reader.list_schemas()?;

            for (k, v) in schemas.iter() {
                println!("{}: {}", k.to_string_lossy(), v.hash);
            }

            assert_eq!(schemas.len(), 7);
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/bounding_box.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/one_shot.csv")));
            assert!(
                schemas.contains_key(&PathBuf::from("nlp/classification/annotations/train.tsv"))
            );
            assert!(schemas.contains_key(&PathBuf::from("large_files/test.csv")));
            assert!(schemas.contains_key(&PathBuf::from("nlp/classification/annotations/test.tsv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/two_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/test/annotations.csv")));

            Ok(())
        })
    }

    #[test]
    fn test_schema_reader_get_schema_ref_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = repositories::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;

            let schema_ref = &PathBuf::from("annotations")
                .join("train")
                .join("bounding_box.csv")
                .to_string_lossy()
                .to_string();
            let schemas = schema_reader.list_schemas_for_ref(schema_ref)?;

            assert_eq!(schemas.len(), 1);
            assert!(schemas.contains_key(&PathBuf::from(schema_ref)));

            Ok(())
        })
    }

    #[test]
    fn test_schema_reader_get_schema_ref_by_name() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = repositories::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;

            let schema_ref = "bounding_box";
            let schemas = schema_reader.list_schemas_for_ref(schema_ref)?;

            assert_eq!(schemas.len(), 4);
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/bounding_box.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/one_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/two_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/test/annotations.csv")));

            Ok(())
        })
    }

    #[test]
    fn test_schema_reader_get_schema_ref_by_hash() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = repositories::commits::list(&repo)?;
            let last_commit = history.first().unwrap();
            let schema_reader = SchemaReader::new(&repo, &last_commit.id)?;

            let schema_ref = "b821946753334c083124fd563377d795";
            let schemas = schema_reader.list_schemas_for_ref(schema_ref)?;

            for (k, v) in schemas.iter() {
                println!("{}: {}", k.to_string_lossy(), v);
            }

            assert_eq!(schemas.len(), 4);
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/bounding_box.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/one_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/train/two_shot.csv")));
            assert!(schemas.contains_key(&PathBuf::from("annotations/test/annotations.csv")));

            Ok(())
        })
    }
}
