//! # oxen schemas
//!
//! Interact with schemas
//!

use std::collections::HashMap;
use std::path::PathBuf;

use rmp_serde::Serializer;
use rocksdb::DBWithThreadMode;
use rocksdb::MultiThreaded;
use serde::Serialize;

use crate::constants;
use crate::core::db;
use crate::core::v0_10_0::index::SchemaReader;
use crate::core::v0_10_0::index::Stager;

use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::error::OxenError;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::StagedEntryStatus;
use crate::model::{Commit, LocalRepository, Schema};
use crate::repositories;
use crate::util;

use std::path::Path;

pub fn list(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let tree = CommitMerkleTree::from_commit(repo, commit)?;
    let mut schemas = HashMap::new();
    r_list_schemas(repo, tree.root, PathBuf::new(), &mut schemas)?;
    Ok(schemas)
}

fn r_list_schemas(
    repo: &LocalRepository,
    node: MerkleTreeNode,
    current_path: impl AsRef<Path>,
    schemas: &mut HashMap<PathBuf, Schema>,
) -> Result<(), OxenError> {
    for child in node.children {
        match &child.node {
            EMerkleTreeNode::VNode(_) => {
                let child_path = current_path.as_ref();
                r_list_schemas(repo, child, child_path, schemas)?;
            }
            EMerkleTreeNode::Directory(dir_node) => {
                let child_path = current_path.as_ref().join(&dir_node.name);
                r_list_schemas(repo, child, child_path, schemas)?;
            }
            EMerkleTreeNode::File(file_node) => match &file_node.metadata {
                Some(GenericMetadata::MetadataTabular(metadata)) => {
                    let child_path = current_path.as_ref().join(&file_node.name);
                    schemas.insert(child_path, metadata.tabular.schema.clone());
                }
                _ => {}
            },
            _ => {}
        }
    }
    Ok(())
}

pub fn get_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    let path = path.as_ref();
    let node = repositories::tree::get_file_by_path(repo, commit, &path)?;
    let Some(node) = node else {
        return Err(OxenError::path_does_not_exist(&path));
    };

    let Some(GenericMetadata::MetadataTabular(metadata)) = &node.metadata else {
        return Err(OxenError::path_does_not_exist(&path));
    };

    Ok(Some(metadata.tabular.schema.clone()))
}

/// Get a staged schema
pub fn get_staged(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    todo!()
}

/// List all the staged schemas
pub fn list_staged(repo: &LocalRepository) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    todo!()
}

/// Get the current schema for a given schema ref
pub fn get_from_head(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    todo!()
}

/// Get a string representation of the schema given a schema ref
pub fn show(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    staged: bool,
    verbose: bool,
) -> Result<String, OxenError> {
    todo!()
}

/// Remove a schema override from the staging area, TODO: Currently undefined behavior for non-staged schemas
pub fn rm(repo: &LocalRepository, path: impl AsRef<Path>, staged: bool) -> Result<(), OxenError> {
    todo!()
}

/// Add metadata to the schema
pub fn add_schema_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let db = get_staged_db(repo)?;
    let path = path.as_ref();

    // Get the FileNode from the CommitMerkleTree
    let Some(commit) = repositories::commits::head_commit_maybe(repo)? else {
        return Err(OxenError::basic_str(
            "Cannot add metadata, no commits found.",
        ));
    };

    let Some(mut file_node) = repositories::tree::get_file_by_path(repo, &commit, path)? else {
        return Err(OxenError::path_does_not_exist(path));
    };

    // Update the metadata
    match &mut file_node.metadata {
        Some(GenericMetadata::MetadataTabular(m)) => {
            m.tabular.schema.metadata = Some(metadata.to_owned());
        }
        _ => {
            return Err(OxenError::path_does_not_exist(path));
        }
    }

    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Modified,
        node: MerkleTreeNode::from_file(file_node),
    };

    let key = path.to_string_lossy();
    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();
    db.put(key.as_bytes(), &buf)?;
    Ok(HashMap::new())
}

/// Add metadata to a specific column
pub fn add_column_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let db = get_staged_db(repo)?;
    let path = path.as_ref();
    let column = column.as_ref();

    // Get the FileNode from the CommitMerkleTree
    let Some(commit) = repositories::commits::head_commit_maybe(repo)? else {
        return Err(OxenError::basic_str(
            "Cannot add metadata, no commits found.",
        ));
    };

    let Some(mut file_node) = repositories::tree::get_file_by_path(repo, &commit, path)? else {
        return Err(OxenError::path_does_not_exist(path));
    };

    // Update the column metadata
    let mut results = HashMap::new();
    match &mut file_node.metadata {
        Some(GenericMetadata::MetadataTabular(m)) => {
            log::debug!("add_column_metadata: {m:?}");
            for f in m.tabular.schema.fields.iter_mut() {
                log::debug!("add_column_metadata: checking column {f:?} == {column}");

                if f.name == column {
                    log::debug!("add_column_metadata: found column {f:?}");
                    f.metadata = Some(metadata.to_owned());
                }
            }
            results.insert(path.to_path_buf(), m.tabular.schema.clone());
        }
        _ => {
            return Err(OxenError::path_does_not_exist(path));
        }
    }

    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Modified,
        node: MerkleTreeNode::from_file(file_node),
    };

    let key = path.to_string_lossy();
    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();
    db.put(key.as_bytes(), &buf)?;
    Ok(results)
}

fn get_staged_db(repo: &LocalRepository) -> Result<DBWithThreadMode<MultiThreaded>, OxenError> {
    let path = staged_db_path(&repo.path)?;
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&path))?;
    Ok(db)
}

pub fn staged_db_path(path: &Path) -> Result<PathBuf, OxenError> {
    let path = util::fs::oxen_hidden_dir(path).join(Path::new(constants::STAGED_DIR));
    log::debug!("staged_db_path {:?}", path);
    if !path.exists() {
        std::fs::create_dir_all(&path)?;
    }
    Ok(path)
}
