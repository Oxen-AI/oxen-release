use crate::constants::{self, HASH_FILE};
use crate::core::db::tree_db::{TreeNode, TreeObject, TreeObjectChild};
use crate::core::index::{CommitEntryReader, CommitEntryWriter, TreeDBReader};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, ContentHashable, LocalRepository, NewCommit};
use crate::{api, util};
use std::path::PathBuf;
use std::sync::Arc;

use super::ObjectDBReader;

#[derive(Debug)]
struct SimpleHash {
    hash: String,
}

impl ContentHashable for SimpleHash {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}

pub fn validate_tree_hash(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<bool, OxenError> {
    // Validate more efficiently if we have a commit parent tree
    // if commit.parent_ids.is_empty() {
    //     return validate_complete_merkle_tree(repository, commit);
    // }

    let is_valid = new_validate_complete_merkle_tree(repository, commit);
    match is_valid {
        Ok(is_valid) => {
            log::debug!("got is_valid {:?} for commit {:?}", is_valid, commit);
            Ok(is_valid)
        }
        Err(e) => {
            log::debug!("got is_valid error {:?} for commit {:?}", e, commit);
            Err(e)
        }
    }

    // TODONOW DELETE AND REPLACE WITH ABOVE
    // if commit.parent_ids.is_empty() {
    //     return Ok(true);
    // }

    // let parent_id = &commit.parent_ids[0];
    // let parent_tree_path =
    //     CommitEntryWriter::commit_tree_db(&repository.path.to_path_buf(), &commit.parent_ids[0]);
    // if !parent_tree_path.exists() {
    //     return validate_complete_merkle_tree(repository, commit);
    // }

    // validate_changed_parts_of_merkle_tree(repository, commit, parent_id)
    // TODONOW: undo this - bypassing to test data transmission
    // Ok(true)
}

pub fn compute_commit_content_hash(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<String, OxenError> {
    let commit_entry_reader = CommitEntryReader::new(repository, commit)?;
    let entries = commit_entry_reader.list_entries()?;
    let n_commit = NewCommit::from_commit(commit); // need this to pass in metadata about commit
    let content_hash = compute_versions_hash(repository, &n_commit, &entries)?;
    Ok(content_hash)
}

fn compute_versions_hash(
    repository: &LocalRepository,
    commit: &NewCommit,
    entries: &[CommitEntry],
) -> Result<String, OxenError> {
    // log::debug!("Computing commit hash for {} entries", entries.len());
    let mut hashes: Vec<SimpleHash> = vec![];
    for entry in entries.iter() {
        // Sometimes we have pre computed the HASH, so that we don't have to fully hash contents again to
        // check if data is synced (I guess this is already in the file path...should we just grab it from there instead?)
        // I think the extra hash computation on the server is nice so that you know the actual contents was saved to disk
        let version_path = util::fs::version_path(repository, entry);
        let maybe_hash_file = version_path.parent().unwrap().join(HASH_FILE);
        // log::debug!("Versions hash Entry [{}]: {:?}", i, entry.path);
        if maybe_hash_file.exists() {
            let hash = util::fs::read_from_path(&maybe_hash_file)?;
            // log::debug!(
            //     "compute_versions_hash cached hash [{i}] {hash} => {:?}",
            //     entry.path
            // );
            hashes.push(SimpleHash { hash });
            continue;
        }

        let hash = util::hasher::hash_file_contents_with_retry(&version_path)?;
        // log::debug!("Got hash: {:?} -> {}", entry.path, hash);

        hashes.push(SimpleHash { hash })
    }

    let content_id = util::hasher::compute_commit_hash(commit, &hashes);
    Ok(content_id)
}

// For when we don't have parent to compare to
fn validate_complete_merkle_tree(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<bool, OxenError> {
    let tree_db_reader = TreeDBReader::new(repository, &commit.id)?;
    r_validate_complete_merkle_node(repository, commit, &tree_db_reader, PathBuf::from(""))
}

fn r_validate_complete_merkle_node(
    repository: &LocalRepository,
    commit: &Commit,
    tree_reader: &TreeDBReader,
    node_path: PathBuf,
) -> Result<bool, OxenError> {
    let node = tree_reader.get_entry(node_path)?.unwrap();
    match node {
        // Base case: if the node is a file, check the hash
        TreeNode::File { path, hash } => {
            let version_path = util::fs::version_path_from_hash_and_file(
                &repository.path,
                hash.clone(),
                path.clone(),
            );
            let maybe_hash_file = version_path.parent().unwrap().join(HASH_FILE);
            if maybe_hash_file.exists() {
                let disk_hash = util::fs::read_from_path(&maybe_hash_file)?;
                if disk_hash != hash {
                    // log::debug!("validation failing on hash mismatch complete for file {:?}", path);
                    return Ok(false);
                }
                Ok(true)
            } else {
                // TODO: Not sure why we're occasionally missing hash files, should be generated on
                // posting commit entries
                let disk_hash = util::hasher::hash_file_contents_with_retry(&version_path)?;
                if hash != disk_hash {
                    // log::debug!("validation failing on re-hash complete for file {:?}", path);
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
        TreeNode::Schema { path, hash } => {
            // Get schema from db
            let schema_path = path
                .strip_prefix(constants::SCHEMAS_TREE_PREFIX)?
                .to_path_buf();
            log::debug!("commit_validator getting schema at path {:?}", schema_path);
            let maybe_schema =
                api::local::schemas::get_by_path_from_ref(repository, &commit.id, schema_path)?;

            match maybe_schema {
                Some(schema) => {
                    if schema.hash != hash {
                        return Ok(false);
                    }
                }
                None => {
                    return Ok(false);
                }
            }

            Ok(true)
        }
        TreeNode::Directory { children, .. } => {
            for child in children {
                let child_path = child.path();
                if !r_validate_complete_merkle_node(
                    repository,
                    commit,
                    tree_reader,
                    child_path.to_path_buf(),
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

// More efficient, only check the hashes of paths that have changed
fn validate_changed_parts_of_merkle_tree(
    repository: &LocalRepository,
    commit: &Commit,
    parent_id: &str,
) -> Result<bool, OxenError> {
    let this_tree_reader = TreeDBReader::new(repository, &commit.id)?;
    let parent_tree_db_path =
        CommitEntryWriter::commit_tree_db(&repository.path.to_path_buf(), parent_id);
    let parent_tree_reader = TreeDBReader::new_from_path(parent_tree_db_path)?;
    r_validate_changed_parts_of_merkle_node(
        repository,
        commit,
        &this_tree_reader,
        &parent_tree_reader,
        PathBuf::from(""),
    )
}

fn r_validate_changed_parts_of_merkle_node(
    repository: &LocalRepository,
    commit: &Commit,
    this_tree_reader: &TreeDBReader,
    parent_tree_reader: &TreeDBReader,
    node_path: PathBuf,
) -> Result<bool, OxenError> {
    let node: TreeNode = this_tree_reader.get_entry(node_path.clone())?.unwrap();
    match node {
        // Base case: if the node is a file, check the hash
        TreeNode::File { path, hash } => {
            let maybe_parent_node = parent_tree_reader.get_entry(path.clone())?;
            if let Some(parent_node) = maybe_parent_node {
                if parent_node.hash() == &hash {
                    return Ok(true);
                }
            }
            let version_path = util::fs::version_path_from_hash_and_file(
                &repository.path,
                hash.clone(),
                path.clone(),
            );
            let maybe_hash_file = version_path.parent().unwrap().join(HASH_FILE);
            if maybe_hash_file.exists() {
                let disk_hash = util::fs::read_from_path(&maybe_hash_file)?;
                if disk_hash != hash {
                    // log::debug!("validation failing on hash mismatch changed for file {:?}", path);
                    return Ok(false);
                }
                return Ok(true);
            } else {
                let disk_hash = util::hasher::hash_file_contents_with_retry(&version_path)?;
                if hash != disk_hash {
                    // log::debug!("validation failing on hash rehash changed for file {:?}", path);
                    return Ok(false);
                } else {
                    return Ok(true);
                }
            }
        }
        TreeNode::Schema { hash, path } => {
            // Get schema from db
            let schema_path = path
                .strip_prefix(constants::SCHEMAS_TREE_PREFIX)?
                .to_path_buf();
            log::debug!("commit_validator getting schema at path {:?}", schema_path);
            let maybe_schema =
                api::local::schemas::get_by_path_from_ref(repository, &commit.id, schema_path)?;

            match maybe_schema {
                Some(schema) => {
                    if schema.hash != hash {
                        return Ok(false);
                    }
                }
                None => {
                    return Ok(false);
                }
            }

            return Ok(true);
        }
        TreeNode::Directory {
            children,
            hash,
            path,
        } => {
            let maybe_parent_node = parent_tree_reader.get_entry(path)?;
            if let Some(parent_node) = maybe_parent_node {
                if parent_node.hash() == &hash {
                    return Ok(true);
                }
            }
            for child in children {
                let child_path = child.path();
                if !r_validate_changed_parts_of_merkle_node(
                    repository,
                    commit,
                    this_tree_reader,
                    parent_tree_reader,
                    child_path.to_path_buf(),
                )? {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

fn new_validate_complete_merkle_tree(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<bool, OxenError> {
    let object_reader = ObjectDBReader::new(repository)?;
    let root_hash = commit.root_hash.clone();
    log::debug!("got root_hash {:?}", root_hash);
    let root_node = object_reader.get_dir(&root_hash)?.unwrap();
    for child in root_node.children() {
        if !new_r_validate_complete_merkle_node(repository, commit, object_reader.clone(), child)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn new_r_validate_complete_merkle_node(
    repository: &LocalRepository,
    commit: &Commit,
    object_reader: Arc<ObjectDBReader>,
    child_node: &TreeObjectChild,
) -> Result<bool, OxenError> {
    // Any missing node in the objects dir = invalid commit, we don't have the data
    match child_node {
        TreeObjectChild::File { path, hash } => {
            let version_path = util::fs::version_path_from_hash_and_file(
                &repository.path,
                hash.clone(),
                path.clone(),
            );

            let maybe_hash_file = version_path.parent().unwrap().join(HASH_FILE);
            if maybe_hash_file.exists() {
                let disk_hash = util::fs::read_from_path(&maybe_hash_file)?;
                if &disk_hash != hash {
                    return Ok(false);
                }
                Ok(true)
            } else {
                let disk_hash = util::hasher::hash_file_contents_with_retry(&version_path)?;
                if hash != &disk_hash {
                    log::debug!("found file issue for file {:?}", path);
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
        TreeObjectChild::Schema { path, hash } => {
            // TODONOW check path here

            let schema_path = path
                .strip_prefix(constants::SCHEMAS_TREE_PREFIX)?
                .to_path_buf();

            let maybe_schema = api::local::schemas::get_by_path_from_ref(
                repository,
                &commit.id,
                schema_path.clone(),
            )?;

            log::debug!(
                "got maybe_schema {:#?} for path {:?} with provided hash {:?}",
                maybe_schema,
                path,
                hash,
            );

            match maybe_schema {
                Some(schema) => {
                    if &schema.hash != hash {
                        log::debug!("found schema issue for schema {:?}", schema_path);
                        return Ok(false);
                    }
                }
                None => {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        TreeObjectChild::Dir { path, hash } => {
            let node = object_reader.get_dir(&hash)?;
            if node.is_none() {
                return Ok(false);
            }
            let node = node.unwrap();
            for child in node.children() {
                if !new_r_validate_complete_merkle_node(
                    repository,
                    commit,
                    object_reader.clone(),
                    child,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        TreeObjectChild::VNode { path, hash } => {
            let node = object_reader.get_vnode(&hash)?;
            if node.is_none() {
                return Ok(false);
            }
            let node = node.unwrap();
            for child in node.children() {
                if !new_r_validate_complete_merkle_node(
                    repository,
                    commit,
                    object_reader.clone(),
                    child,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}
