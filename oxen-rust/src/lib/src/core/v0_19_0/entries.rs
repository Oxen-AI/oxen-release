use crate::error::OxenError;
use crate::model::merkle_tree::node::{DirNode, EMerkleTreeNode, MerkleTreeNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::{Commit, EntryDataType, LocalRepository, MerkleHash, MetadataEntry};
use crate::opts::PaginateOpts;
use crate::repositories;
use crate::view::entries::ResourceVersion;
use crate::view::PaginatedDirEntries;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::index::CommitMerkleTree;

pub fn list_directory(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    revision: impl AsRef<str>,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    let directory = directory.as_ref();
    let revision = revision.as_ref();
    let page = paginate_opts.page_num;
    let page_size = paginate_opts.page_size;

    let resource = Some(ResourceVersion {
        path: directory.to_str().unwrap().to_string(),
        version: revision.to_string(),
    });

    let commit = repositories::revisions::get(repo, revision)?
        .ok_or(OxenError::revision_not_found(revision.into()))?;

    let dir = repositories::tree::get_dir_with_children(repo, &commit, directory)?
        .ok_or(OxenError::resource_not_found(directory.to_str().unwrap()))?;

    let EMerkleTreeNode::Directory(dir_node) = &dir.node else {
        return Err(OxenError::resource_not_found(directory.to_str().unwrap()));
    };

    // Found commits is used to cache the commits so that we don't have
    // to read them from disk again while looping over entries
    let mut found_commits: HashMap<MerkleHash, Commit> = HashMap::new();
    let dir_entry = dir_node_to_metadata_entry(repo, &dir, &mut found_commits)?;
    let entries: Vec<MetadataEntry> = dir_entries(repo, &dir, directory, &mut found_commits)?;
    let total_pages = 1;
    let total_entries = entries.len();

    let metadata: Option<MetadataDir> = Some(MetadataDir::new(dir_node.data_types()));

    Ok(PaginatedDirEntries {
        dir: dir_entry,
        entries,
        resource,
        metadata,
        page_size,
        page_number: page,
        total_pages,
        total_entries,
    })
}

pub fn get_directory(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<DirNode>, OxenError> {
    let node = CommitMerkleTree::dir_without_children(repo, commit, path)?;
    let Some(node) = node else {
        return Ok(None);
    };
    Ok(Some(node.dir()?))
}

pub fn dir_entries(
    repo: &LocalRepository,
    dir: &MerkleTreeNode,
    search_directory: impl AsRef<Path>,
    found_commits: &mut HashMap<MerkleHash, Commit>,
) -> Result<Vec<MetadataEntry>, OxenError> {
    let mut entries: Vec<MetadataEntry> = Vec::new();
    let current_directory = PathBuf::from("");
    p_dir_entries(
        repo,
        dir,
        search_directory,
        current_directory,
        found_commits,
        &mut entries,
    )?;
    Ok(entries)
}

fn dir_node_to_metadata_entry(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    found_commits: &mut HashMap<MerkleHash, Commit>,
) -> Result<Option<MetadataEntry>, OxenError> {
    let EMerkleTreeNode::Directory(dir_node) = &node.node else {
        return Ok(None);
    };

    if !found_commits.contains_key(&dir_node.last_commit_id) {
        let commit = repositories::commits::get_by_hash(repo, &dir_node.last_commit_id)?
            .ok_or(OxenError::resource_not_found(dir_node.name.clone()))?;
        found_commits.insert(dir_node.last_commit_id, commit);
    }

    let commit = found_commits.get(&dir_node.last_commit_id).unwrap();

    Ok(Some(MetadataEntry {
        filename: dir_node.name.clone(),
        is_dir: true,
        latest_commit: Some(commit.clone()),
        resource: None,
        size: dir_node.num_bytes,
        data_type: EntryDataType::Dir,
        mime_type: "inode/directory".to_string(),
        extension: "".to_string(),
        metadata: Some(GenericMetadata::MetadataDir(MetadataDir::new(
            dir_node.data_types(),
        ))),
        is_queryable: None,
    }))
}

fn p_dir_entries(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    search_directory: impl AsRef<Path>,
    current_directory: impl AsRef<Path>,
    found_commits: &mut HashMap<MerkleHash, Commit>,
    entries: &mut Vec<MetadataEntry>,
) -> Result<(), OxenError> {
    let search_directory = search_directory.as_ref();
    let current_directory = current_directory.as_ref();
    for child in &node.children {
        match &child.node {
            EMerkleTreeNode::VNode(vnode) => {
                p_dir_entries(
                    repo,
                    child,
                    search_directory,
                    current_directory,
                    found_commits,
                    entries,
                )?;
            }
            EMerkleTreeNode::Directory(child_dir) => {
                if current_directory == search_directory && !child_dir.name.is_empty() {
                    let metadata = dir_node_to_metadata_entry(repo, child, found_commits)?;
                    entries.push(metadata.unwrap());
                }
                let current_directory = current_directory.join(&child_dir.name);
                p_dir_entries(
                    repo,
                    child,
                    search_directory,
                    current_directory,
                    found_commits,
                    entries,
                )?;
            }
            EMerkleTreeNode::File(child_file) => {
                if current_directory == search_directory {
                    if !found_commits.contains_key(&child_file.last_commit_id) {
                        let commit =
                            repositories::commits::get_by_hash(repo, &child_file.last_commit_id)?
                                .ok_or(OxenError::resource_not_found(child_file.name.clone()))?;
                        found_commits.insert(child_file.last_commit_id, commit);
                    }

                    let commit = found_commits.get(&child_file.last_commit_id).unwrap();
                    let metadata = MetadataEntry {
                        filename: child_file.name.clone(),
                        is_dir: false,
                        latest_commit: Some(commit.clone()),
                        resource: None,
                        size: child_file.num_bytes,
                        data_type: child_file.data_type.clone(),
                        mime_type: child_file.mime_type.clone(),
                        extension: child_file.extension.clone(),
                        metadata: child_file.metadata.clone(),
                        is_queryable: None,
                    };
                    entries.push(metadata);
                }
            }
            _ => {}
        }
    }
    Ok(())
}
