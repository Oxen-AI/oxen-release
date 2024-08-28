use crate::error::OxenError;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::{Commit, LocalRepository};
use crate::opts::PaginateOpts;
use crate::repositories;
use crate::view::entries::ResourceVersion;
use crate::view::PaginatedDirEntries;

use std::path::Path;

use super::index::merkle_tree::node::DirNode;
use super::index::merkle_tree::CommitMerkleTree;

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

    let tree = CommitMerkleTree::from_commit(repo, &commit)?;
    let entries = CommitMerkleTree::dir_entries(repo, &tree.root, directory)?;
    let dir = CommitMerkleTree::dir(repo, &tree.root, directory)?
        .ok_or(OxenError::resource_not_found(directory.to_str().unwrap()))?;

    let total_pages = 1;
    let total_entries = entries.len();

    let metadata: Option<MetadataDir> = match &dir.metadata {
        Some(GenericMetadata::MetadataDir(metadata)) => Some(metadata.clone()),
        _ => None,
    };

    Ok(PaginatedDirEntries {
        dir: Some(dir),
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
    Ok(Some(node))
}
