use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::core::db::key_val::path_db;
use crate::core::db::{self};
use crate::core::v0_10_0::index::CommitEntryReader;
use crate::core::v0_10_0::index::Stager;
use crate::error::OxenError;
use crate::model::{
    Commit, CommitEntry, LocalRepository, MerkleHash, MerkleTreeNode, MerkleTreeNodeType,
    MetadataEntry, ParsedResource,
};
use crate::opts::RestoreOpts;
use crate::util;
use crate::{repositories, resource};

use super::merkle_tree::node::{CommitNode, MerkleTreeNodeData};
use super::merkle_tree::CommitMerkleTree;

pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    log::debug!("restore::restore: start");
    if opts.staged {
        log::debug!("restore::restore: handling staged restore");
        return restore_staged(repo, opts);
    }

    let path = opts.path;
    let commit: Commit = repositories::commits::get_commit_or_head(repo, opts.source_ref)?;
    log::debug!("restore::restore: got commit {:?}", commit.id);

    let dir = CommitMerkleTree::dir_from_path_with_children(repo, &commit, &path)?;

    match dir {
        Some(dir) => {
            log::debug!("restore::restore: restoring directory");
            restore_dir(repo, dir, &path)
        }
        None => {
            log::debug!("restore::restore: restoring file");
            let metadata = CommitMerkleTree::from_path(repo, &commit, &path)?;
            if metadata.root.dtype != MerkleTreeNodeType::File {
                return Err(OxenError::basic_str("Path is not a file"));
            }

            let child_file = metadata.root.file().unwrap();

            let resource = ParsedResource {
                commit: Some(commit.clone()),
                branch: None,
                path: path.clone(),
                version: PathBuf::from(&commit.clone().id),
                resource: PathBuf::from(commit.clone().id).join(path),
            };

            let metadata_entry = MetadataEntry {
                filename: child_file.name.clone(),
                is_dir: false,
                latest_commit: Some(commit.clone()),
                resource: Some(resource),
                size: child_file.num_bytes,
                data_type: child_file.data_type.clone(),
                mime_type: child_file.mime_type.clone(),
                extension: child_file.extension.clone(),
                metadata: None,
                is_queryable: None,
            };

            restore_file(repo, &metadata_entry)
        }
    }
}

fn restore_staged(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    log::debug!("restore::restore_staged: start");
    // This function basically removes the opts.path from the stage db
    log::debug!("restore::restore_staged: end");
    Ok(())
}

fn restore_dir(
    repo: &LocalRepository,
    dir: MerkleTreeNodeData,
    path: &Path,
) -> Result<(), OxenError> {
    log::debug!("restore::restore_dir: start");
    let entries = CommitMerkleTree::dir_entries(repo, &dir, path)?;
    log::debug!("restore::restore_dir: got {} entries", entries.len());

    let msg = format!("Restoring Directory: {:?}", dir);
    let bar = util::progress_bar::oxen_progress_bar_with_msg(entries.len() as u64, &msg);

    // iterate over entries in parallel
    entries.iter().for_each(|entry| {
        log::debug!("restore::restore_dir: restoring entry {:?}", entry.filename);
        match restore_file(repo, &entry) {
            Ok(_) => log::debug!("restore::restore_dir: entry restored successfully"),
            Err(e) => {
                log::error!(
                    "restore::restore_dir: error restoring file {:?}: {:?}",
                    entry,
                    e
                );
            }
        }
        bar.inc(1);
    });
    bar.finish_and_clear();
    log::debug!("restore::restore_dir: end");

    Ok(())
}

pub fn restore_file(repo: &LocalRepository, entry: &MetadataEntry) -> Result<(), OxenError> {
    log::debug!("restore::restore_file: start for {:?}", entry.filename);
    restore_file_with_metadata(repo, entry)?;
    log::debug!("restore::restore_file: end");
    Ok(())
}

pub fn restore_file_with_metadata(
    repo: &LocalRepository,
    entry: &MetadataEntry,
) -> Result<(), OxenError> {
    log::debug!(
        "restore::restore_file_with_metadata: start for {:?}",
        entry.filename
    );
    restore_regular(repo, entry)?;
    log::debug!("restore::restore_file_with_metadata: end");
    Ok(())
}

fn restore_regular(repo: &LocalRepository, entry: &MetadataEntry) -> Result<(), OxenError> {
    log::debug!("restore::restore_regular: start for {:?}", entry.filename);
    let entry_resource = entry.resource.clone().ok_or_else(|| {
        OxenError::basic_str(format!(
            "Could not find resource for path: {:?}",
            entry.filename
        ))
    })?;
    log::debug!("restore::restore_regular: got entry resource");

    println!(
        "this is the commit {:?}",
        MerkleHash::from_str(&entry_resource.commit.clone().unwrap().id)
    );

    let file_hash =
        CommitMerkleTree::from_path(repo, &entry_resource.commit.unwrap(), &entry_resource.path)?
            .root
            .hash;

    log::debug!("restore::restore_regular: got file hash {:?}", file_hash);

    let version_path =
        util::fs::version_path_from_node(repo, &file_hash.to_string(), &entry_resource.path);
    log::debug!("restore::restore_regular: calculated version path");

    let working_path = repo.path.join(entry_resource.path);
    let parent = working_path.parent().unwrap();
    if !parent.exists() {
        log::debug!("restore::restore_regular: creating parent directory");
        util::fs::create_dir_all(parent)?;
    }

    log::debug!("restore::restore_regular: copying file");
    log::debug!("restore::restore_regular: version_path {:?}", version_path);
    log::debug!("restore::restore_regular: working_path {:?}", working_path);
    util::fs::copy(version_path, working_path.clone())?;
    log::debug!("restore::restore_regular: end");
    Ok(())
}
