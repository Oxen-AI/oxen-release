use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded, WriteBatch};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::constants::STAGED_DIR;
use crate::core::db::key_val::path_db;
use crate::core::db::{self};
use crate::core::v0_10_0::index::CommitEntryReader;
use crate::core::v0_10_0::index::Stager;
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::{
    Commit, CommitEntry, LocalRepository, MerkleHash, MetadataEntry, ParsedResource,
};
use crate::opts::RestoreOpts;
use crate::util;
use crate::{repositories, resource};

pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    log::debug!("restore::restore: start");
    if opts.staged {
        log::debug!("restore::restore: handling staged restore");
        return restore_staged(repo, opts);
    }

    let path = opts.path;
    let commit: Commit = repositories::commits::get_commit_or_head(repo, opts.source_ref)?;
    log::debug!("restore::restore: got commit {:?}", commit.id);
    log::debug!("restore::restore: got path {:?}", path);

    let dir = CommitMerkleTree::dir_with_children(repo, &commit, &path)?;

    match dir {
        Some(dir) => {
            log::debug!("restore::restore: restoring directory");
            restore_dir(repo, dir, &path)
        }
        None => {
            log::debug!("restore::restore: restoring file");
            match CommitMerkleTree::from_path(repo, &commit, &path, false) {
                Ok(merkle_tree) => {
                    log::debug!(
                        "restore::restore: got merkle tree {:?}",
                        merkle_tree.root.node
                    );
                    if !matches!(&merkle_tree.root.node, EMerkleTreeNode::File(_)) {
                        return Err(OxenError::basic_str("Path is not a file"));
                    }

                    let child_file = merkle_tree.root.file().unwrap();

                    restore_file(repo, &child_file, &path)
                }
                Err(OxenError::Basic(msg))
                    if msg.to_string().contains("Merkle tree hash not found") =>
                {
                    log::warn!("restore::restore: No file found at path {:?}", path);
                    println!("No file found at the specified path: {:?}", path);
                    Err(OxenError::basic_str("No file found at the specified path"))
                }
                Err(e) => Err(e),
            }
        }
    }
}

fn restore_staged(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    log::debug!("restore::restore_staged: start");
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    if let Some(db) = open_staged_db(&db_path)? {
        let mut batch = WriteBatch::default();

        if opts.path == PathBuf::from(".") {
            // If path is ".", remove all staged entries
            for result in db.iterator(rocksdb::IteratorMode::Start) {
                if let Ok((key, _)) = result {
                    batch.delete(key);
                }
            }
            log::debug!("restore::restore_staged: prepared to clear all staged entries");
        } else {
            // Remove specific staged entry
            let key = opts.path.to_string_lossy().into_owned();
            batch.delete(key);
            log::debug!(
                "restore::restore_staged: prepared to remove staged entry for path {:?}",
                opts.path
            );
        }

        db.write(batch)?;
        log::debug!("restore::restore_staged: changes committed to the database");
    } else {
        log::debug!("restore::restore_staged: no staged database found");
    }

    log::debug!("restore::restore_staged: end");
    Ok(())
}

fn open_staged_db(db_path: &Path) -> Result<Option<DBWithThreadMode<SingleThreaded>>, OxenError> {
    if db_path.join("CURRENT").exists() {
        let opts = db::key_val::opts::default();
        let db = DBWithThreadMode::open(&opts, dunce::simplified(db_path))?;
        Ok(Some(db))
    } else {
        Ok(None)
    }
}

fn restore_dir(
    repo: &LocalRepository,
    dir: MerkleTreeNode,
    path: &PathBuf,
) -> Result<(), OxenError> {
    log::debug!("restore::restore_dir: start");
    let file_nodes = CommitMerkleTree::dir_entries(&dir)?;
    log::debug!("restore::restore_dir: got {} entries", file_nodes.len());

    let msg = format!("Restoring Directory: {:?}", dir);
    let bar = util::progress_bar::oxen_progress_bar_with_msg(file_nodes.len() as u64, &msg);

    let mut existing_files = HashSet::new();
    if let Ok(entries) = fs::read_dir(path) {
        existing_files.extend(
            entries
                .filter_map(Result::ok)
                .filter(|e| e.path().is_file())
                .map(|e| e.path()),
        );
    }

    file_nodes.iter().for_each(|file_node| {
        let file_path = path.join(&file_node.name);
        existing_files.remove(&file_path);

        match restore_file(repo, file_node, &file_path) {
            Ok(_) => log::debug!("restore::restore_dir: entry restored successfully"),
            Err(e) => {
                log::error!(
                    "restore::restore_dir: error restoring file {:?}: {:?}",
                    file_node.name,
                    e
                );
            }
        }
        bar.inc(1);
    });

    for file_to_remove in existing_files {
        fs::remove_file(file_to_remove)?;
    }

    bar.finish_and_clear();
    log::debug!("restore::restore_dir: end");

    Ok(())
}

pub fn restore_file(
    repo: &LocalRepository,
    file_node: &FileNode,
    path: &PathBuf,
) -> Result<(), OxenError> {
    log::debug!("restore::restore_file: start for {:?}", file_node.name);
    do_restore_file(repo, file_node, path)?;
    log::debug!("restore::restore_file: end");
    Ok(())
}

fn do_restore_file(
    repo: &LocalRepository,
    file_node: &FileNode,
    path: &PathBuf,
) -> Result<(), OxenError> {
    log::debug!("restore::restore_regular: start for {:?}", file_node.name);
    log::debug!("restore::restore_regular: got entry resource");

    let file_hash = file_node.hash;

    log::debug!("restore::restore_regular: got file hash {:?}", file_hash);

    let version_path = util::fs::version_path_from_node(repo, &file_hash.to_string(), path);
    log::debug!("restore::restore_regular: calculated version path");

    let working_path = repo.path.join(path);
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
