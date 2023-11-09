//! goes through the commit entry list and pre-computes the hash to verify everything is synced

use crate::core::index::commit_validator;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::{api, util};

pub fn compute(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!("Running compute_and_write_hash");

    // sleep to make sure the commit is fully written to disk
    // Issue was with a lot of text files in this integration test:
    //     "test_remote_ls_return_data_types_just_top_level_dir"
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Construct commit merkle tree if not sent by client
    if !api::local::commits::has_merkle_tree(repo, commit)? {
        log::debug!(
            "Merkle tree for commit {} not provided by client, creating",
            commit.id
        );
        api::local::commits::construct_commit_merkle_tree(repo, commit)?;
    }

    // Keeping this in for short-term debugging purposes, will fix this after schemas are fixed

    // log::debug!("computing entry hash {} -> {}", commit.id, commit.message);
    // let commit_entry_reader = CommitEntryReader::new(repo, commit)?;
    // let entries = commit_entry_reader.list_entries()?;
    // let n_commit = NewCommit::from_commit(commit); // need this to pass in metadata about commit
    // let entries_hash = util::hasher::compute_commit_hash(&n_commit, &entries);

    // log::debug!("computing content hash {} -> {}", commit.id, commit.message);
    // let content_hash = commit_validator::compute_commit_content_hash(repo, commit)?;

    let tree_is_valid = commit_validator::validate_tree_hash(repo, commit)?;

    if tree_is_valid {
        log::debug!("writing commit is valid from tree {:?}", commit);
        write_is_valid(repo, commit, "true")?;
    } else {
        log::debug!("writing commit is not valid from tree {:?}", commit);
        write_is_valid(repo, commit, "false")?;
    }

    // log::debug!(
    //     "computing comparing entries_hash == content_hash {} == {} for commit {}",
    //     entries_hash,
    //     content_hash,
    //     commit.id
    // );

    // let old_is_valid = content_hash == entries_hash;
    // if tree_is_valid != old_is_valid {
    //     panic!(
    //         "Tree is valid: {} but content is valid: {} for commit {}",
    //         tree_is_valid, old_is_valid, commit.id
    //     );
    // }

    // if content_hash == entries_hash {
    //     // log::debug!("Writing commit {} commit is valid: true", commit.id);
    //     write_is_valid(repo, commit, "true")?;
    // } else {
    //     // log::debug!("Writing commit {} commit is valid: false", commit.id);
    //     write_is_valid(repo, commit, "false")?;
    // }

    Ok(())
}

pub fn is_valid(repo: &LocalRepository, commit: &Commit) -> Result<bool, OxenError> {
    match read_is_valid(repo, commit) {
        Ok(val) => Ok(val == "true"),
        Err(_) => Ok(false),
    }
}

fn write_is_valid(repo: &LocalRepository, commit: &Commit, val: &str) -> Result<(), OxenError> {
    let hash_file_path = util::fs::commit_content_is_valid_path(repo, commit);
    util::fs::write_to_path(hash_file_path, val)?;
    Ok(())
}

fn read_is_valid(repo: &LocalRepository, commit: &Commit) -> Result<String, OxenError> {
    let hash_file_path = util::fs::commit_content_is_valid_path(repo, commit);
    let value = util::fs::read_from_path(hash_file_path)?;
    Ok(value)
}
