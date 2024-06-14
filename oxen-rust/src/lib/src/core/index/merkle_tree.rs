use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rocksdb::{DBWithThreadMode, MultiThreaded};

use crate::constants::HISTORY_DIR;
use crate::constants::TREE_DIR;
use crate::core::db::{self, str_val_db};

use crate::error::OxenError;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::util;
use crate::core::index::commit_merkle_tree::CommitMerkleTree;



pub struct MerkleTree {
    // This is the smaller tree that contains the directories and their hashes
    pub dir_tree: CommitMerkleTree,
}

impl MerkleTree {
    pub fn new(repo: &LocalRepository, commit: &Commit) -> Result<Self, OxenError> {
        let dir_tree = CommitMerkleTree::new(repo, commit)?;
        Ok(Self { dir_tree })
    }

    fn read_tree(repo: &LocalRepository, commit: &Commit) -> Result<Self, OxenError> {
        let dir_tree = CommitMerkleTree::new(repo, commit)?;
        Ok(Self { dir_tree })
    }
}