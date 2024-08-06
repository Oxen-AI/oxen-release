//! Core functionality for Oxen
//!

use crate::constants::{OXEN_HIDDEN_DIR, TREE_DIR};
use crate::model::LocalRepository;

pub mod db;
pub mod df;

pub mod v1;
pub mod v2;

pub fn is_v1(repo: &LocalRepository) -> bool {
    // We added the tree db in v2, so if it doesn't exist, we are in v1
    !repo.path.join(OXEN_HIDDEN_DIR).join(TREE_DIR).exists()
}

pub fn is_v2(repo: &LocalRepository) -> bool {
    // We added the tree db in v2, so if it exists, we are in v2
    repo.path.join(OXEN_HIDDEN_DIR).join(TREE_DIR).exists()
}
