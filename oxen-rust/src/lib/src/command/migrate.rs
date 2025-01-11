use std::path::Path;

use crate::{error::OxenError, model::LocalRepository};

pub mod m06_add_child_counts_to_dir_and_vnode;
// pub use m06_add_child_counts_to_dir_and_vnode::OptimizeMerkleTreesMigration;

pub trait Migrate {
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError>;
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
}
