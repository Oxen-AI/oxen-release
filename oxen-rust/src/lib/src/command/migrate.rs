use std::path::Path;

use crate::{error::OxenError, model::LocalRepository};

pub mod create_merkle_trees;
pub use create_merkle_trees::CreateMerkleTreesMigration;

pub mod propagate_schemas;
pub use propagate_schemas::PropagateSchemasMigration;

pub mod update_version_files;
pub use update_version_files::UpdateVersionFilesMigration;

pub mod cache_dataframe_size;
pub use cache_dataframe_size::CacheDataFrameSizeMigration;

pub trait Migrate {
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError>;
    fn name(&self) -> &'static str;
}
