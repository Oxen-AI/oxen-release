use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use jwalk::WalkDir;

use crate::constants::HISTORY_DIR;
use crate::constants::TREE_DIR;
use crate::constants::{HASH_FILE, VERSIONS_DIR, VERSION_FILE_NAME};
use crate::core::cache::cachers;
use crate::core::index::{CommitEntryReader, CommitReader, SchemaWriter};
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util::fs::version_dir_from_hash;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{api, util};

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
    fn name(&self) -> &'static str;
}
