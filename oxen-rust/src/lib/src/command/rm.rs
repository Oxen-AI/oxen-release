//! # oxen rm
//!
//! Remove files from the index and working directory
//!

use crate::core::index;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;

/// Removes the path from the index
pub async fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    index::rm(repo, opts).await
}
