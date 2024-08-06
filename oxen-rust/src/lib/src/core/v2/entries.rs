
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, MetadataEntry};
use crate::view::PaginatedDirEntries;
use crate::opts::PaginateOpts;

use std::path::Path;

pub fn list_directory(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    revision: impl AsRef<str>,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    Err(OxenError::basic_str("Not implemented"))
}