//! # oxen status
//!
//! Check which files have been modified, added, or removed,
//! and which files are staged for commit.
//!

use std::path::Path;

use crate::core::index::{CommitEntryReader, Stager};
use crate::error::OxenError;
use crate::model::{LocalRepository, StagedData};

/// # oxen status
///
/// Get status of files in repository, returns what files are tracked,
/// added, untracked, etc
///
/// Empty Repository:
///
/// ```
/// use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// let base_dir = Path::new("/tmp/repo_dir_status_1");
/// // Initialize empty repo
/// let repo = command::init(&base_dir)?;
/// // Get status on repo
/// let status = command::status(&repo)?;
/// assert!(status.is_clean());
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
///
/// Repository with files
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// let base_dir = Path::new("/tmp/repo_dir_status_2");
/// // Initialize empty repo
/// let repo = command::init(&base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Get status on repo
/// let status = command::status(&repo)?;
/// assert_eq!(status.untracked_files.len(), 1);
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn status(repository: &LocalRepository) -> Result<StagedData, OxenError> {
    log::debug!("status before new_from_head");
    let reader = CommitEntryReader::new_from_head(repository)?;
    log::debug!("status before Stager::new");
    let stager = Stager::new(repository)?;
    log::debug!("status before stager.status");
    let status = stager.status(&reader)?;
    Ok(status)
}

/// # oxen status path/to/dir
///
/// Similar to status but takes the a directory to start looking for changes
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// let base_dir = Path::new("/tmp/repo_dir_status_2");
/// // Initialize empty repo
/// let repo = command::init(&base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Get status on repo
/// let status = command::status(&repo)?;
/// assert_eq!(status.untracked_files.len(), 1);
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn status_from_dir(repository: &LocalRepository, dir: &Path) -> Result<StagedData, OxenError> {
    log::debug!("status before new_from_head");
    let reader = CommitEntryReader::new_from_head(repository)?;
    log::debug!("status before Stager::new");
    let stager = Stager::new(repository)?;
    log::debug!("status before stager.status");
    let status = stager.status_from_dir(&reader, dir)?;
    Ok(status)
}
