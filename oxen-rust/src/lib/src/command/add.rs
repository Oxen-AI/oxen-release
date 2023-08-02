//! # oxen add
//!
//! Stage data for commit
//!

use std::path::Path;

use crate::core::index::{oxenignore, CommitEntryReader, Stager};
use crate::{api, error::OxenError, model::LocalRepository};

/// # Stage files into repository
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
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_add");
/// let repo = command::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// command::add(&repo, &hello_file)?;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn add<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let stager = Stager::new_with_merge(repo)?;
    let commit = api::local::commits::head_commit(repo)?;
    let reader = CommitEntryReader::new(repo, &commit)?;
    let ignore = oxenignore::create(repo);
    log::debug!("---START--- oxen add: {:?}", path.as_ref());
    stager.add(path.as_ref(), &reader, &ignore)?;
    log::debug!("---END--- oxen add: {:?}", path.as_ref());
    Ok(())
}
