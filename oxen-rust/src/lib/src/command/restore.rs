//! # oxen restore
//!
//! Restore a file to a previous version
//!

use crate::error::OxenError;
use crate::index;
use crate::model::LocalRepository;
use crate::opts::RestoreOpts;

/// # Restore a removed file that was committed
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::test;
/// # use liboxen::error::OxenError;
/// # use liboxen::opts::RestoreOpts;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir_commit");
/// let repo = command::init(base_dir)?;
///
/// // Write file to disk
/// let hello_name = "hello.txt";
/// let hello_path = base_dir.join(hello_name);
/// util::fs::write_to_path(&hello_path, "Hello World");
///
/// // Stage the file
/// command::add(&repo, &hello_path)?;
///
/// // Commit staged
/// let commit = command::commit(&repo, "My commit message")?.unwrap();
///
/// // Remove the file from disk
/// std::fs::remove_file(hello_path)?;
///
/// // Restore the file
/// command::restore(&repo, RestoreOpts::from_path_ref(hello_name, commit.id))?;
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    index::restore(repo, opts)
}
