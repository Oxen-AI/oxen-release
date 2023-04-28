//! # oxen commit
//!
//! Commit the staged data
//!

use crate::api;
use crate::command;
use crate::error;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};

/// # Commit the staged files in the repo
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::test;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir_commit");
/// let repo = command::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// command::add(&repo, &hello_file)?;
///
/// // Commit staged
/// command::commit(&repo, "My commit message")?;
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn commit(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    let mut status = command::status(repo)?;
    if !status.has_added_entries() {
        return Err(OxenError::NothingToCommit(
            error::string_error::StringError::new(
                r"No files are staged, not committing.
Stage a file or directory with `oxen add <file>`"
                    .to_string(),
            ),
        ));
    }
    let commit = api::local::commits::commit(repo, &mut status, message)?;
    log::info!("DONE COMMITTING in command::commit {}", commit.id);
    Ok(commit)
}
