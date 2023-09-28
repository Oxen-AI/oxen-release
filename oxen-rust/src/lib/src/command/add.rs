//! # oxen add
//!
//! Stage data for commit
//!

use glob::glob;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::helpers;
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

    // Collect paths that match the glob pattern either:
    // 1. In the repo working directory (untracked or modified files)
    // 2. In the commit entry db (removed files)
    let mut paths: HashSet<PathBuf> = HashSet::new();
    if let Some(path_str) = path.as_ref().to_str() {
        if helpers::is_glob_path(path_str) {
            // Match against any untracked entries in the current dir
            for entry in glob(path_str)? {
                paths.insert(entry?);
            }

            let pattern_entries = api::local::commits::glob_entry_paths(repo, &commit, path_str)?;
            paths.extend(pattern_entries);
        } else {
            // Non-glob path
            paths.insert(path.as_ref().to_owned());
        }
    }

    // Get all entries in the head commit
    for path in paths {
        stager.add(path.as_ref(), &reader, &ignore)?;
    }

    log::debug!("---END--- oxen add: {:?}", path.as_ref());
    Ok(())
}
