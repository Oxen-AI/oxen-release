//! # oxen add
//!
//! Stage data for commit
//!

use glob::{glob, Pattern};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

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

    // Copilot, write a conditional for if the path contains glob characters
    // TODONOW: Make this more inclusive

    let glob_chars = vec!['*', '?', '[', ']'];

    log::debug!("Processing path {:?}", path.as_ref());

    // Non-duplicatively collect paths that match the glob pattern either:
    // 1. In the repo working directory (untracked or modified files)
    // 2. In the commit entry db (removed files)
    let mut paths: HashSet<PathBuf> = HashSet::new();
    if let Some(path_str) = path.as_ref().to_str() {
        if glob_chars.iter().any(|c| path_str.contains(*c)) {
            // Also match against any untracked entries in the current dir
            for entry in glob(path_str)? {
                paths.insert(entry?);
            }

            // Match the glob pattern against previously committed entries
            let pattern = Pattern::new(path_str)?;
            let head = api::local::commits::head_commit(repo)?;
            let reader = CommitEntryReader::new(repo, &head)?;
            let entries = reader.list_entries()?;
            let entry_paths: Vec<PathBuf> =
                entries.iter().map(|entry| entry.path.to_owned()).collect();

            for path in entry_paths
                .iter()
                .filter(|entry_path| pattern.matches_path(entry_path))
                .map(|entry_path| entry_path.to_owned())
            {
                paths.insert(path);
            }
        } else {
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
