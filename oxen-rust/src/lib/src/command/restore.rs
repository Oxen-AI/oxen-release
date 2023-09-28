//! # oxen restore
//!
//! Restore a file to a previous version
//!

use std::collections::HashSet;
use std::path::PathBuf;

use crate::api;
use crate::core::index::{self, CommitEntryReader};
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RestoreOpts;

use glob::Pattern;

use super::helpers;

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
/// let base_dir = Path::new("repo_dir_commit");
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
/// util::fs::remove_file(hello_path)?;
///
/// // Restore the file
/// command::restore(&repo, RestoreOpts::from_path_ref(hello_name, commit.id))?;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    let commit = api::local::commits::head_commit(repo)?;
    let path = &opts.path;
    let mut paths: HashSet<PathBuf> = HashSet::new();

    // Quoted wildcard path strings, expand to include present and removed files
    if let Some(path_str) = path.to_str() {
        if helpers::is_glob_path(path_str) {
            let pattern = Pattern::new(path_str)?;
            let stager = index::Stager::new(repo)?;
            let commit_reader = CommitEntryReader::new(repo, &commit)?;
            let staged_data = stager.status(&commit_reader)?;

            // If --staged, only operate on staged files
            if opts.staged {
                for entry in staged_data.staged_files {
                    let entry_path_str = entry.0.to_str().unwrap();
                    if pattern.matches(entry_path_str) {
                        paths.insert(entry.0.to_owned());
                    }
                }
            // Otherwise, `restore` should operate on unstaged modifications and removals.
            } else {
                let modified_and_removed: Vec<PathBuf> = staged_data
                    .modified_files
                    .into_iter()
                    .chain(staged_data.removed_files.into_iter())
                    .collect();
                for entry in modified_and_removed {
                    let entry_path_str = entry.to_str().unwrap();
                    if pattern.matches(entry_path_str) {
                        paths.insert(entry.to_owned());
                    }
                }
            }
        } else {
            paths.insert(path.to_owned());
        }
    }

    for path in paths {
        let mut opts = opts.clone();
        opts.path = path;
        index::restore(repo, opts)?;
    }

    Ok(())
}
