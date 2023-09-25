//! # oxen restore
//!
//! Restore a file to a previous version
//!

use std::collections::HashSet;
use std::path::PathBuf;

use crate::api;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::core::index;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RestoreOpts;

use glob::glob;

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
            for entry in glob(path_str)? {
                let entry = entry?;
                if !entry.starts_with(OXEN_HIDDEN_DIR) {
                    paths.insert(entry);
                }
            }
            let pattern_entries = api::local::commits::glob_entry_paths(repo, &commit, path_str)?;
            paths.extend(pattern_entries);
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
