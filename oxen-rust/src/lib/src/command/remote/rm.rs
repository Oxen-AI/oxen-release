//! # oxen rm
//!
//! Remove files from the index and working directory
//!

use std::collections::HashSet;
use std::path::PathBuf;

use crate::constants::OXEN_HIDDEN_DIR;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::{api, core::index};
use crate::command::helpers;

use glob::glob;


/// Removes the path from the index
pub async fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let commit = api::local::commits::head_commit(repo)?;
    let path = &opts.path;

    let mut paths: HashSet<PathBuf> = HashSet::new();
    if let Some(path_str) = path.to_str() {
        if helpers::is_glob_path(path_str) {
            // Match against any entries in the current dir, excluding .oxen
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
        let opts = RmOpts::from_path_opts(path, opts);
        index::rm(repo, &opts).await?;
    }

    Ok(())
}
