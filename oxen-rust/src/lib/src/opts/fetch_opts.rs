use std::path::PathBuf;

use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};

#[derive(Clone, Debug)]
pub struct FetchOpts {
    // The remote to fetch from
    pub remote: String,
    // The branch to clone
    pub branch: String,
    // If you only want to clone a subdirectory / tree, you can specify it here
    pub subtree_path: Option<PathBuf>,
    // The depth at which to clone the subtree.
    pub depth: Option<u32>,
    // If true, recursively clones the whole repository history
    // by default, only the head commit is cloned to save time and disk space
    pub all: bool,
}

impl Default for FetchOpts {
    fn default() -> Self {
        Self::new()
    }
}

impl FetchOpts {
    /// Sets `branch` to `DEFAULT_BRANCH_NAME` and defaults `all` to `false`
    pub fn new() -> FetchOpts {
        FetchOpts {
            remote: DEFAULT_REMOTE_NAME.to_string(),
            branch: DEFAULT_BRANCH_NAME.to_string(),
            subtree_path: None,
            depth: None,
            all: false,
        }
    }

    pub fn from_branch(branch: impl AsRef<str>) -> FetchOpts {
        FetchOpts {
            branch: branch.as_ref().to_string(),
            ..FetchOpts::new()
        }
    }
}
