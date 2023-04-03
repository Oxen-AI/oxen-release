use std::path::{Path, PathBuf};

use crate::constants::DEFAULT_BRANCH_NAME;

#[derive(Clone, Debug)]
pub struct CloneOpts {
    pub url: String,
    pub dst: PathBuf,
    pub branch: String,
    pub shallow: bool,
}

impl CloneOpts {
    /// Sets branch to DEFAULT_BRANCH_NAME and defaults shallow to false
    pub fn new(url: String, dst: impl AsRef<Path>) -> CloneOpts {
        CloneOpts {
            url,
            dst: dst.as_ref().to_path_buf(),
            branch: DEFAULT_BRANCH_NAME.to_string(),
            shallow: false,
        }
    }
}
