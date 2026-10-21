use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default)]
pub struct RmOpts {
    pub paths: Vec<PathBuf>,
    pub staged: bool,
    pub recursive: bool,
    /// Removes the paths even when that loses changes not yet committed.
    pub force: bool,
}

impl RmOpts {
    /// Sets path and defaults all other options to false
    pub fn from_path<P: AsRef<Path>>(path: P) -> RmOpts {
        RmOpts {
            paths: vec![path.as_ref().to_owned()],
            ..Default::default()
        }
    }

    /// Sets `staged = true` to remove file from the staging index
    pub fn from_staged_path<P: AsRef<Path>>(path: P) -> RmOpts {
        RmOpts {
            paths: vec![path.as_ref().to_owned()],
            staged: true,
            ..Default::default()
        }
    }

    /// Sets `recursive = true` to remove dir
    pub fn from_path_recursive<P: AsRef<Path>>(path: P) -> RmOpts {
        RmOpts {
            paths: vec![path.as_ref().to_owned()],
            recursive: true,
            ..Default::default()
        }
    }
}
