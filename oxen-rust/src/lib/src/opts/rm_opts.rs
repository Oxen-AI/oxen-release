use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct RmOpts {
    pub path: PathBuf,
    pub staged: bool,
    pub force: bool,
    pub recursive: bool,
}

impl RmOpts {
    /// Sets path and defaults all other options to false
    pub fn from_path<P: AsRef<Path>>(path: P) -> RmOpts {
        RmOpts {
            path: path.as_ref().to_owned(),
            staged: false,
            force: false,
            recursive: false,
        }
    }

    /// Sets `staged = true` to remove file from the staging index
    pub fn from_staged_path<P: AsRef<Path>>(path: P) -> RmOpts {
        RmOpts {
            path: path.as_ref().to_owned(),
            staged: true,
            force: false,
            recursive: false,
        }
    }

    /// Updates the `path` and copies values from `opts`
    pub fn from_path_opts<P: AsRef<Path>>(path: P, opts: &RmOpts) -> RmOpts {
        RmOpts {
            path: path.as_ref().to_owned(),
            staged: opts.staged,
            force: opts.force,
            recursive: opts.recursive,
        }
    }
}
