use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct RmOpts {
    pub path: PathBuf,
    pub staged: bool,
    pub recursive: bool,
    pub is_cli: bool,
    // TODO: add `force` flag
}

impl RmOpts {
    /// Sets path and defaults all other options to false
    pub fn from_path<P: AsRef<Path>>(path: P) -> RmOpts {
        RmOpts {
            path: path.as_ref().to_owned(),
            staged: false,
            recursive: false,
            is_cli: false,
        }
    }

    /// Sets `staged = true` to remove file from the staging index
    pub fn from_staged_path<P: AsRef<Path>>(path: P) -> RmOpts {
        RmOpts {
            path: path.as_ref().to_owned(),
            staged: true,
            recursive: false,
            is_cli: false,
        }
    }

    /// Sets `recursive = true` to remove dir
    pub fn from_path_recursive<P: AsRef<Path>>(path: P) -> RmOpts {
        RmOpts {
            path: path.as_ref().to_owned(),
            staged: false,
            recursive: true,
            is_cli: false,
        }
    }

    /// Updates the `path` and copies values from `opts`
    pub fn from_path_opts<P: AsRef<Path>>(path: P, opts: &RmOpts) -> RmOpts {
        RmOpts {
            path: path.as_ref().to_owned(),
            staged: opts.staged,
            recursive: opts.recursive,
            is_cli: opts.is_cli,
        }
    }
}
