use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct RestoreOpts {
    pub path: PathBuf,
    pub staged: bool,
    pub is_remote: bool,
    pub source_ref: Option<String>, // commit id or branch name
}

impl RestoreOpts {
    pub fn from_path<P: AsRef<Path>>(path: P) -> RestoreOpts {
        RestoreOpts {
            path: path.as_ref().to_owned(),
            staged: false,
            is_remote: false,
            source_ref: None,
        }
    }

    pub fn from_staged_path<P: AsRef<Path>>(path: P) -> RestoreOpts {
        RestoreOpts {
            path: path.as_ref().to_owned(),
            staged: true,
            is_remote: false,
            source_ref: None,
        }
    }

    pub fn from_path_ref<P: AsRef<Path>, S: AsRef<str>>(path: P, source_ref: S) -> RestoreOpts {
        RestoreOpts {
            path: path.as_ref().to_owned(),
            staged: false,
            is_remote: false,
            source_ref: Some(source_ref.as_ref().to_owned()),
        }
    }
}
