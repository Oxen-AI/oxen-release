use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SyncDir {
    pub path: PathBuf,
}

impl SyncDir {
    pub fn from(path: &str) -> SyncDir {
        SyncDir {
            path: PathBuf::from(path),
        }
    }
}
