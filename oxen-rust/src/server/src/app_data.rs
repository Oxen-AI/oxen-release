use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct OxenAppData {
    pub path: PathBuf,
}

impl OxenAppData {
    pub fn from(path: &str) -> OxenAppData {
        OxenAppData {
            path: PathBuf::from(path),
        }
    }
}
