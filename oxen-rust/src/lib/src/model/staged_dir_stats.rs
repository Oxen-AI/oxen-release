
use std::path::{PathBuf, Path};
use std::hash::{Hash, Hasher};

// Used for a quick summary of directory
#[derive(Debug, Clone)]
pub struct StagedDirStats {
    pub path: PathBuf,
    pub num_files_staged: usize,
    pub total_files: usize,
}

impl StagedDirStats {
    pub fn from_path<T: AsRef<Path>>(path: T) -> StagedDirStats {
        StagedDirStats {
            path: path.as_ref().to_path_buf(),
            num_files_staged: 0,
            total_files: 0,
        }
    }
}

// Hash on the path field so we can quickly look up
impl PartialEq for StagedDirStats {
    fn eq(&self, other: &StagedDirStats) -> bool {
        self.path == other.path
    }
}
impl Eq for StagedDirStats {}
impl Hash for StagedDirStats {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}
