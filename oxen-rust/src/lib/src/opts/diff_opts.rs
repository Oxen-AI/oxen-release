use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct DiffOpts {
    pub path_1: PathBuf,
    pub path_2: Option<PathBuf>,
    pub keys: Vec<String>,
    pub targets: Vec<String>,
    pub repo_dir: Option<PathBuf>,
    pub revision_1: Option<String>,
    pub revision_2: Option<String>,
    pub output: Option<PathBuf>,
}
