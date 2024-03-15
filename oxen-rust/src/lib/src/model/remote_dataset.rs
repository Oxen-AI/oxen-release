use std::path::PathBuf;

use super::RemoteRepository;

pub struct RemoteDataset {
    pub repo: RemoteRepository, 
    pub identifier: String, 
    pub path: PathBuf,
}