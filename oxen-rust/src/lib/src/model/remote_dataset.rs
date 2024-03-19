use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::RemoteRepository;

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoteDataset {
    // pub repo: RemoteRepository, 
    pub identifier: String, 
    pub path: PathBuf,
}