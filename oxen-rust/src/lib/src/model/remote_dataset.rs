use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoteDataset {
    // pub repo: RemoteRepository,
    pub identifier: String,
    pub path: PathBuf,
}
