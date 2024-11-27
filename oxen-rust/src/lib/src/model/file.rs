use super::User;

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FileNew {
    pub path: PathBuf,
    pub contents: Vec<u8>,
    pub user: User,
}

impl FileNew {
    pub fn new(path: impl AsRef<Path>, contents: impl AsRef<str>, user: User) -> FileNew {
        FileNew {
            path: path.as_ref().to_path_buf(),
            contents: contents.as_ref().as_bytes().to_vec(),
            user,
        }
    }
}
