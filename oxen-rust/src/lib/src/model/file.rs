use super::User;

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FileNew {
    pub path: PathBuf,
    pub contents: String,
    pub user: User,
}

impl FileNew {
    pub fn new(path: impl AsRef<Path>, contents: impl AsRef<str>, user: User) -> FileNew {
        FileNew {
            path: path.as_ref().to_path_buf(),
            contents: String::from(contents.as_ref()),
            user,
        }
    }
}
