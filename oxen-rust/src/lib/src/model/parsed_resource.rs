/// Simple object to serialize and deserialize an object id
use serde::{Deserialize, Serialize};

use super::{Branch, Commit};
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ParsedResource {
    pub commit: Commit,         // Always resolve to a commit
    pub branch: Option<Branch>, // Maybe resolves to a branch
    pub file_path: PathBuf,     // File path that was past the commit or branch
    pub resource: PathBuf,      // full resource we parsed
}

impl ParsedResource {
    pub fn version(&self) -> String {
        match &self.branch {
            Some(branch) => branch.name.clone(),
            None => self.commit.id.clone(),
        }
    }
}

impl std::fmt::Display for ParsedResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.branch {
            Some(branch) => write!(f, "{}/{}", branch.name, self.file_path.to_string_lossy()),
            None => write!(f, "{}/{}", self.commit.id, self.file_path.to_string_lossy()),
        }
    }
}
