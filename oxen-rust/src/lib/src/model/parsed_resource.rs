/// Simple object to serialize and deserialize an object id
use serde::{Deserialize, Serialize};

use super::{Branch, Commit};
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ParsedResource {
    pub commit: Option<Commit>, // Maybe resolves to a commit
    pub branch: Option<Branch>, // Maybe resolves to a branch
    pub path: PathBuf,          // File path that was past the commit or branch
    pub version: PathBuf,       // This is the split out branch / commit id
    pub resource: PathBuf,      // full resource we parsed
}

impl std::fmt::Display for ParsedResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.version.to_string_lossy(), self.path.to_string_lossy())
    }
}
