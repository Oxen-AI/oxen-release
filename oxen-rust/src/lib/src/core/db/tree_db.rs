use crate::constants::{
    OBJECTS_DIR, OBJECT_DIRS_DIR, OBJECT_FILES_DIR, OBJECT_SCHEMAS_DIR, OBJECT_VNODES_DIR,
    OXEN_HIDDEN_DIR, SCHEMAS_TREE_PREFIX,
};
use crate::error::OxenError;
use crate::model::{LocalRepository, StagedDirStats, StagedEntry, StagedEntryStatus, StagedSchema};
use crate::{core::db, model::CommitEntry};
use rocksdb::{DBWithThreadMode, ThreadMode};
use serde::{Deserialize, Serialize};
use core::panic;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub struct TreeDB<T: ThreadMode> {
    pub db: DBWithThreadMode<T>,
}

impl<T: ThreadMode> TreeDB<T> {
    pub fn new(db_path: &Path) -> Result<TreeDB<T>, OxenError> {
        let read_only = false;
        TreeDB::p_new(db_path, read_only)
    }

    pub fn new_read_only(db_path: &Path) -> Result<TreeDB<T>, OxenError> {
        let read_only = true;
        TreeDB::p_new(db_path, read_only)
    }

    pub fn p_new(db_path: &Path, read_only: bool) -> Result<TreeDB<T>, OxenError> {
        if !db_path.exists() {
            std::fs::create_dir_all(db_path)?;
        }
        let opts = db::opts::default();
        let db = if read_only {
            if !db_path.join("CURRENT").exists() {
                if let Err(err) = std::fs::create_dir_all(db_path) {
                    log::error!("Could not create staging dir {:?}\nErr: {}", db_path, err);
                }
                let _db: DBWithThreadMode<T> =
                    DBWithThreadMode::open(&opts, dunce::simplified(db_path))?;
            }

            DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(db_path), false)?
        } else {
            DBWithThreadMode::open(&opts, dunce::simplified(db_path))?
        };
        Ok(TreeDB { db })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TreeNode {
    File {
        path: PathBuf,
        hash: String,
    },
    Schema {
        path: PathBuf,
        hash: String,
    },
    Directory {
        path: PathBuf,
        children: Vec<TreeChild>,
        hash: String,
    },
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TreeObjectChildWithStatus {
    pub child: TreeObjectChild,
    pub status: StagedEntryStatus,
}

impl TreeObjectChildWithStatus {
    pub fn from_staged_entry(path: PathBuf, entry: &StagedEntry) -> TreeObjectChildWithStatus {
        TreeObjectChildWithStatus {
            child: TreeObjectChild::File {
                path,
                hash: entry.hash.clone(),
            },
            status: entry.status.clone(),
        }
    }

    pub fn from_staged_schema(
        path: PathBuf,
        staged_schema: &StagedSchema,
    ) -> TreeObjectChildWithStatus {
        TreeObjectChildWithStatus {
            child: TreeObjectChild::Schema {
                path: PathBuf::from(SCHEMAS_TREE_PREFIX).join(path),
                hash: staged_schema.schema.hash.clone(),
            },
            status: staged_schema.status.clone(),
        }
    }

    // TODONOW: This is a little hacky given that we don't maintain a hash for directories at status-time
    pub fn from_staged_dir(dir_stats: &StagedDirStats) -> TreeObjectChildWithStatus {
        TreeObjectChildWithStatus {
            child: TreeObjectChild::Dir {
                path: dir_stats.path.clone(),
                hash: "".to_string(),
            },
            status: dir_stats.status.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TreeObjectChild {
    File { path: PathBuf, hash: String },
    Schema { path: PathBuf, hash: String },
    Dir { path: PathBuf, hash: String },
    VNode { path: PathBuf, hash: String },
}

impl TreeObjectChild {
    pub fn hash(&self) -> &String {
        match self {
            TreeObjectChild::File { hash, .. } => hash,
            TreeObjectChild::Schema { hash, .. } => hash,
            TreeObjectChild::Dir { hash, .. } => hash,
            TreeObjectChild::VNode { hash, .. } => hash,
        }
    }

    pub fn path(&self) -> &PathBuf {
        match self {
            TreeObjectChild::File { path, .. } => path,
            TreeObjectChild::Schema { path, .. } => path,
            TreeObjectChild::Dir { path, .. } => path,
            TreeObjectChild::VNode { path, .. } => path,
        }
    }

    // TODONOW: unicode weirdness?
    pub fn path_as_str(&self) -> &str {
        match self {
            TreeObjectChild::File { path, .. } => path.to_str().unwrap(),
            TreeObjectChild::Schema { path, .. } => path.to_str().unwrap(),
            TreeObjectChild::Dir { path, .. } => path.to_str().unwrap(),
            TreeObjectChild::VNode { path, .. } => path.to_str().unwrap(),
        }
    }
}

// impl Ord for TreeObjectChild {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.path().cmp(&other.path())
//     }
// }

// impl PartialOrd for TreeObjectChild {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl Eq for TreeObjectChild {}

// impl PartialEq for TreeObjectChild {
//     fn eq(&self, other: &Self) -> bool {
//         self.path() == other.path()
//     }
// }



#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TreeObject {
    File {
        hash: String,
        num_bytes: u64, 
        last_modified_seconds: i64,
        last_modified_nanoseconds: u32,
    },
    Schema {
        hash: String,
    },
    Dir {
        children: Vec<TreeObjectChild>,
        hash: String,
    },
    VNode {
        children: Vec<TreeObjectChild>,
        hash: String,
        name: String,
    },
}

impl TreeObject {
    pub fn hash(&self) -> &String {
        match self {
            TreeObject::File { hash, .. } => hash,
            TreeObject::Schema { hash } => hash,
            TreeObject::Dir { hash, .. } => hash,
            TreeObject::VNode { hash, .. } => hash,
        }
    }
    // TODONOW: do we actually need this
    pub fn from_entry(commit_entry: &CommitEntry) -> TreeObject {
        TreeObject::File {
            hash: commit_entry.hash.clone(),
            num_bytes: commit_entry.num_bytes,
            last_modified_seconds: commit_entry.last_modified_seconds,
            last_modified_nanoseconds: commit_entry.last_modified_nanoseconds,
        }
    }

    pub fn children(&self) -> &Vec<TreeObjectChild> {
        match self {
            TreeObject::File { .. } => panic!("File does not have children"),
            TreeObject::Schema { .. } => panic!("Schema does not have children"),
            TreeObject::Dir { children, .. } => children,
            TreeObject::VNode { children, .. } => children,
        }
    }

    pub fn set_children(&mut self, new_children: Vec<TreeObjectChild>) {
        match self {
            TreeObject::File { .. } => panic!("File does not have children"),
            TreeObject::Schema { .. } => panic!("Schema does not have children"),
            TreeObject::Dir { children, .. } => *children = new_children,
            TreeObject::VNode { children, .. } => *children = new_children,
        }
    }

    pub fn set_hash(&mut self, new_hash: String) {
        match self {
            TreeObject::File { hash, .. } => *hash = new_hash,
            TreeObject::Schema { hash } => *hash = new_hash,
            TreeObject::Dir { hash, .. } => *hash = new_hash,
            TreeObject::VNode { hash, .. } => *hash = new_hash,
        }
    }

    // TODONOW error handling and typing here
    pub fn name(&self) -> &String {
        match self {
            TreeObject::File { .. } => panic!("File does not have a name"),
            TreeObject::Schema { .. } => panic!("Schema does not have a name"),
            TreeObject::Dir { .. } => panic!("Dir does not have a name"),
            TreeObject::VNode { name, .. } => name,
        }
    }

    pub fn object_path(&self, repo: &LocalRepository) -> PathBuf {
        let objects_dir = repo.path.join(OXEN_HIDDEN_DIR).join(OBJECTS_DIR);
        let top_hash = &self.hash()[..2];
        let bottom_hash = &self.hash()[2..];
        let base_path = match self {
            TreeObject::File { hash, .. } => objects_dir.join(OBJECT_FILES_DIR),
            TreeObject::Schema { hash } => objects_dir.join(OBJECT_SCHEMAS_DIR),
            TreeObject::Dir { hash, .. } => objects_dir.join(OBJECT_DIRS_DIR),
            TreeObject::VNode { hash, .. } => objects_dir.join(OBJECT_VNODES_DIR),
        };
        base_path.join(top_hash).join(bottom_hash)
    }

    pub fn write(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        let path = self.object_path(repo);
        std::fs::create_dir_all(path.parent().unwrap())?; // Will always have a parent

        let mut file = std::fs::File::create(path)?;

        let bytes = serde_json::to_vec(self)?;
        file.write_all(&bytes)?;
        Ok(())
    }

    pub fn read(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        let path = self.object_path(repo);
        let mut file = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let tree_object: TreeObject = serde_json::from_slice(&bytes)?;
        Ok(())
    }

    pub fn binary_search_on_path(
        &self,
        path: &PathBuf,
    ) -> Result<Option<TreeObjectChild>, OxenError> {
        match self {
            // TODONOW: ERror handling
            TreeObject::File { .. } => panic!("File does not have children"),
            TreeObject::Schema { .. } => panic!("Schema does not have children"),
            TreeObject::Dir { children, .. } => {
                let result = children.binary_search_by(|probe| {
                    let probe_path = probe.path();
                    probe_path.cmp(path)
                });

                match result {
                    Ok(index) => Ok(Some(children[index].clone())),
                    Err(_) => Ok(None),
                }
            }
            TreeObject::VNode { children, .. } => {
                let result = children.binary_search_by(|probe| {
                    let probe_path = probe.path();
                    probe_path.cmp(path)
                });

                match result {
                    Ok(index) => Ok(Some(children[index].clone())),
                    Err(_) => Ok(None),
                }
            }
        }
    }

    pub fn to_commit_entry(&self, path: &PathBuf, commit_id: &str) -> CommitEntry {
        match self {
            TreeObject::File {
                hash, 
                num_bytes,
                last_modified_seconds,
                last_modified_nanoseconds,
            } => {
                CommitEntry {
                    commit_id: commit_id.to_string(),
                    path: path.to_owned(),
                    hash: hash.to_owned(),
                    num_bytes: *num_bytes,
                    last_modified_seconds: *last_modified_seconds,
                    last_modified_nanoseconds: *last_modified_nanoseconds,
                }
            },
            _ => panic!("Cannot convert non-file object to CommitEntry") // TODONOW error handling
        }
    }
}

impl Default for TreeNode {
    fn default() -> Self {
        TreeNode::Directory {
            path: PathBuf::new(),
            hash: String::new(),
            children: Vec::new(),
        }
    }
}

impl TreeNode {
    pub fn path(&self) -> &PathBuf {
        match self {
            TreeNode::File { path, .. } => path,
            TreeNode::Schema { path, .. } => path,
            TreeNode::Directory { path, .. } => path,
        }
    }

    pub fn set_path(&mut self, new_path: PathBuf) {
        match self {
            TreeNode::File { path, .. } => *path = new_path,
            TreeNode::Schema { path, .. } => *path = new_path,
            TreeNode::Directory { path, .. } => *path = new_path,
        }
    }

    pub fn hash(&self) -> &String {
        match self {
            TreeNode::File { hash, .. } => hash,
            TreeNode::Schema { hash, .. } => hash,
            TreeNode::Directory { hash, .. } => hash,
        }
    }

    pub fn set_hash(&mut self, new_hash: String) {
        match self {
            TreeNode::File { hash, .. } => {
                *hash = new_hash;
            }
            TreeNode::Directory { hash, .. } => {
                *hash = new_hash;
            }
            TreeNode::Schema { hash, .. } => {
                *hash = new_hash;
            }
        }
    }

    pub fn children(&self) -> Result<&Vec<TreeChild>, OxenError> {
        match self {
            TreeNode::Directory { children, .. } => Ok(children),
            TreeNode::Schema { .. } => Err(OxenError::basic_str(
                "Node is Schema type, cannot have children",
            )),
            TreeNode::File { .. } => Err(OxenError::basic_str(
                "Node is File type, cannot have children",
            )),
        }
    }

    // Assumes children are sorted - with any other transforms applied
    // after reading node out of db, this will break down
    pub fn upsert_child(&mut self, child: TreeChild) -> Result<(), OxenError> {
        match self {
            TreeNode::File { .. } => Err(OxenError::basic_str(
                "Node is File type, cannot have children",
            )),
            TreeNode::Schema { .. } => Err(OxenError::basic_str(
                "Node is Schema type, cannot have children",
            )),
            TreeNode::Directory { children, .. } => {
                let path_to_find = child.path();
                match children.binary_search_by(|probe| {
                    let probe_path = probe.path();
                    probe_path.cmp(path_to_find)
                }) {
                    Ok(index) => {
                        children[index] = child;
                        Ok(())
                    }
                    Err(index) => {
                        children.insert(index, child);
                        Ok(())
                    }
                }
            }
        }
    }

    pub fn delete_child(&mut self, target_path: &PathBuf) -> Result<(), OxenError> {
        match self {
            TreeNode::File { .. } => Err(OxenError::basic_str(
                "Node is File type, cannot have children",
            )),
            TreeNode::Schema { .. } => Err(OxenError::basic_str(
                "Node is Schema type, cannot have children",
            )),
            TreeNode::Directory { children, .. } => {
                // Search for child by path
                match children.binary_search_by(|probe| {
                    let probe_path = probe.path();
                    probe_path.cmp(target_path)
                }) {
                    Ok(index) => {
                        children.remove(index);
                        Ok(())
                    }
                    Err(_) => Ok(()),
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TreeChild {
    File { path: PathBuf, hash: String },
    Schema { path: PathBuf, hash: String },
    Directory { path: PathBuf, hash: String },
}

impl TreeChild {
    pub fn path(&self) -> &PathBuf {
        match self {
            TreeChild::File { path, .. } => path,
            TreeChild::Schema { path, .. } => path,
            TreeChild::Directory { path, .. } => path,
        }
    }

    pub fn hash(&self) -> &String {
        match self {
            TreeChild::File { hash, .. } => hash,
            TreeChild::Schema { hash, .. } => hash,
            TreeChild::Directory { hash, .. } => hash,
        }
    }
}
