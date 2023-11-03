use crate::core::db;
use rocksdb::{DBWithThreadMode, ThreadMode};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::OxenError;

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

#[derive(Serialize, Deserialize, Debug)]
pub enum TreeNode {
    File {
        path: PathBuf,
        hash: String,
    },
    Directory {
        path: PathBuf,
        children: Vec<TreeChild>,
        hash: String,
    },
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
            TreeNode::Directory { path, .. } => path,
        }
    }

    pub fn set_path(&mut self, new_path: PathBuf) {
        match self {
            TreeNode::File { path, .. } => *path = new_path,
            TreeNode::Directory { path, .. } => *path = new_path,
        }
    }

    pub fn hash(&self) -> &String {
        match self {
            TreeNode::File { hash, .. } => hash,
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
        }
    }

    pub fn children(&self) -> Result<&Vec<TreeChild>, OxenError> {
        match self {
            TreeNode::File { .. } => Err(OxenError::basic_str(
                "Node is File type, cannot have children",
            )),
            TreeNode::Directory { children, .. } => Ok(children),
        }
    }

    // Assumes children are sorted - with any other transforms applied
    // after reading node out of db, this will break down
    pub fn upsert_child(&mut self, child: TreeChild) -> Result<(), OxenError> {
        match self {
            TreeNode::File { .. } => Err(OxenError::basic_str(
                "Node is File type, cannot have children",
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

#[derive(Serialize, Deserialize, Debug)]
pub enum TreeChild {
    File { path: PathBuf, hash: String },
    Directory { path: PathBuf, hash: String },
}

impl TreeChild {
    pub fn path(&self) -> &PathBuf {
        match self {
            TreeChild::File { path, .. } => path,
            TreeChild::Directory { path, .. } => path,
        }
    }

    pub fn hash(&self) -> &String {
        match self {
            TreeChild::File { hash, .. } => hash,
            TreeChild::Directory { hash, .. } => hash,
        }
    }
}
