use crate::core::db;
use rocksdb::{DBWithThreadMode, IteratorMode, ThreadMode};
use serde::{de, Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::{error::OxenError, model::LocalRepository};

pub struct TreeDB<T: ThreadMode> {
    pub db: DBWithThreadMode<T>,
    repository: LocalRepository, // TODONOW needed?
}

// TODONOW: handle path parsing rather than passing in the db path


impl<T: ThreadMode> TreeDB<T> {
    pub fn new(repository: &LocalRepository, db_path: &Path) -> Result<TreeDB<T>, OxenError> {
        let read_only = false;
        TreeDB::p_new(repository, db_path, read_only)
    }

    pub fn new_read_only(
        repository: &LocalRepository,
        db_path: &Path,
    ) -> Result<TreeDB<T>, OxenError> {
        let read_only = true;
        TreeDB::p_new(repository, db_path, read_only)
    }

    pub fn p_new(
        repository: &LocalRepository,
        db_path: &Path,
        read_only: bool,
    ) -> Result<TreeDB<T>, OxenError> {
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }
        let opts = db::opts::default();
        let db = if read_only {
            if !db_path.join("CURRENT").exists() {
                if let Err(err) = std::fs::create_dir_all(&db_path) {
                    log::error!(
                        "Could not create staging dir {:?} for repo {:?}\nErr: {}",
                        db_path,
                        repository.path,
                        err
                    );
                }
                let _db: DBWithThreadMode<T> =
                    DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
            }

            DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), false)?
        } else {
            DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?
        };
        Ok(TreeDB {
            db,
            repository: repository.clone(),
        })
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
    // TODONOW might not actually need these paths bc they are the keys but idk.
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

    pub fn children(&self) -> &Vec<TreeChild> {
        match self {
            TreeNode::File { .. } => panic!("Node is File type, cannot have children"), // TODONOW error handling
            TreeNode::Directory { children, .. } => children,
        }
    }

    pub fn set_children(&mut self, new_children: Vec<TreeChild>) {
        match self {
            TreeNode::File { .. } => panic!("Node is File type, cannot have children"), // TODONOW error handling
            TreeNode::Directory { children, .. } => *children = new_children,
        }
    }

    pub fn set_hash(&mut self, new_hash: String) {
        match self {
            TreeNode::File { hash, .. } => *hash = new_hash,
            TreeNode::Directory { hash, .. } => *hash = new_hash,
        }
    }

    pub fn add_child(&mut self, child: TreeChild) -> Result<(), OxenError> {
        match self {
            TreeNode::File { .. } => panic!("Node is File type, cannot have children"), // TODONOW error handling
            TreeNode::Directory { children, .. } => children.push(child),
        }
        Ok(())
    }

    pub fn update_child(&mut self, child: TreeChild) -> Result<(), OxenError> {
        match self {
            TreeNode::File { .. } => panic!("Node is File type, cannot have children"), // TODONOW error handling
            TreeNode::Directory { children, .. } => {
                let index = children.binary_search_by(|c| c.path().cmp(child.path()));
                match index {
                    Ok(index) => {
                        children.remove(index);
                    },
                    Err(_) => panic!("Child not found"), // TODONOW oxenerror
                }
            }
        }
        Ok(())
    }

    pub fn delete_child(&mut self, child_path: &PathBuf) -> Result<(), OxenError> {
        match self {
            TreeNode::File { .. } => panic!("Node is File type, cannot have children"), // TODONOW error handling
            TreeNode::Directory { children, .. } => {
                let index = children.binary_search_by(|c| c.path().cmp(child_path));
                match index {
                    Ok(index) => {
                        children.remove(index);
                    },
                    Err(_) => panic!("Child not found"), // TODONOW oxenerror
                }
            }
        }
        Ok(())
    }

    pub fn sort_children(&mut self) -> Result<(), OxenError> {
        match self {
            TreeNode::File { .. } => panic!("Node is File type, cannot have children"), // TODONOW error handling
            TreeNode::Directory { children, .. } => {
                children.sort_by(|a, b| a.path().cmp(b.path()));
            }
        }
        Ok(())
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
