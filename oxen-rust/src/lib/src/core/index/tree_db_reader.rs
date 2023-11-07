use crate::core::db::path_db;
use crate::core::db::tree_db::{TreeDB, TreeNode};
use crate::error::OxenError;
use crate::model::repository::local_repository::LocalRepository;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::CommitEntryWriter;
pub struct TreeDBReader {
    pub db: DBWithThreadMode<MultiThreaded>,
}

impl TreeDBReader {
    // TODONOW: should we directly initialize the tree_db class, or just the treedb reader?
    pub fn new(repo: &LocalRepository, commit_id: &str) -> Result<Self, OxenError> {
        let path = CommitEntryWriter::commit_tree_db(&repo.path.to_path_buf(), commit_id);
        let db = TreeDB::new_read_only(&path)?;
        Ok(TreeDBReader { db: db.db })
    }

    pub fn new_from_path(db_path: PathBuf) -> Result<Self, OxenError> {
        let db = TreeDB::new_read_only(&db_path)?;
        Ok(TreeDBReader { db: db.db })
    }

    pub fn new_from_db(db: DBWithThreadMode<MultiThreaded>) -> Result<Self, OxenError> {
        Ok(TreeDBReader { db })
    }

    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<TreeNode>, OxenError> {
        let path = path.as_ref();
        path_db::get_entry(&self.db, path)
    }

    pub fn get_root_node(&self) -> Result<Option<TreeNode>, OxenError> {
        path_db::get_entry(&self.db, Path::new(""))
    }
}

pub struct TreeDBMerger {
    pub client_reader: TreeDBReader,
    pub server_reader: TreeDBReader,
    pub lca_reader: TreeDBReader,
}

impl TreeDBMerger {
    pub fn new(
        client_db_path: PathBuf,
        server_db_path: PathBuf,
        lca_db_path: PathBuf,
    ) -> Result<Self, OxenError> {
        Ok(TreeDBMerger {
            client_reader: TreeDBReader::new_from_path(client_db_path).unwrap(),
            server_reader: TreeDBReader::new_from_path(server_db_path).unwrap(),
            lca_reader: TreeDBReader::new_from_path(lca_db_path).unwrap(),
        })
    }

    pub fn r_tree_has_conflict(
        &self,
        client_node: &Option<TreeNode>,
        server_node: &Option<TreeNode>,
        lca_node: &Option<TreeNode>,
    ) -> Result<bool, OxenError> {
        // All 3 are present
        match (client_node, server_node, lca_node) {
            (Some(client_node), Some(server_node), Some(lca_node)) => {
                if client_node.hash() == server_node.hash() {
                    return Ok(false); // No changes in either head commit
                }

                if client_node.hash() == lca_node.hash() || server_node.hash() == lca_node.hash() {
                    return Ok(false); // Changes in only one head commit
                }

                // We have a hash conflict: if all are directories, recurse down on chidlren. Otherwise, conflict.
                // TODO: we could get more granular here and check if, ex., client + server are dirs but lca is file.
                // biasing towards safety to start
                match (client_node, server_node, lca_node) {
                    (
                        TreeNode::Directory {
                            children: client_children,
                            ..
                        },
                        TreeNode::Directory {
                            children: server_children,
                            ..
                        },
                        TreeNode::Directory {
                            children: _lca_children,
                            ..
                        },
                    ) => {
                        // Recurse down on children
                        let mut visited_paths: HashSet<&PathBuf> = HashSet::new();
                        for child in client_children {
                            visited_paths.insert(child.path());
                            let client_child: Option<TreeNode> =
                                self.client_reader.get_entry(child.path())?;
                            let server_child: Option<TreeNode> =
                                self.server_reader.get_entry(child.path())?;
                            let lca_child: Option<TreeNode> =
                                self.lca_reader.get_entry(child.path())?;

                            if self.r_tree_has_conflict(&client_child, &server_child, &lca_child)? {
                                return Ok(true);
                            }
                        }
                        // Check for deletion on client + modification on server - conflict
                        for child in server_children {
                            if !visited_paths.contains(child.path()) {
                                // If it's not in the LCA OR client child, no conflict
                                let maybe_lca_node: Option<TreeNode> =
                                    self.lca_reader.get_entry(child.path())?;
                                if let Some(lca_node) = maybe_lca_node {
                                    // If the hashes differ here, then we have a CHANGE in server and DELETION in client. conflict.
                                    if lca_node.hash() != child.hash() {
                                        return Ok(true);
                                    }
                                }
                            }
                        }
                        // If neither loop runs, then server_children = client_children = None
                        // (this case is covered by hash checks, but the compiler doesn't know that)
                        Ok(false)
                    }
                    (_, _, _) => {
                        Ok(true) // If at least one of these is a file, merge conflict (for now)
                    }
                }
            }
            (Some(client_node), Some(server_node), None) => {
                // Missing LCA node - node is new to both commits
                if client_node.hash() == server_node.hash() {
                    Ok(false) // Duplicate addition between two commits. No conflict
                } else {
                    match (client_node, server_node) {
                        (
                            TreeNode::Directory {
                                children: client_children,
                                ..
                            },
                            TreeNode::Directory { .. },
                        ) => {
                            // Recurse down on children
                            let mut visited_paths: HashSet<&PathBuf> = HashSet::new();
                            for child in client_children {
                                visited_paths.insert(child.path());
                                let client_child: Option<TreeNode> =
                                    self.client_reader.get_entry(child.path())?;
                                let server_child: Option<TreeNode> =
                                    self.server_reader.get_entry(child.path())?;
                                let lca_child: Option<TreeNode> = Option::None;

                                if self.r_tree_has_conflict(
                                    &client_child,
                                    &server_child,
                                    &lca_child,
                                )? {
                                    return Ok(true);
                                }
                            }
                            Ok(false)
                        }
                        (_, _) => {
                            Ok(true) // If at least one of the new, differently-hashed additions is a file, merge conflict.
                        }
                    }
                }
            }
            (Some(head), None, Some(lca)) | (None, Some(head), Some(lca)) => {
                if head.hash() == lca.hash() {
                    Ok(false)
                } else {
                    match (head, lca) {
                        (
                            TreeNode::Directory {
                                children: head_children,
                                ..
                            },
                            TreeNode::Directory { .. },
                        ) => {
                            // Recurse down on children
                            let mut visited_paths: HashSet<&PathBuf> = HashSet::new();
                            for head_child in head_children {
                                visited_paths.insert(head_child.path());
                                // Client + server symmetric here
                                let client_child: Option<TreeNode> =
                                    self.client_reader.get_entry(head_child.path())?;
                                let server_child: Option<TreeNode> = Option::None;
                                let lca_child: Option<TreeNode> =
                                    self.client_reader.get_entry(head_child.path())?;

                                if self.r_tree_has_conflict(
                                    &client_child,
                                    &server_child,
                                    &lca_child,
                                )? {
                                    return Ok(true);
                                }
                            }
                            Ok(false)
                        }
                        (_, _) => {
                            Ok(true) // If at least one of the new, differently-hashed additions is a file, merge conflict.
                        }
                    }
                }
            }
            (Some(_head), None, None) | (None, Some(_head), None) => {
                // Node is new to one commit, doesn't exist in the other or LCA. No merge conflict.
                Ok(false)
            }
            _ => Ok(true),
        }
    }
}
