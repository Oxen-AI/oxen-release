use crate::core::db::tree_db::{TreeDB, TreeNode, TreeObject, TreeObjectChild};
use crate::core::db::{self, path_db};
use crate::error::OxenError;
use crate::model::repository::local_repository::LocalRepository;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::{CommitEntryWriter, TreeObjectReader};
pub struct TreeDBReader {
    pub db: DBWithThreadMode<MultiThreaded>,
}

impl TreeDBReader {
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

// Either a CommitEntryWriter (TODONOW: READER?) or a single rocksdb
pub enum CommitTreeReader {
    TreeObjectReader(TreeObjectReader),
    DB(DBWithThreadMode<MultiThreaded>),
}

impl CommitTreeReader {
    pub fn get_entry_from_child(
        &self,
        child: &TreeObjectChild,
    ) -> Result<Option<TreeObject>, OxenError> {
        match self {
            CommitTreeReader::TreeObjectReader(reader) => reader.get_node_from_child(&child),
            CommitTreeReader::DB(db) => {
                // TODONOW get rid of this debug print
                log::debug!("we're looking for child {:?}", child);
                // Print every item in db
                let iter = db.iterator(rocksdb::IteratorMode::Start);
                for item in iter {
                    match item {
                        Ok((key_bytes, value_bytes)) => {
                            match String::from_utf8(key_bytes.to_vec()) {
                                Ok(key_str) => {
                                    let key_path = PathBuf::from(key_str);

                                    // Attempting to deserialize the value into TreeNode
                                    let deserialized_value: Result<TreeObject, _> =
                                        serde_json::from_slice(&value_bytes);
                                    match deserialized_value {
                                        Ok(tree_node) => {
                                            log::debug!(
                                                "\n\n client testing entry: {:?} -> {:?}\n\n",
                                                key_path,
                                                tree_node
                                            );
                                        }
                                        Err(e) => {
                                            log::error!(
                                                "client error deserializing value: {:?}",
                                                e
                                            );
                                        }
                                    }
                                }
                                Err(_) => {
                                    log::error!("tree_db Could not decode key {:?}", key_bytes);
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("tree_db error: {:?}", e);
                        }
                    }
                }
                path_db::get_entry(db, child.hash())
            }
        }
    }

    pub fn get_root_entry(&self) -> Result<Option<TreeObject>, OxenError> {
        match self {
            CommitTreeReader::TreeObjectReader(reader) => reader.get_root_node(),
            CommitTreeReader::DB(db) => path_db::get_entry(db, ""),
        }
    }
}

pub struct NewTreeDBMerger {
    pub client_reader: CommitTreeReader,
    pub server_reader: CommitTreeReader,
    pub lca_reader: CommitTreeReader,
}

impl NewTreeDBMerger {
    pub fn new(
        client_db_path: PathBuf,
        server_reader: TreeObjectReader,
        lca_reader: TreeObjectReader,
    ) -> NewTreeDBMerger {
        let opts = db::opts::default();
        let client_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open(&opts, client_db_path).unwrap();

        NewTreeDBMerger {
            client_reader: CommitTreeReader::DB(client_db),
            server_reader: CommitTreeReader::TreeObjectReader(server_reader),
            lca_reader: CommitTreeReader::TreeObjectReader(lca_reader),
        }
    }

    pub fn r_tree_has_conflict(
        &self,
        client_node: &Option<TreeObject>,
        server_node: &Option<TreeObject>,
        lca_node: &Option<TreeObject>,
    ) -> Result<bool, OxenError> {
        log::debug!(
            "calling tree on client {:?} server {:?} lca {:?}",
            client_node,
            server_node,
            lca_node
        );
        // All 3 are present
        match (client_node, server_node, lca_node) {
            (Some(client_node), Some(server_node), Some(lca_node)) => {
                self.handle_all_nodes_present(client_node, server_node, lca_node)
            }
            (Some(client_node), Some(server_node), None) => {
                self.handle_missing_lca(client_node, server_node)
            }
            (Some(head), None, Some(lca)) | (None, Some(head), Some(lca)) => {
                self.handle_missing_head(head, lca)
            }
            (Some(_head), None, None) | (None, Some(_head), None) => {
                // Node is new to one commit, doesn't exist in the other or LCA. No merge conflict.
                Ok(false)
            }
            _ => {
                log::debug!("flagging conflict due to undefined pattern");
                log::debug!("client node is {:?}", client_node);
                log::debug!("server node is {:?}", server_node);
                log::debug!("lca node is {:?}", lca_node);
                Ok(true)
            }
        }
    }

    pub fn handle_all_nodes_present(
        &self,
        client_node: &TreeObject,
        server_node: &TreeObject,
        lca_node: &TreeObject,
    ) -> Result<bool, OxenError> {
        if client_node.hash() == server_node.hash() {
            return Ok(false); // No changes in either head commit
        }

        if client_node.hash() == lca_node.hash() || server_node.hash() == lca_node.hash() {
            return Ok(false); // Changes in only one head commit
        }

        // TODONOW: break this out to specify that all 3 must be dirs or vnodes together
        match (client_node, server_node, lca_node) {
            (
                TreeObject::Dir {
                    children: client_children,
                    ..
                }
                | TreeObject::VNode {
                    children: client_children,
                    ..
                },
                TreeObject::Dir {
                    children: server_children,
                    ..
                }
                | TreeObject::VNode {
                    children: server_children,
                    ..
                },
                TreeObject::Dir {
                    children: _lca_children,
                    ..
                }
                | TreeObject::VNode {
                    children: _lca_children,
                    ..
                },
            ) => {
                // Recurse down on children
                let mut visited_paths: HashSet<&PathBuf> = HashSet::new();
                for child in client_children {
                    visited_paths.insert(child.path());

                    // Client child can be obtained directly from the node. Server and lca must first be binary searched on all children of that node
                    // since it is PATH comparisons that matter here, not hash comparisons.

                    let client_child: Option<TreeObject> =
                        self.client_reader.get_entry_from_child(child)?;

                    let maybe_server_child = server_node.binary_search_on_path(child.path())?;
                    let maybe_lca_child = lca_node.binary_search_on_path(child.path())?;

                    let server_child: Option<TreeObject> = match maybe_server_child {
                        Some(child) => self.server_reader.get_entry_from_child(&child)?,
                        None => None,
                    };
                    let lca_child: Option<TreeObject> = match maybe_lca_child {
                        Some(child) => self.lca_reader.get_entry_from_child(&child)?,
                        None => None,
                    };

                    if self.r_tree_has_conflict(&client_child, &server_child, &lca_child)? {
                        return Ok(true);
                    }
                }

                // Deletion on client and modification on server IS a conflict
                // Anything below is already not in the client child
                for child in server_children {
                    if !visited_paths.contains(child.path()) {
                        // If it's not in the LCA OR client child, no conflict
                        let maybe_lca_child = lca_node.binary_search_on_path(child.path())?;
                        let maybe_lca_node = match maybe_lca_child {
                            Some(child) => self.lca_reader.get_entry_from_child(&child)?,
                            None => None,
                        };

                        if let Some(lca_node) = maybe_lca_node {
                            // If the hashes differ here, then we have a CHANGE in server and DELETION in client. conflict.
                            if lca_node.hash() != child.hash() {
                                log::debug!("flagging conflict due to deletion on client and modification on server");
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
                log::debug!("flagging conflict due to different file type in all 3 nodes 1");
                Ok(true) // If at least one of these is a file or schema, this is a merge conflict.
            }
        }
    }

    fn handle_missing_lca(
        &self,
        client_node: &TreeObject,
        server_node: &TreeObject,
    ) -> Result<bool, OxenError> {
        // Missing LCA node == node is new to both commits, so hash must be exactly same
        if client_node.hash() == server_node.hash() {
            return Ok(false);
        }
        // TODONOW: separate out vnodes from dirs to ensure are same
        match (client_node, server_node) {
            (
                TreeObject::Dir {
                    children: client_children,
                    ..
                }
                | TreeObject::VNode {
                    children: client_children,
                    ..
                },
                TreeObject::Dir { .. } | TreeObject::VNode { .. },
            ) => {
                // TODONOW CHECKHERE
                // If different paths and are dir or vnode, recurse down on children
                let mut visited_paths: HashSet<&PathBuf> = HashSet::new();
                for child in client_children {
                    visited_paths.insert(child.path());
                    let client_child: Option<TreeObject> =
                        self.client_reader.get_entry_from_child(child)?;

                    let maybe_server_child = server_node.binary_search_on_path(child.path())?;
                    let server_child: Option<TreeObject> = match maybe_server_child {
                        Some(child) => self.server_reader.get_entry_from_child(&child)?,
                        None => None,
                    };
                    let lca_child: Option<TreeObject> = None;

                    if self.r_tree_has_conflict(&client_child, &server_child, &lca_child)? {
                        return Ok(true);
                    }
                    // TODONOW: Maybe additional pass for doesn't exist on client but does on server?
                    // Oh well i guess that's prohbably fine..
                }
                Ok(false)
            }
            // One of the new files is a file or schema, and its hash does not match the other. == Merge conflict
            (_, _) => {
                log::debug!("flagging conflict due to different file type in all 3 nodes 2");
                Ok(true)
            }
        }
    }

    fn handle_missing_head(&self, head: &TreeObject, lca: &TreeObject) -> Result<bool, OxenError> {
        // Missing one of the head nodes == node is deleted in one commit. Persist the deletion and merge
        if head.hash() == lca.hash() {
            return Ok(false);
        }

        // TODONOW: separate out vnodes from dirs to ensure are same
        match (head, lca) {
            (
                TreeObject::Dir {
                    children: head_children,
                    ..
                }
                | TreeObject::VNode {
                    children: head_children,
                    ..
                },
                TreeObject::Dir {
                    children: _lca_children,
                    ..
                }
                | TreeObject::VNode {
                    children: _lca_children,
                    ..
                },
            ) => {
                // TODONOW: We might not need these hashsets
                // TODONOW: these could be broken out into a Some None Some vs. Nome Some Some
                // architecture if worried about symmetry
                // Recurse down on children
                let mut visited_paths: HashSet<&PathBuf> = HashSet::new();

                for head_child in head_children {
                    visited_paths.insert(head_child.path());
                    // Client + server symmetric here
                    // TODONOW - think a bit more about this symmetricity
                    let head_node: Option<TreeObject> =
                        self.client_reader.get_entry_from_child(head_child)?;
                    let other_head_node: Option<TreeObject> = None;

                    let maybe_lca_child = lca.binary_search_on_path(head_child.path())?;
                    let lca_node = match maybe_lca_child {
                        Some(child) => self.lca_reader.get_entry_from_child(&child)?,
                        None => None,
                    };

                    if self.r_tree_has_conflict(&head_node, &other_head_node, &lca_node)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            // One of the new files is a file or schema, and its hash does not match the other. == Merge conflict
            (_, _) => {
                log::debug!("flagging conflict due to different file type in all 3 nodes 3");
                Ok(true)
            }
        }
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
                self.handle_all_nodes_present(client_node, server_node, lca_node)
            }
            (Some(client_node), Some(server_node), None) => {
                self.handle_missing_lca(client_node, server_node)
            }
            (Some(head), None, Some(lca)) | (None, Some(head), Some(lca)) => {
                self.handle_missing_head(head, lca)
            }
            (Some(_head), None, None) | (None, Some(_head), None) => {
                // Node is new to one commit, doesn't exist in the other or LCA. No merge conflict.
                Ok(false)
            }
            _ => Ok(true),
        }
    }

    fn handle_all_nodes_present(
        &self,
        client_node: &TreeNode,
        server_node: &TreeNode,
        lca_node: &TreeNode,
    ) -> Result<bool, OxenError> {
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
                    let lca_child: Option<TreeNode> = self.lca_reader.get_entry(child.path())?;

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
                Ok(true) // If at least one of these is a file or schema, merge conflict (for now)
            }
        }
    }

    fn handle_missing_lca(
        &self,
        client_node: &TreeNode,
        server_node: &TreeNode,
    ) -> Result<bool, OxenError> {
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

                        if self.r_tree_has_conflict(&client_child, &server_child, &lca_child)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                (_, _) => {
                    Ok(true) // If at least one of the new, differently-hashed additions is a file or schema, merge conflict.
                }
            }
        }
    }

    fn handle_missing_head(&self, head: &TreeNode, lca: &TreeNode) -> Result<bool, OxenError> {
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

                        if self.r_tree_has_conflict(&client_child, &server_child, &lca_child)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                (_, _) => {
                    Ok(true) // If at least one of the new, differently-hashed additions is a file or schema, merge conflict.
                }
            }
        }
    }
}
