use crate::core::db::path_db;
use crate::core::db::tree_db::{TreeDB, TreeNode};
use crate::error::OxenError;
use crate::model::repository::local_repository::LocalRepository;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{PathBuf, Path};
use std::collections::HashSet;

use super::CommitEntryWriter;
pub struct TreeDBReader {
    pub db: DBWithThreadMode<MultiThreaded>
}

// TODONOW naming of these (client, server etc)
// TODONOW: do we actually need these repos here or in treedb land...
impl TreeDBReader {

    pub fn new(repo: &LocalRepository, commit_id: &str) -> Result<Self, OxenError> {
        let path = CommitEntryWriter::commit_tree_db(&repo.path.to_path_buf(), commit_id);
        let db = TreeDB::new_read_only( &path)?;
        Ok(TreeDBReader {
            db: db.db // TODONOW fix this...
        })
    }

    pub fn new_from_path(db_path: PathBuf) -> Result<Self, OxenError> {
        let db = TreeDB::new_read_only(&db_path)?;
        Ok(TreeDBReader {
            db: db.db
        })
    }

    pub fn new_from_db(repo: &LocalRepository, db: DBWithThreadMode<MultiThreaded>) -> Result<Self, OxenError> {
        Ok(TreeDBReader {
            db
        })
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

    pub fn r_tree_has_conflict(&self, client_node: &TreeNode, server_node: &TreeNode, lca_node: &TreeNode) -> Result<bool, OxenError> {
        // Base checks
    if client_node.hash() == server_node.hash() {
        return Ok(false); // No changes in either commit
    }
    if client_node.hash() == lca_node.hash() || server_node.hash() == lca_node.hash() {
        return Ok(false); // Changes in only one commit since LCA
    }

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

            let mut visited_paths: HashSet<&PathBuf> = HashSet::new();
            for client_child in client_children {
                visited_paths.insert(client_child.path());
                // We know there's a client child bc we're iterating over it - unsure for server or lca.
                let client_child: TreeNode = 
                    self.client_reader.get_entry(client_child.path())?.unwrap();
                let server_child: Option<TreeNode> =
                    self.server_reader.get_entry(client_child.path())?;
                let lca_child: Option<TreeNode> = 
                    self.lca_reader.get_entry(client_child.path())?;

                // Addition on client
                if server_child.is_none() && lca_child.is_none() {
                    return Ok(false);
                }

                // Deletion on server
                if server_child.is_none() && lca_child.is_some() {
                    // Deleted on server, unchanged on client.
                    if lca_child.unwrap().hash() == client_child.hash() {
                        return Ok(false);
                    } else {
                        // Deleted on server, changed on client == conflict
                        return Ok(true);
                    }
                }

                // TODONOW: Recursive Call: When making a recursive call in r_tree_has_conflict, you might encounter scenarios where either the server_child or lca_child does not exist (i.e., they're None). You'll need a way to handle these Option<TreeNode> types when they're None. Consider using unwrap_or with a default value or a different approach.

                if self.r_tree_has_conflict(
                    &client_child,
                    &server_child.unwrap(),
                    &lca_child.unwrap(),
                )? {
                    return Ok(true);
                }
            }

            // Check for deletion on client + modification on server - conflict
            for server_child in server_children {
                if !visited_paths.contains(server_child.path()) {
                    // If it's not in the LCA OR client child, no conflict 
                    let maybe_lca_node: Option<TreeNode> = self.lca_reader.get_entry(server_child.path())?;
                    if maybe_lca_node.is_some() {
                        // If the hashes differ here, then we have a CHANGE in server and DELETION in client. conflict. 
                        if maybe_lca_node.unwrap().hash() != server_child.hash() {
                            return Ok(true);
                        }
                    }

                }
            }
            return Ok(false);
        }
        // For files, if we reach here, it's a conflict because they have different hashes and neither matches the LCA
        (TreeNode::File { .. }, TreeNode::File { .. }, TreeNode::File { .. }) => Ok(true),

        // Other cases, including changing between file and directory types, are conflicts
        _ => Ok(true),
    }

    }
}