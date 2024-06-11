use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rocksdb::{DBWithThreadMode, MultiThreaded};

use crate::constants::HISTORY_DIR;
use crate::constants::TREE_DIR;
use crate::core::db::{self, str_val_db};

use crate::error::OxenError;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::util;

pub struct CommitMerkleTreeNode {
    pub path: PathBuf,
    pub hash: String,
    pub children: HashMap<String, CommitMerkleTreeNode>,
}

impl CommitMerkleTreeNode {
    /// Constant time lookup by hash
    pub fn get_by_hash(&self, hash: impl AsRef<str>) -> Option<&CommitMerkleTreeNode> {
        self.children.get(hash.as_ref())
    }

    /// Linear time lookup by path
    pub fn get_by_path(&self, path: impl AsRef<Path>) -> Option<&CommitMerkleTreeNode> {
        self.children.values().find(|&child| child.path == path.as_ref())
    }

    /// Check if the node is a leaf node (i.e. it has no children)
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }
}

pub struct CommitMerkleTree {
    pub root: CommitMerkleTreeNode,
}

impl CommitMerkleTree {
    pub fn new(repo: &LocalRepository, commit: &Commit) -> Result<CommitMerkleTree, OxenError> {
        let root = CommitMerkleTree::read_tree(repo, commit)?;
        Ok(CommitMerkleTree { root })
    }

    pub fn get_tree_node(&self, path: impl AsRef<Path>) -> Option<&CommitMerkleTreeNode> {
        self.root.get_by_path(path)
    }

    fn db_dir(repo: &LocalRepository, commit: &Commit, path: impl AsRef<Path>) -> PathBuf {
        if Path::new("") == path.as_ref() {
            return util::fs::oxen_hidden_dir(&repo.path)
                .join(Path::new(HISTORY_DIR))
                .join(&commit.id)
                .join(TREE_DIR);
        }
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(&commit.id)
            .join(TREE_DIR)
            .join(path.as_ref())
    }

    fn read_tree(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitMerkleTreeNode, OxenError> {
        let root_path = Path::new("");
        let root = CommitMerkleTreeNode {
            path: root_path.to_path_buf(),
            hash: commit.id.clone(),
            children: CommitMerkleTree::read_children_from_db(repo, commit, root_path)?,
        };

        Ok(root)
    }

    fn read_children_from_db(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<HashMap<String, CommitMerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let db_dir = CommitMerkleTree::db_dir(repo, commit, path);

        if !db_dir.exists() {
            return Ok(HashMap::new());
        }

        let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&db::opts::default(), db_dir, false)?;
        let vals: Vec<(String, String)> = str_val_db::list(&dir_hashes_db)?;

        let mut nodes = HashMap::new();
        for (dir, hash) in vals {
            if dir.is_empty() {
                // There is always a root node, so we skip it
                continue;
            }

            let child_dir = path.join(dir);
            let children = CommitMerkleTree::read_children_from_db(repo, commit, &child_dir)?;
            let child = CommitMerkleTreeNode {
                path: child_dir,
                hash: hash.clone(),
                children,
            };
            nodes.insert(hash, child);
        }

        Ok(nodes)
    }
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;

    use super::*;

    #[test]
    fn test_read_commit_merkle_tree() -> Result<(), OxenError> {
        let repo_path = Path::new("data")
            .join("test")
            .join("commit_dbs")
            .join("repo");
        let repo = LocalRepository::new(&repo_path)?;
        let commit = Commit {
            id: String::from("1234"),
            parent_ids: vec![],
            message: String::from("initial commit"),
            author: String::from("Ox"),
            email: String::from("ox@oxen.ai"),
            timestamp: OffsetDateTime::now_utc(),
            root_hash: None,
        };

        /*
        Our test db looks like this:
        .
        └── images
            ├── test
            │   ├── dandelion
            │   ├── roses
            │   └── tulips
            └── train
                ├── daisy
                ├── roses
                └── tulips
        */

        let tree = CommitMerkleTree::new(&repo, &commit)?;

        assert_eq!(tree.root.hash, commit.id);
        assert_eq!(tree.root.children.len(), 1);

        // Make sure "images" and "train" are in the root children
        assert!(tree
            .root
            .children
            .iter()
            .any(|(_, x)| x.path == PathBuf::from("images")));

        // Get the "images" child
        let images = tree.root.get_by_path(PathBuf::from("images"));
        assert!(images.is_some());
        assert_eq!(images.unwrap().children.len(), 2);

        // Make sure "test" and "train" are in the "images" children
        assert!(images
            .unwrap()
            .children
            .iter()
            .any(|(_, x)| x.path == PathBuf::from("images/test")));
        assert!(images
            .unwrap()
            .children
            .iter()
            .any(|(_, x)| x.path == PathBuf::from("images/train")));

        // Get the "test" child
        let test = images.unwrap().get_by_path(PathBuf::from("images/test"));
        assert!(test.is_some());
        assert_eq!(test.unwrap().children.len(), 3);

        // Get the "dandelion" child
        let dandelion = test
            .unwrap()
            .get_by_path(PathBuf::from("images/test/dandelion"));
        assert!(dandelion.is_some());

        Ok(())
    }
}
