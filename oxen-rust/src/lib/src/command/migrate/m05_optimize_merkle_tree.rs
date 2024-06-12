use rocksdb::{DBWithThreadMode, MultiThreaded};
use serde_json::Value;

use super::Migrate;

use std::path::{Path, PathBuf};

use crate::core::db::tree_db::TreeObjectChild;
use crate::core::db::{self, kv_db, str_json_db, str_val_db};
use crate::core::index::commit_merkle_tree::{CommitMerkleTree, CommitMerkleTreeNode};
use crate::core::index::{CommitReader, ObjectDBReader, TreeObjectReader};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{api, constants, util};

pub struct OptimizeMerkleTreesMigration;
impl Migrate for OptimizeMerkleTreesMigration {
    fn name(&self) -> &'static str {
        "optimize_merkle_trees"
    }

    fn description(&self) -> &'static str {
        "Optimize merkle trees for all commits"
    }

    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            create_merkle_trees_for_all_repos_up(path)?;
        } else {
            let repo = LocalRepository::new(path)?;
            create_merkle_trees_up(&repo)?;
        }
        Ok(())
    }

    fn down(&self, _path: &Path, _all: bool) -> Result<(), OxenError> {
        log::warn!("Optimize merkle trees migration is not reversible");
        Ok(())
    }

    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError> {
        let objects_dir = repo
            .path
            .join(constants::OXEN_HIDDEN_DIR)
            .join(constants::TREE_DIR);
        if !objects_dir.exists() {
            return Ok(true);
        }
        // This may need a more elaborate check for migrations that are aborted with a single repo...
        // but it's too computationally expensive to parse through all the trees.
        Ok(false)
    }
}

pub fn create_merkle_trees_for_all_repos_up(path: &Path) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to migrate...");
    let namespaces = api::local::repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        // Show the canonical namespace path
        log::debug!(
            "This is the namespace path we're walking: {:?}",
            namespace_path.canonicalize()?
        );
        let repos = api::local::repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match create_merkle_trees_up(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not migrate merkle trees for repo {:?}\nErr: {}",
                        repo.path.canonicalize(),
                        err
                    )
                }
            }
        }
        bar.inc(1);
    }
    Ok(())
}

pub fn create_merkle_trees_up(repo: &LocalRepository) -> Result<(), OxenError> {
    // Get all commits in repo, then construct merkle tree for each commit
    let reader = CommitReader::new(repo)?;
    let all_commits = reader.list_all()?;
    // sort these by timestamp from oldest to newest
    let mut all_commits = all_commits.clone();
    all_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let bar = oxen_progress_bar(all_commits.len() as u64, ProgressBarType::Counter);
    // let commit_writer = CommitWriter::new(repo)?;
    for commit in all_commits {
        // First convert the .oxen/history/{COMMIT_ID}/dirs directory into
        // a .oxen/history/{COMMIT_ID}/tree dir that keeps all the pointers to
        // the root merkle tree objects
        convert_dirs_to_tree_dir(repo, &commit)?;

        // Populate the global merkle tree from the old objects dir
        migrate_merkle_tree(repo, &commit)?;

        bar.inc(1);
    }

    Ok(())
}

/*
convert_dirs_to_tree_dir converts our flat dirs db into a tree db

tree .oxen/history/176afdb4a043e49f/dirs/
├── 000008.sst

""
"code"
"images"
"images/daisy"
"images/dandelion"
"images/roses"
"images/sunflowers"
"images/tulips"
"metadata"

into

tree .oxen/history/176afdb4a043e49f/tree/
├── 000008.sst
└── images
    ├── 000008.sst
    ├── test
    │   ├── 000008.sst
    │   ├── 000013.sst
    └── train
        ├── 000008.sst
        ├── 000013.sst

""
"code"
"images"
  "daisy"
  "dandelion"
  "roses"
  "sunflowers"
  "tulips"
"metadata"
*/
fn convert_dirs_to_tree_dir(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    println!("Converting dirs to tree for commit: {}", commit.id);

    let dir_hashes_dir = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::HISTORY_DIR)
        .join(&commit.id)
        .join(constants::DIR_HASHES_DIR);
    let tree_dir = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::HISTORY_DIR)
        .join(&commit.id)
        .join(constants::TREE_DIR);

    // Clear tree dir if exists
    if tree_dir.exists() {
        println!("Clearing tree dir: {:?}", tree_dir);
        util::fs::remove_dir_all(&tree_dir)?;
    }

    // Create tree dir
    util::fs::create_dir_all(&tree_dir)?;

    println!("Reading old directories db: {:?}", dir_hashes_dir);

    // Read all the entries from the dirs db
    let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&db::opts::default(), &dir_hashes_dir, false)?;
    let vals: Vec<(String, String)> = str_val_db::list(&dir_hashes_db)?;

    /* At this point we have a list like this

    ""
    "code"
    "images"
    "images/daisy"
    "images/dandelion"
    "images/roses"
    "images/sunflowers"
    "images/tulips"
    "metadata"
    */

    for (dir, hash) in vals {
        let hash = hash.replace("\"", "");
        let split_dir = dir.split('/').collect::<Vec<&str>>();

        if split_dir.len() == 1 {
            // This is a root directory
            // Write this to the root tree dir
            let tree_db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&db::opts::default(), &tree_dir)?;
            tree_db.put(dir, hash.as_bytes())?;
        } else {
            // This means there is a slash
            // Take the last element of the split_dir
            // and write it to a tree dir for 0..split_dir.len() - 1
            let last_dir = split_dir.last().unwrap();
            let first_elems = split_dir[..split_dir.len() - 1].to_vec();
            // Create a path from the first elements
            let base_path: PathBuf = first_elems.iter().collect();

            let tree_path = tree_dir.join(base_path);
            if !tree_path.exists() {
                util::fs::create_dir_all(&tree_path)?;
            }

            let tree_db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&db::opts::default(), &tree_path)?;
            tree_db.put(last_dir, hash.as_bytes())?;
        }
    }

    Ok(())
}

fn migrate_merkle_tree(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    let tree_dir = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR);

    if tree_dir.exists() {
        println!("Clearing tree dir: {:?}", tree_dir);
        util::fs::remove_dir_all(&tree_dir)?;
    }

    // Instantiate the object reader, most expensive operation
    let object_reader = ObjectDBReader::new(repo)?;

    // iterate over the commit tree to get the root nodes
    let commit_merkle_tree = CommitMerkleTree::new(repo, commit)?;
    r_migrate_merkle_nodes(repo, commit, &object_reader, &commit_merkle_tree.root)?;

    Ok(())
}

fn r_migrate_merkle_nodes(repo: &LocalRepository, commit: &Commit, reader: &ObjectDBReader, node: &CommitMerkleTreeNode) -> Result<(), OxenError> {
    if node.is_leaf() {
        return Ok(());
    }

    for (hash, child) in &node.children {
        println!("hash: {} path: {:?}", hash, child.path);
        r_migrate_merkle_nodes(repo, commit, &reader, &child)?;
        migrate_vnodes(repo, &reader, &child)?;
    }

    Ok(())
}

fn migrate_vnodes(
    repo: &LocalRepository,
    reader: &ObjectDBReader,
    node: &CommitMerkleTreeNode
) -> Result<(), OxenError> {
    // Read the values from the .oxen/objects/dirs db and write them
    // to the proper .oxen/tree/{path} with their hash as the key and type 
    // and metadata as the value
    //
    println!("Getting object for path: {:?} -> {:?}", node.path, node.hash);
    let obj = reader.get_dir(&node.hash)?;

    let Some(tree_obj) = obj else {
        return Err(OxenError::basic_str(format!("could not get dir objects for {}", node.hash)));
    };

    // These should all be vnodes, so write them to .oxen/tree/{node.path}/{node.hash}
    let tree_path = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(&node.path)
        .join(&node.hash);

    // Write all the VNodes
    let tree_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&db::opts::default(), &tree_path)?;
    for child in tree_obj.children() {
        match child {
            TreeObjectChild::VNode { path, hash } => {
                println!("child: {:?} -> {:?}", path, hash);
                // mkdir if not exists
                if !tree_path.exists() {
                    util::fs::create_dir_all(&tree_path)?;
                }

                // serialize child to json
                // TODO: row has hash twice, should just have path
                // 16442f8e6fdc8ae0ff617e59211721d4	{"VNode":{"path":"57","hash":"16442f8e6fdc8ae0ff617e59211721d4"}}

                let child_json = serde_json::to_string(&child)?;
                tree_db.put(hash.as_bytes(), child_json.as_bytes())?;

                // Look up all the files from that vnode
                migrate_files(repo, &reader, &node, &child)?;
            },
            _ => {
                return Err(OxenError::basic_str(format!("unexpected child type: {:?}", child)));
            }
        }
    }

    Ok(())
}

fn migrate_files(
    repo: &LocalRepository,
    reader: &ObjectDBReader,
    node: &CommitMerkleTreeNode,
    vnode: &TreeObjectChild
) -> Result<(), OxenError> {
    match vnode {
        TreeObjectChild::VNode { path, hash } => {
            let tree_path = repo
                .path
                .join(constants::OXEN_HIDDEN_DIR)
                .join(constants::TREE_DIR)
                .join(&node.path)
                .join(&node.hash)
                .join(&path);

            let tree_db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&db::opts::default(), &tree_path)?;
            
            let tree_obj = reader.get_vnode(hash)?;
            let Some(tree_obj) = tree_obj else {
                return Err(OxenError::basic_str(format!("could not get vnode objects for {}", node.hash)));
            };
            for child in tree_obj.children() {
                match child {
                    TreeObjectChild::File { path, hash } => {
                        println!("file: {:?} -> {:?}", path, hash);
                        let child_json = serde_json::to_string(&child)?;
                        tree_db.put(hash.as_bytes(), child_json.as_bytes())?;
                    }
                    TreeObjectChild::Dir { path, hash } => {
                        println!("dir: {:?} -> {:?}", path, hash);
                        let child_json = serde_json::to_string(&child)?;
                        tree_db.put(hash.as_bytes(), child_json.as_bytes())?;
                    }
                    TreeObjectChild::Schema { path, hash } => {
                        println!("vnode: {:?} -> {:?}", path, hash);
                        let child_json = serde_json::to_string(&child)?;
                        tree_db.put(hash.as_bytes(), child_json.as_bytes())?;
                    }
                    _ => {
                        return Err(OxenError::basic_str(format!("unexpected child type: {:?}", child)));
                    }
                }
            }
        },
        _ => {
            return Err(OxenError::basic_str(format!("unexpected child type: {:?}", vnode)));
        }
    }

    Ok(())
}

pub fn create_merkle_trees_for_all_repos_down(_path: &Path) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}

pub fn create_merkle_trees_down(_repo: &LocalRepository) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}
