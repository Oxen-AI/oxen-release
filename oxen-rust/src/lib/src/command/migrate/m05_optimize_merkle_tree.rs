use rocksdb::{DBWithThreadMode, MultiThreaded};

use super::Migrate;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::core::db::tree_db::TreeObjectChild;
use crate::core::db::{self, str_val_db};
use crate::core::index::commit_merkle_tree::CommitMerkleTree;
use crate::core::index::commit_merkle_tree_node::{CommitMerkleTreeNode, MerkleTreeNodeType};
use crate::core::index::{CommitReader, ObjectDBReader};
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

    // Clear tree dir if exists (in order to run migration many times)
    let tree_dir = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR);

    if tree_dir.exists() {
        println!("Clearing tree dir: {:?}", tree_dir);
        util::fs::remove_dir_all(&tree_dir)?;
    } else {
        // Create tree dir
        util::fs::create_dir_all(&tree_dir)?;
    }

    let bar = oxen_progress_bar(all_commits.len() as u64, ProgressBarType::Counter);
    // let commit_writer = CommitWriter::new(repo)?;
    for commit in all_commits {
        // Populate the global merkle tree from the old objects dir
        migrate_merkle_tree(repo, &commit)?;

        bar.inc(1);
    }

    Ok(())
}

fn migrate_merkle_tree(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    // Instantiate the object reader, most expensive operation
    let object_reader = ObjectDBReader::new(repo)?;

    // // iterate over the commit tree to get the root nodes
    // let commit_merkle_tree = CommitMerkleTree::new(repo, commit)?;
    // println!("Commit {} -> '{}' merkle tree:", commit.id, commit.message);
    // commit_merkle_tree.print();

    // Get the root hash
    let dir_hashes_dir = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::HISTORY_DIR)
        .join(&commit.id)
        .join(constants::DIR_HASHES_DIR);
    let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&db::opts::default(), &dir_hashes_dir, false)?;
    let hash: String = str_val_db::get(&dir_hashes_db, "")?.unwrap();

    let root = CommitMerkleTreeNode {
        path: PathBuf::from(""),
        hash: hash,
        dtype: MerkleTreeNodeType::Dir,
        data: ".".to_string(),
        children: HashSet::new(),
    };

    migrate_vnodes(repo, &object_reader, &root)?;

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
    println!("Getting object for node: {:?} -> {:?}", node.path, node.hash);
    let hash = &node.hash.replace("\"", "");
    let obj = reader.get_dir(&hash)?;

    let Some(tree_obj) = obj else {
        return Err(OxenError::basic_str(format!("could not get dir objects for {}", node.hash)));
    };

    // These should all be vnodes, so write them to .oxen/tree/{node.path}/{node.hash}
    let tree_path = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(&hash);
    println!("Writing vnodes to path: {:?}", tree_path);

    // Write all the VNodes
    let tree_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&db::opts::default(), &tree_path)?;
    for child in tree_obj.children() {
        match child {
            TreeObjectChild::VNode { path, hash } => {
                println!("vnode: {:?} -> {:?}", path, hash);
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
                .join(&hash);
            println!("writing files to tree_path: {:?}", tree_path);

            let tree_db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&db::opts::default(), &tree_path)?;
            
            let tree_obj = reader.get_vnode(hash)?;
            let Some(tree_obj) = tree_obj else {
                return Err(OxenError::basic_str(format!("could not get vnode objects for {}", node.hash)));
            };
            for child in tree_obj.children() {
                match child {
                    TreeObjectChild::File { path, hash } => {
                        let child_json = serde_json::to_string(&serde_json::json!({
                            "File": {"path": path.file_name().unwrap().to_str().unwrap()}
                        }))?;
                        println!("\tfile: {:?}", child_json);
                        tree_db.put(hash.as_bytes(), child_json.as_bytes())?;
                    }
                    TreeObjectChild::Dir { path, hash } => {
                        let file_name = path.file_name().unwrap().to_str().unwrap();
                        let child_json = serde_json::to_string(&serde_json::json!({
                            "Dir": {"path": file_name}
                        }))?;
                        println!("\tdir: {:?}", child_json);
                        tree_db.put(hash.as_bytes(), child_json.as_bytes())?;

                        let dir = CommitMerkleTreeNode {
                            path: path.to_owned(),
                            hash: hash.to_owned(),
                            dtype: MerkleTreeNodeType::Dir,
                            data: file_name.to_owned(),
                            children: HashSet::new(),
                        };
                        migrate_vnodes(repo, &reader, &dir)?;
                    }
                    TreeObjectChild::Schema { path, hash } => {
                        let child_json = serde_json::to_string(&serde_json::json!({
                            "Schema": {"path": path.file_name().unwrap().to_str().unwrap()}
                        }))?;
                        println!("\tschema: {:?}", child_json);
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
