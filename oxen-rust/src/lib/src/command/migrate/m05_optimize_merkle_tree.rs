use rocksdb::{DBWithThreadMode, MultiThreaded};

use super::Migrate;

use std::collections::HashSet;
use std::path::{Path};

use crate::core::db::merkle_node_db::MerkleNodeDB;
use crate::core::db::tree_db::TreeObjectChild;
use crate::core::db::{self, str_val_db};
use crate::core::index::commit_merkle_tree::{MerkleNode, MerkleNodeType};
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
        DBWithThreadMode::open_for_read_only(&db::opts::default(), dir_hashes_dir, false)?;
    let hash: String = str_val_db::get(&dir_hashes_db, "")?.unwrap();

    // let root = CommitMerkleTreeNode {
    //     path: PathBuf::from(""),
    //     hash,
    //     dtype: MerkleTreeNodeType::Dir,
    //     children: HashSet::new(),
    // };

    migrate_dir(repo, &object_reader, &hash)?;

    Ok(())
}

fn migrate_dir(
    repo: &LocalRepository,
    reader: &ObjectDBReader,
    dir_hash: &str,
) -> Result<(), OxenError> {
    // Read the values from the .oxen/objects/dirs db and write them
    // to the proper .oxen/tree/{path} with their hash as the key and type
    // and metadata as the value
    //
    println!("Getting dir for node: {:?}", dir_hash);

    /*
    To tune this...
    Let's read in all the VNodes, and make this more configurable
    about how many files are within each VNode.

    Compute number of VNode Buckets based on number of children.

    N = Number of Children
    M = Number of VNodes

    If we want each bucket to be ~10,000 entries

    Should be N / (2^M) <= 10,000, solve for M
    N / 10,000 = (2^M)
    M = log2(N / 10000)

    Or...we could just divide by the node count we want...?
    But I think I'd rather it be logarithmic than linear?
    TODO: Plot this function

    * log2(1,000,000 / 10,000)
        * 1,000,000,000 / (2^16) = 1,000,000,000 / 65,536 = 15,258
            * 65,536 VNodes
            * 15,258 Children Per VNode
        * 1,000,000 / (2^6) = 1,000,000 / 64 = 15,625
            * 64 VNodes
            * 15,625 Children Per VNode
        * 500,000 / (2^5) = 500,000 / 32 = 15,625
            * 32 VNodes
            * 15,258 Children Per VNode
        * 200,000 / (2^4) = 200,000 / 16 = 12,500
            * 16 VNodes
            * 12,5000 Children Per VNode
    */

    let dir_hash = &dir_hash.replace('"', "");
    let dir_obj = reader.get_dir(dir_hash)?;

    let Some(dir_obj) = dir_obj else {
        return Err(OxenError::basic_str(format!(
            "could not get dir objects for {}",
            dir_hash
        )));
    };

    // Write all the VNodes
    // let tree_db: DBWithThreadMode<MultiThreaded> =
    //     DBWithThreadMode::open(&db::opts::default(), &tree_path)?;

    // TODO: We have to read/flatten each one of these VNodes
    //       Collect the subnodes, count them, save the count
    //       then bucket them based on count

    let mut children: Vec<TreeObjectChild> = Vec::new();
    for child in dir_obj.children() {
        if let TreeObjectChild::VNode { path: _, hash } = child {
            let vnode_obj = reader.get_vnode(hash)?.expect("could not get vnode object");

            for child in vnode_obj.children() {
                children.push(child.clone());
            }
        }
    }

    // log2(N / 10000)
    let total_children = children.len();
    let num_vnodes = (total_children as f32 / 10000_f32).log2();
    let num_vnodes = 2u128.pow(num_vnodes.ceil() as u32);
    println!("{} VNodes for {} children", num_vnodes, total_children);

    // Group the children into their buckets
    let mut buckets: Vec<Vec<TreeObjectChild>> = vec![Vec::new(); num_vnodes as usize];
    for child in children {
        let hash_int = u128::from_str_radix(child.hash(), 16).expect("Failed to parse hex string");
        let bucket = hash_int % num_vnodes;
        buckets[bucket as usize].push(child);
    }

    // Compute new hashes for each bucket
    let mut bucket_hashes: Vec<u128> = vec![0; num_vnodes as usize];
    for (i, bucket) in buckets.iter().enumerate() {
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        for child in bucket {
            // TODO: child.hash() is a string and we should just use
            //       the u128 hash for speed and consistency
            hasher.update(child.hash().as_bytes());
        }
        bucket_hashes[i] = hasher.digest128();
    }

    // Write the buckets to the dir db
    let tree_path = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(dir_hash);

    let mut dir_db = MerkleNodeDB::open(tree_path, false)?;
    dir_db.write_size(num_vnodes as u64)?;
    for (i, bhash) in bucket_hashes.iter().enumerate() {
        let shash = format!("{:x}", bhash);
        println!("Bucket [{}] for {:?}", i, shash);
        let node = MerkleNode {
            dtype: MerkleNodeType::VNode,
            path: shash,
        };
        dir_db.write_one(*bhash, &node)?;
    }

    // Re-Write the N vnodes
    for (i, bucket) in buckets.iter().enumerate() {
        let uhash = bucket_hashes[i];
        let shash = format!("{:x}", uhash);
        let tree_path = repo
            .path
            .join(constants::OXEN_HIDDEN_DIR)
            .join(constants::TREE_DIR)
            .join(&shash);

        if tree_path.exists() {
            println!(
                "vnode database already exists at tree_path: {:?}",
                tree_path
            );
            return Ok(());
        }

        println!("Writing vnodes to path: {:?}", tree_path);

        // Write the children of the VNodes
        let mut tree_db = MerkleNodeDB::open(&tree_path, false)?;
        let num_children = bucket.len();
        tree_db.write_size(num_children as u64)?;
        for (j, child) in bucket.iter().enumerate() {
            let (dtype, hash, path) = match child {
                TreeObjectChild::VNode { path, hash } => (MerkleNodeType::VNode, hash, path),
                TreeObjectChild::File { path, hash } => (MerkleNodeType::File, hash, path),
                TreeObjectChild::Dir { path, hash } => (MerkleNodeType::Dir, hash, path),
                TreeObjectChild::Schema { path, hash } => (MerkleNodeType::Schema, hash, path),
            };

            if MerkleNodeType::VNode != dtype {
                let file_name = path.file_name().unwrap().to_str().unwrap();
                let val = MerkleNode {
                    dtype: dtype.clone(),
                    path: file_name.to_owned(),
                };
                let uhash = u128::from_str_radix(hash, 16).expect("Failed to parse hex string");
                println!("Bucket [{}] Val [{}] {} for {:?}", i, j, hash, val);
                tree_db.write_one(uhash, &val)?;
            }

            // Recurse if it's a directory
            if MerkleNodeType::Dir == dtype {
                migrate_dir(repo, reader, hash)?;
            }
        }
    }
    Ok(())
}

fn migrate_files(
    repo: &LocalRepository,
    reader: &ObjectDBReader,
    vnode: &TreeObjectChild,
) -> Result<(), OxenError> {
    match vnode {
        TreeObjectChild::VNode { path, hash } => {
            let tree_path = repo
                .path
                .join(constants::OXEN_HIDDEN_DIR)
                .join(constants::TREE_DIR)
                .join(hash);

            if tree_path.exists() {
                println!(
                    "database {:?} already exists at tree_path: {:?}",
                    path, tree_path
                );
                return Ok(());
            }

            println!("writing children {:?} to tree_path: {:?}", path, tree_path);

            // let tree_db: DBWithThreadMode<MultiThreaded> =
            //     DBWithThreadMode::open(&db::opts::default(), &tree_path)?;

            let tree_obj = reader.get_vnode(hash)?;
            let Some(tree_obj) = tree_obj else {
                return Err(OxenError::basic_str(format!(
                    "could not get children objects for vnode {}",
                    hash
                )));
            };

            let mut tree_db = MerkleNodeDB::open(&tree_path, false)?;
            tree_db.write_size(tree_obj.children().len() as u64)?;

            for child in tree_obj.children() {
                match child {
                    TreeObjectChild::File { path, hash } => {
                        let val = MerkleNode {
                            dtype: MerkleNodeType::File,
                            path: path.file_name().unwrap().to_str().unwrap().to_owned(),
                        };
                        // let mut buf = Vec::new();
                        // val.serialize(&mut Serializer::new(&mut buf)).unwrap();
                        // tree_db.put(hash.as_bytes(), &buf)?;
                        let hash_int =
                            u128::from_str_radix(hash, 16).expect("Failed to parse hex string");
                        tree_db.write_one(hash_int, &val)?;
                    }
                    TreeObjectChild::Dir { path, hash } => {
                        let file_name = path.file_name().unwrap().to_str().unwrap();
                        let val = MerkleNode {
                            dtype: MerkleNodeType::Dir,
                            path: file_name.to_owned(),
                        };
                        // let mut buf = Vec::new();
                        // val.serialize(&mut Serializer::new(&mut buf)).unwrap();
                        // tree_db.put(hash.as_bytes(), &buf)?;
                        let hash_int =
                            u128::from_str_radix(hash, 16).expect("Failed to parse hex string");
                        tree_db.write_one(hash_int, &val)?;

                        let dir = CommitMerkleTreeNode {
                            path: path.to_owned(),
                            hash: hash.to_owned(),
                            dtype: MerkleTreeNodeType::Dir,
                            children: HashSet::new(),
                        };
                        // migrate_vnodes(repo, reader, &dir)?;
                    }
                    TreeObjectChild::Schema { path, hash } => {
                        let val = MerkleNode {
                            dtype: MerkleNodeType::Schema,
                            path: path.file_name().unwrap().to_str().unwrap().to_owned(),
                        };
                        // let mut buf = Vec::new();
                        // val.serialize(&mut Serializer::new(&mut buf)).unwrap();
                        // tree_db.put(hash.as_bytes(), &buf)?;
                        let hash_int =
                            u128::from_str_radix(hash, 16).expect("Failed to parse hex string");
                        tree_db.write_one(hash_int, &val)?;
                    }
                    _ => {
                        return Err(OxenError::basic_str(format!(
                            "unexpected child type: {:?}",
                            child
                        )));
                    }
                }
            }
        }
        _ => {
            return Err(OxenError::basic_str(format!(
                "unexpected child type: {:?}",
                vnode
            )));
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
