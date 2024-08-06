use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use time::OffsetDateTime;

use super::Migrate;

use crate::core::db;
use crate::core::db::key_val::str_val_db;
use crate::core::db::key_val::tree_db::{TreeObject, TreeObjectChild};
use crate::core::db::merkle::merkle_node_db::MerkleNodeDB;
use crate::core::v1::index::object_db_reader::get_object_reader;
use crate::core::v1::index::{
    CommitDirEntryReader, CommitEntryReader, CommitReader, ObjectDBReader,
};
// use crate::core::v2::index::file_chunker::{ChunkShardManager, FileChunker};
use crate::core::v2::index::merkle_tree::node::*;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util::progress_bar::{oxen_progress_bar, spinner_with_msg, ProgressBarType};
use crate::{constants, repositories, util};

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
            .join(constants::TREE_DIR)
            .join(constants::NODES_DIR);
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
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        // Show the canonical namespace path
        log::debug!(
            "This is the namespace path we're walking: {:?}",
            namespace_path.canonicalize()?
        );
        let repos = repositories::list_repos_in_namespace(&namespace_path);
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
    println!("👋 Starting to migrate merkle trees for {:?}", repo.path);

    // Get all commits in repo, then construct merkle tree for each commit
    let commit_reader = CommitReader::new(repo)?;
    let all_commits = commit_reader.list_all()?;
    // sort these by timestamp from oldest to newest
    let mut all_commits = all_commits.clone();
    all_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    println!("Migrate {} commits for {:?}", all_commits.len(), repo.path);

    // Setup these object readers and entry readers to help pre-compute of latest commit for each file
    let mut object_readers: Vec<Arc<ObjectDBReader>> = Vec::new();
    for commit in &all_commits {
        object_readers.push(get_object_reader(repo, &commit.id)?);
    }

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
    for (commit_idx, _) in all_commits.iter().enumerate() {
        // Populate the global merkle tree from the old objects dir
        migrate_merkle_tree(
            repo,
            &commit_reader,
            &all_commits,
            commit_idx,
            &object_readers,
        )?;

        bar.inc(1);
    }

    Ok(())
}

fn migrate_merkle_tree(
    repo: &LocalRepository,
    commit_reader: &CommitReader,
    commits: &Vec<Commit>,
    commit_idx: usize,
    object_readers: &Vec<Arc<ObjectDBReader>>,
) -> Result<(), OxenError> {
    let commit = &commits[commit_idx];
    println!("== START Migrating merkle tree for commit: {} ==", commit);
    let commit_dir = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::HISTORY_DIR)
        .join(&commit.id);

    log::debug!("Checking if commit dir exists: {:?}", commit_dir);
    if !commit_dir.exists() {
        log::warn!("Skipping commit {:?}, not downloaded", commit.id);
        return Ok(());
    }

    // Get the entry reader for the commit
    let entry_reader = CommitEntryReader::new(repo, commit)?;

    // Get the root hash
    let dir_hashes_dir = commit_dir.join(constants::DIR_HASHES_DIR);

    let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&db::key_val::opts::default(), dir_hashes_dir, false)?;
    let hash: String = str_val_db::get(&dir_hashes_db, "")?.unwrap();
    let hash = hash.replace('"', "");

    let dir_path = Path::new("");

    let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
    for (i, c) in commits.iter().enumerate() {
        let reader = CommitDirEntryReader::new(repo, &c.id, dir_path, object_readers[i].clone())?;
        commit_entry_readers.push((c.clone(), reader));
    }

    // Write the buckets to the dir db
    let tree_path = repo
        .path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(constants::NODES_DIR)
        .join(&commit.id);

    // Create a directory of vnodes
    let mut dir_db = MerkleNodeDB::open_read_write(tree_path)?;
    dir_db.write_meta("", MerkleTreeNodeType::Root, 1)?;

    write_dir_node(
        repo,
        commit,
        commit_idx,
        &entry_reader,
        object_readers,
        &commit_entry_readers,
        &mut dir_db,
        &dir_path,
        &hash,
    )?;

    migrate_dir(
        repo,
        commits,
        commit_idx,
        commit_reader,
        &entry_reader,
        object_readers,
        dir_path,
        "",
        &hash,
    )?;

    Ok(())
}

fn migrate_dir(
    repo: &LocalRepository,
    commits: &Vec<Commit>,
    commit_idx: usize,
    commit_reader: &CommitReader,
    entry_reader: &CommitEntryReader,
    object_readers: &Vec<Arc<ObjectDBReader>>,
    dir_path: &Path, // full path to dir (/path/to/dir)
    dir_name: &str,  // base name of dir (dir)
    dir_hash: &str,
) -> Result<(), OxenError> {
    // Read the values from the .oxen/objects/dirs db and write them
    // to the proper .oxen/tree/{path} with their hash as the key and type
    // and metadata as the value
    //
    println!(
        "Processing dir path [{:?}] hash [{}] for commit {}",
        dir_path,
        dir_hash,
        commits[commit_idx]
    );

    /*
    The number of VNodes is dynamic depending on the number of children in
    the directory.

    This helps us with reads/writes making them lean if we have many
    children in a directory.

    N = Number of Children
    M = Number of VNodes

    If we want each bucket to be ~10,000 entries

    Should be N / (2^M) <= 10,000, solve for M
    N / 10,000 = (2^M)
    M = log2(N / 10000)

    It's logarithmic, because we don't want too many vnodes per dir

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

    let commit = &commits[commit_idx];
    let dir_hash = &dir_hash.replace('"', "");
    let obj_reader = &object_readers[commit_idx];
    let dir_obj = obj_reader.get_dir(dir_hash)?;

    let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
    for (i, c) in commits.iter().enumerate() {
        let reader = CommitDirEntryReader::new(repo, &c.id, dir_path, object_readers[i].clone())?;
        commit_entry_readers.push((c.clone(), reader));
    }

    let Some(dir_obj) = dir_obj else {
        return Err(OxenError::basic_str(format!(
            "could not get dir objects for {}",
            dir_hash
        )));
    };

    // Write all the VNodes
    let mut children: Vec<TreeObjectChild> = Vec::new();
    for child in dir_obj.children() {
        if let TreeObjectChild::VNode { path: _, hash } = child {
            let vnode_obj = obj_reader
                .get_vnode(hash)?
                .expect("could not get vnode object");

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
        .join(constants::NODES_DIR)
        .join(&dir_hash);

    // Create a directory of vnodes
    let mut dir_db = MerkleNodeDB::open_read_write(tree_path)?;
    dir_db.write_meta(dir_name, MerkleTreeNodeType::Dir, num_vnodes as u64)?;
    for (i, bhash) in bucket_hashes.iter().enumerate() {
        let shash = format!("{:x}", bhash);
        println!("Bucket [{}] for {:?}", i, shash);
        let node = VNode { id: i as u32 };
        dir_db.write_one(*bhash, MerkleTreeNodeType::VNode, &node)?;
    }

    // Re-Write the N vnodes
    for (i, bucket) in buckets.iter().enumerate() {
        let uhash = bucket_hashes[i];
        let shash = format!("{:x}", uhash);

        let node_path = repo
            .path
            .join(constants::OXEN_HIDDEN_DIR)
            .join(constants::TREE_DIR)
            .join(constants::NODES_DIR)
            .join(&shash);

        if node_path.exists() {
            println!(
                "vnode database already exists at tree_path: {:?}",
                node_path
            );
            return Ok(());
        }

        println!("Writing vnodes to path: {:?}", node_path);

        // Write the children of the VNodes
        let mut node_db = MerkleNodeDB::open_read_write(&node_path)?;
        let num_children = bucket.len();
        node_db.write_meta(&shash, MerkleTreeNodeType::VNode, num_children as u64)?;
        for (j, child) in bucket.iter().enumerate() {
            let (dtype, hash, path) = match child {
                TreeObjectChild::VNode { path, hash } => (MerkleTreeNodeType::VNode, hash, path),
                TreeObjectChild::File { path, hash } => (MerkleTreeNodeType::File, hash, path),
                TreeObjectChild::Dir { path, hash } => (MerkleTreeNodeType::Dir, hash, path),
                TreeObjectChild::Schema { path, hash } => (MerkleTreeNodeType::Schema, hash, path),
            };

            if j % 1000 == 0 {
                log::debug!("writing child {} {:?} {}", j, dtype, path.display());
            }

            match dtype {
                MerkleTreeNodeType::Root => {
                    // pass, we never explicitly write the root
                    panic!("migrate_dir should not get to Root");
                }
                MerkleTreeNodeType::VNode => {
                    // pass, we already wrote the vnode
                    panic!("migrate_dir should not get to VNode");
                }
                MerkleTreeNodeType::FileChunk => {
                    // pass, we do this in migrate_file
                    panic!("migrate_dir should not get to FileChunk");
                }
                MerkleTreeNodeType::File => {
                    // If it's a file, let's chunk it and make the chunk leaf nodes
                    write_file_node(
                        &entry_reader,
                        &commit_entry_readers,
                        &mut node_db,
                        path,
                        hash,
                    )?;
                }
                MerkleTreeNodeType::Dir => {
                    write_dir_node(
                        repo,
                        commit,
                        commit_idx,
                        &entry_reader,
                        object_readers,
                        &commit_entry_readers,
                        &mut node_db,
                        path,
                        hash,
                    )?;
                    // Recurse if it's a directory
                    let file_name = path.file_name().unwrap().to_str().unwrap();
                    migrate_dir(
                        repo,
                        commits,
                        commit_idx,
                        commit_reader,
                        &entry_reader,
                        object_readers,
                        path,
                        file_name,
                        hash,
                    )?;
                }
                MerkleTreeNodeType::Schema => {
                    // Schema we can directly write
                    let file_name = path.file_name().unwrap().to_str().unwrap();
                    let val = SchemaNode {
                        name: file_name.to_owned(),
                    };
                    let uhash = u128::from_str_radix(hash, 16).expect("Failed to parse hex string");
                    // println!("Bucket [{}] Val [{}] {} for {:?}", i, j, hash, val);
                    node_db.write_one(uhash, MerkleTreeNodeType::Schema, &val)?;
                }
            }
        }
    }
    Ok(())
}

fn write_dir_node(
    repo: &LocalRepository,
    commit: &Commit,
    commit_idx: usize,
    entry_reader: &CommitEntryReader,
    object_readers: &Vec<Arc<ObjectDBReader>>,
    commit_entry_readers: &[(Commit, CommitDirEntryReader)],
    node_db: &mut MerkleNodeDB,
    path: &Path,
    hash: &str,
) -> Result<(), OxenError> {
    println!("Write dir node for path [{:?}] and hash [{}]", path, hash);
    let file_name = path.file_name().unwrap_or_default().to_str().unwrap();
    let uhash = u128::from_str_radix(hash, 16).expect("Failed to parse hex string");

    // TODO Compute num_bytes, last_commit_id, last_modified_seconds, last_modified_nanoseconds
    let mut num_bytes = 0;
    let mut last_commit_id = 0;
    let mut last_commit_timestamp: OffsetDateTime = OffsetDateTime::from_unix_timestamp(0).unwrap();
    let mut last_modified_seconds = 0;
    let mut last_modified_nanoseconds = 0;
    let mut data_type_counts: HashMap<String, usize> = HashMap::new();

    let mut entries_processed = 0;

    // List dir children
    let dirs = entry_reader.list_dirs()?;

    let dirs: Vec<PathBuf> = dirs
        .iter()
        .filter(|dir| dir.starts_with(path) || Path::new("") == path)
        .map(|dir| dir.to_path_buf())
        .collect();

    let progress_bar = spinner_with_msg("Processing dir children");
    println!("Processing subdirs for path [{:?}] children [{}]", path, dirs.len());

    for dir in dirs {
        log::debug!("processing dir: {:?}", dir);
        let dir_entry_reader =
            CommitDirEntryReader::new(repo, &commit.id, &dir, object_readers[commit_idx].clone())?;

        let mut readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
        for (i, (c, _)) in commit_entry_readers.iter().enumerate() {
            if c.timestamp > last_commit_timestamp {
                let reader = CommitDirEntryReader::new(repo, &c.id, &dir, object_readers[i].clone())?;
                readers.push((c.clone(), reader));
            }
        }

        let entries = dir_entry_reader.list_entries()?;
        for entry in entries {
            num_bytes += entry.num_bytes;

            // If we have less than 2 readers, we know the latest commit is the one we're processing
            if readers.len() > 1 {
                log::debug!("checking latest commit on {:?} readers: {:?}", entry.path, readers.len());
                let Some(latest_commit) =
                    repositories::entries::get_latest_commit_for_entry(&readers, &entry)?
                else {
                    log::error!(
                        "Skipping entry {:?}, could not get latest commit",
                        entry.path
                    );
                    continue;
                };
                if latest_commit.timestamp > last_commit_timestamp {
                    last_commit_timestamp = latest_commit.timestamp;
                    let commit_id = &latest_commit.id;
                    // convert string hash to u128
                    last_commit_id =
                        u128::from_str_radix(&commit_id, 16).expect("Failed to parse hex string");
                    last_modified_seconds = entry.last_modified_seconds;
                    last_modified_nanoseconds = entry.last_modified_nanoseconds;
                    log::debug!(
                        "Setting latest commit: {} for {:?} {} {} {}",
                        latest_commit,
                        entry.path,
                        entry.num_bytes,
                        commit_id,
                        last_commit_timestamp
                    );
                }
            }

            entries_processed += 1;

            let data_type = util::fs::data_type_from_extension(&entry.path);
            let data_type_str = format!("{}", data_type);
            data_type_counts
                .entry(data_type_str)
                .and_modify(|count| *count += 1)
                .or_insert(1);

            progress_bar.set_message(format!(
                "Processing {:?} children: {} ({})",
                path,
                entries_processed,
                bytesize::ByteSize::b(num_bytes)
            ));
        }
    }

    let val = DirNode {
        name: file_name.to_owned(),
        hash: uhash,
        num_bytes,
        last_commit_id,
        last_modified_seconds,
        last_modified_nanoseconds,
        data_type_counts,
    };
    let uhash = u128::from_str_radix(hash, 16).expect("Failed to parse hex string");
    println!("Writing dir node {:?} to {:?}", val, node_db.path());
    node_db.write_one(uhash, MerkleTreeNodeType::Dir, &val)?;
    Ok(())
}

fn write_file_node(
    entry_reader: &CommitEntryReader,
    commit_entry_readers: &[(Commit, CommitDirEntryReader)],
    node_db: &mut MerkleNodeDB,
    path: &Path,
    hash: &str,
) -> Result<(), OxenError> {
    let obj_reader = entry_reader.get_obj_reader();
    // read other meta data from file object
    let file_obj = obj_reader
        .get_file(hash)?
        .ok_or(OxenError::basic_str(format!(
            "could not get file object for {}",
            hash
        )))?;

    let (num_bytes, last_modified_seconds, last_modified_nanoseconds) = match file_obj {
        TreeObject::File {
            num_bytes,
            last_modified_seconds,
            last_modified_nanoseconds,
            ..
        } => (num_bytes, last_modified_seconds, last_modified_nanoseconds),
        _ => return Err(OxenError::basic_str("file object is not a file")),
    };

    // TODO: Find the last commit id for the file
    let latest_commit =
        repositories::entries::get_latest_commit_for_path(commit_entry_readers, path)?.unwrap();
    let last_commit_id =
        u128::from_str_radix(&latest_commit.id, 16).expect("Failed to parse hex string");

    // Chunk the file into 16kb chunks
    /* TODO: This is hard / inefficient to read into Polars for now, ignore
    let commit_entry = reader.get_entry(path)?.ok_or(OxenError::basic_str(format!(
        "could not get file entry for {}",
        path.display()
    )))?;
    let chunker = FileChunker::new(repo);
    let mut csm = ChunkShardManager::new(repo)?;
    csm.open_for_write()?;
    let chunks = chunker.save_chunks(&commit_entry, &mut csm)?;
     */

    // For now, we just have one chunk per file
    let uhash: u128 = u128::from_str_radix(hash, 16).expect("Failed to parse hex string");
    let chunks: Vec<u128> = vec![uhash];

    // Then start refactoring the commands into a "legacy" module so we can still make the old
    // dbs but start implementing them with the new merkle object
    let file_name = path.file_name().unwrap().to_str().unwrap();
    let val = FileNode {
        name: file_name.to_owned(),
        hash: uhash,
        num_bytes,
        chunk_type: FileChunkType::SingleFile,
        storage_backend: FileStorageType::Disk,
        last_commit_id,
        last_modified_seconds,
        last_modified_nanoseconds,
        chunk_hashes: chunks,
    };
    node_db.write_one(uhash, MerkleTreeNodeType::File, &val)?;

    // TODO
    // * Look at the oxen pack command and abstract out this logic
    // * Store the chunks in the .oxen/objects/chunks dir (next to .oxen/objects/schemas)
    // * The file node object will need to be different than the other tree node objects
    //     * file_idx -> chunk_hash
    //     * we will want to store the enum of the type at the top of the node file, so we know what to deserialize
    // * The chunk dir db (.oxen/objects/chunks) will need chunk_hash -> chunk
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
