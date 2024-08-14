use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;

use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, SingleThreaded};
use std::path::PathBuf;
use std::str;
use std::time::Duration;
use std::time::Instant;
use time::OffsetDateTime;

use crate::config::UserConfig;
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::constants::{HEAD_FILE, STAGED_DIR};
use crate::core::db;
use crate::core::db::key_val::path_db;
use crate::core::db::merkle::merkle_node_db::MerkleNodeDB;
use crate::core::v0_10_0::index::RefWriter;
use crate::core::v0_19_0::index::merkle_tree::node::{
    FileChunkType, FileNode, FileStorageType, MerkleTreeNodeType, VNode,
};
use crate::core::v0_19_0::index::merkle_tree::CommitMerkleTree;
use crate::core::v0_19_0::status;
use crate::core::v0_19_0::structs::EntryMetaDataWithPath;
use crate::error::OxenError;
use crate::model::NewCommit;
use crate::model::{Commit, EntryDataType, LocalRepository};

use crate::{repositories, util};

use super::merkle_tree::node::MerkleTreeNodeData;
use super::merkle_tree::node::{CommitNode, DirNode};

#[derive(Clone)]
pub struct EntryVNode {
    pub id: u128,
    pub entries: Vec<EntryMetaDataWithPath>,
}

impl EntryVNode {
    pub fn new(id: u128) -> Self {
        EntryVNode {
            id,
            entries: vec![],
        }
    }
}

pub fn commit(repo: &LocalRepository, message: impl AsRef<str>) -> Result<Commit, OxenError> {
    // time the commit
    let start_time = Instant::now();
    let message = message.as_ref();

    // Read the staged files from the staged db
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let read_progress = ProgressBar::new_spinner();
    read_progress.set_style(ProgressStyle::default_spinner());
    read_progress.enable_steady_tick(Duration::from_millis(100));

    // Read all the staged entries
    let dir_entries = status::read_staged_entries(&db, &read_progress)?;

    // if the HEAD file exists, we have parents
    // otherwise this is the first commit
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);

    let maybe_head_commit = if head_path.exists() {
        let commit = repositories::commits::head_commit(repo)?;
        Some(commit)
    } else {
        None
    };

    let mut parent_ids = vec![];
    if let Some(parent) = &maybe_head_commit {
        parent_ids.push(parent.hash_u128());
    }

    let mut merkle_tree: Option<CommitMerkleTree> = None;
    if let Some(commit) = &maybe_head_commit {
        merkle_tree = Some(CommitMerkleTree::from_commit(repo, &commit)?);
    }

    // TODO: Second commit
    //       - Find the vnodes we need to copy/modify
    //       - Write new vnodes for updated/added/deleted files
    // Right now it is just making a new commit with the staged files, not connecting them to the merkle tree

    // Sort children and split into VNodes
    let vnode_entries = split_into_vnodes(&dir_entries, &merkle_tree)?;

    // Compute the commit hash
    let cfg = UserConfig::get()?;
    let timestamp = OffsetDateTime::now_utc();
    let new_commit = NewCommit {
        parent_ids: parent_ids.iter().map(|id| format!("{:x}", id)).collect(),
        message: message.to_string(),
        author: cfg.name.clone(),
        email: cfg.email.clone(),
        timestamp: timestamp,
    };
    let commit_id = compute_commit_id(&new_commit, &vnode_entries)?;

    let node = CommitNode {
        id: commit_id,
        parent_ids: parent_ids,
        message: message.to_string(),
        author: cfg.name,
        email: cfg.email,
        timestamp: timestamp,
        ..Default::default()
    };

    let mut commit_db = MerkleNodeDB::open_read_write(repo, &node)?;
    write_commit_entries(
        &repo,
        commit_id,
        &mut commit_db,
        &vnode_entries,
        &read_progress,
    )?;
    read_progress.finish_and_clear();

    // Write HEAD file and update branch
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);
    log::debug!("Looking for HEAD file at {:?}", head_path);
    if !head_path.exists() {
        log::debug!("HEAD file does not exist, creating new branch");
        let ref_writer = RefWriter::new(repo)?;
        let branch_name = DEFAULT_BRANCH_NAME;
        let commit_id = format!("{:x}", commit_id);
        ref_writer.set_head(branch_name);
        ref_writer.set_branch_commit_id(branch_name, &commit_id)?;
        ref_writer.set_head_commit_id(&commit_id)?;
    }

    // Clear the staged db
    path_db::clear(&db)?;

    // Print that we finished
    println!("🐂 commit {:x} in {:?}", commit_id, start_time.elapsed());

    Ok(node.to_commit())
}

fn node_data_to_entry(
    base_dir: impl AsRef<Path>,
    node: &MerkleTreeNodeData,
) -> Result<Option<EntryMetaDataWithPath>, OxenError> {
    let base_dir = base_dir.as_ref();
    match node.dtype {
        MerkleTreeNodeType::Dir => {
            let dir_node = node.dir()?;
            Ok(Some(EntryMetaDataWithPath {
                path: base_dir.join(dir_node.name),
                data_type: EntryDataType::Dir,
                hash: node.hash,
                num_bytes: dir_node.num_bytes,
            }))
        }
        MerkleTreeNodeType::File => {
            let file_node = node.file()?;
            Ok(Some(EntryMetaDataWithPath {
                path: base_dir.join(file_node.name),
                data_type: file_node.data_type,
                hash: node.hash,
                num_bytes: file_node.num_bytes,
            }))
        }
        _ => Ok(None),
    }
}

fn get_existing_dir_children(
    tree: &Option<CommitMerkleTree>,
    dir_path: impl AsRef<Path>,
) -> Result<HashSet<EntryMetaDataWithPath>, OxenError> {
    if tree.is_none() {
        return Ok(HashSet::new());
    }

    let dir_path = dir_path.as_ref().to_path_buf();
    let mut children = HashSet::new();

    if let Some(tree) = tree {
        let dir_children = tree.dir_files_and_folders(&dir_path)?;
        children = dir_children
            .into_iter()
            .map(|child| node_data_to_entry(&dir_path, &child))
            .flatten()
            .flatten()
            .collect();
    }

    Ok(children)
}

fn split_into_vnodes(
    entries: &HashMap<PathBuf, Vec<EntryMetaDataWithPath>>,
    tree: &Option<CommitMerkleTree>,
) -> Result<HashMap<PathBuf, Vec<EntryVNode>>, OxenError> {
    let mut results: HashMap<PathBuf, Vec<EntryVNode>> = HashMap::new();

    // Get all the directories in the merkle tree
    let mut dirs: HashSet<PathBuf> = HashSet::new();
    if let Some(tree) = tree {
        dirs = HashSet::from_iter(tree.list_dir_paths()?);
    }

    // Get all the directories in the entries
    for (directory, _) in entries.iter() {
        dirs.insert(directory.clone());
    }

    log::debug!("considering dirs: {:?}", dirs);

    // Create the VNode buckets per directory
    for directory in dirs.iter() {
        // Lookup children in the existing merkle tree
        let mut children = get_existing_dir_children(tree, directory)?;
        log::debug!(
            "got {} existing children for dir {:?}",
            children.len(),
            directory
        );
        if let Some(old_children) = entries.get(directory) {
            // Update the children with the new entries from status
            // TODO: Handle updates and deletes, this is pure addition right now
            for child in old_children.iter() {
                children.insert(child.clone());
            }
        }

        // log2(N / 10000)
        let total_children = children.len();
        let num_vnodes = (total_children as f32 / 10000_f32).log2();
        let num_vnodes = 2u128.pow(num_vnodes.ceil() as u32);
        log::debug!(
            "{} VNodes for {} children in {:?}",
            num_vnodes,
            total_children,
            directory
        );
        let mut vnode_children: Vec<EntryVNode> = vec![EntryVNode::new(0); num_vnodes as usize];

        // Split entries into vnodes
        for child in children.into_iter() {
            let bucket = child.hash % num_vnodes;
            vnode_children[bucket as usize].entries.push(child.clone());
        }

        // Compute hashes and sort entries
        for vnode in vnode_children.iter_mut() {
            // Sort the entries in the vnode by path
            // to make searching for entries faster
            vnode.entries.sort_by(|a, b| a.path.cmp(&b.path));

            // Compute hash for the vnode
            let mut vnode_hasher = xxhash_rust::xxh3::Xxh3::new();
            vnode_hasher.update(b"vnode");
            // make sure to hash the directory name so that it will be unique
            // even if the entries are the same (for example two empty directories at different levels)
            vnode_hasher.update(directory.to_str().unwrap().as_bytes());
            for entry in vnode.entries.iter() {
                vnode_hasher.update(&entry.hash.to_le_bytes());
            }
            vnode.id = vnode_hasher.digest128();
        }

        // Sort before we hash
        results.insert(directory.to_owned(), vnode_children);
    }

    log::debug!("split_into_vnodes results: {:?}", results.len());
    for (dir, vnodes) in results.iter() {
        log::debug!("dir {:?} has {} vnodes", dir, vnodes.len());
        for vnode in vnodes.iter() {
            log::debug!("  vnode {:?} has {} entries", vnode.id, vnode.entries.len());
            for entry in vnode.entries.iter() {
                log::debug!("    entry {:?} has {:?} bytes", entry.path, entry.num_bytes);
            }
        }
    }

    Ok(results)
}

fn compute_commit_id(
    new_commit: &NewCommit,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
) -> Result<u128, OxenError> {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.update(b"commit");
    hasher.update(format!("{:?}", new_commit.parent_ids).as_bytes());
    hasher.update(new_commit.message.as_bytes());
    hasher.update(new_commit.author.as_bytes());
    hasher.update(new_commit.email.as_bytes());
    hasher.update(&new_commit.timestamp.unix_timestamp().to_le_bytes());
    for (_, vnodes) in entries.iter() {
        for vnode in vnodes {
            for child in vnode.entries.iter() {
                hasher.update(&child.hash.to_le_bytes());
            }
        }
    }
    Ok(hasher.digest128())
}

fn write_commit_entries(
    repo: &LocalRepository,
    commit_id: u128,
    commit_db: &mut MerkleNodeDB,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
    read_progress: &ProgressBar,
) -> Result<(), OxenError> {
    // Write the root dir, then recurse into the vnodes and subdirectories
    let root_path = PathBuf::from("");
    let dir_node = compute_dir_node(commit_id, entries, &root_path)?;
    commit_db.add_child(&dir_node)?;

    let mut dir_db = MerkleNodeDB::open_read_write(repo, &dir_node)?;
    let mut total_written = 0;
    r_create_dir_node(
        repo,
        commit_id,
        &mut dir_db,
        entries,
        root_path,
        read_progress,
        &mut total_written,
    )?;

    Ok(())
}

fn r_create_dir_node(
    repo: &LocalRepository,
    commit_id: u128,
    dir_db: &mut MerkleNodeDB,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
    path: impl AsRef<Path>,
    read_progress: &ProgressBar,
    total_written: &mut u64,
) -> Result<(), OxenError> {
    let path = path.as_ref().to_path_buf();
    read_progress.set_message(format!("Committing {} entries [{:?}]", total_written, path));

    let Some(vnodes) = entries.get(&path) else {
        let err_msg = format!("No entries found for directory {:?}", path);
        return Err(OxenError::basic_str(err_msg));
    };

    log::debug!("Writing dir {:?} with {} vnodes", path, vnodes.len());
    for vnode in vnodes.iter() {
        let vnode_obj = VNode {
            id: vnode.id,
            ..Default::default()
        };
        dir_db.add_child(&vnode_obj)?;
        log::debug!(
            "Writing vnode {:x} with {} entries",
            vnode.id,
            vnode.entries.len()
        );

        *total_written += 1;
        read_progress.set_message(format!("Committing {} entries [{:?}]", total_written, path));

        let mut vnode_db = MerkleNodeDB::open_read_write(repo, &vnode_obj)?;
        for entry in vnode.entries.iter() {
            log::debug!(
                "Writing entry {:?} [{:?}] to {:?}",
                entry.path,
                entry.data_type,
                vnode_db.path()
            );
            match entry.data_type {
                EntryDataType::Dir => {
                    let dir_node = compute_dir_node(commit_id, entries, &entry.path)?;
                    vnode_db.add_child(&dir_node)?;
                    *total_written += 1;
                    read_progress
                        .set_message(format!("Committing {} entries [{:?}]", total_written, path));
                    let mut child_db = MerkleNodeDB::open_read_write(repo, &dir_node)?;
                    r_create_dir_node(
                        repo,
                        commit_id,
                        &mut child_db,
                        entries,
                        &entry.path,
                        read_progress,
                        total_written,
                    )?;
                }
                _ => {
                    let file_name = entry.path.file_name().unwrap_or_default().to_str().unwrap();

                    // Just single file chunk for now
                    let chunks = vec![entry.hash];
                    let file_node = FileNode {
                        name: file_name.to_owned(),
                        hash: entry.hash,
                        num_bytes: entry.num_bytes,
                        chunk_type: FileChunkType::SingleFile,
                        storage_backend: FileStorageType::Disk,
                        last_commit_id: commit_id,
                        last_modified_seconds: 0,
                        last_modified_nanoseconds: 0,
                        chunk_hashes: chunks,
                        data_type: entry.data_type.clone(),
                        mime_type: "".to_string(),
                        extension: "".to_string(),
                        dtype: MerkleTreeNodeType::File,
                    };
                    vnode_db.add_child(&file_node)?;
                    *total_written += 1;
                    read_progress
                        .set_message(format!("Committing {} entries [{:?}]", total_written, path));
                }
            }
        }
    }

    Ok(())
}

fn get_children(
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
    dir_path: impl AsRef<Path>,
) -> Result<Vec<PathBuf>, OxenError> {
    let dir_path = dir_path.as_ref().to_path_buf();
    let mut children = vec![];

    for (path, _) in entries.iter() {
        if path.starts_with(&dir_path) && *path != dir_path {
            children.push(path.clone());
        }
    }

    return Ok(children);
}

fn compute_dir_node(
    commit_id: u128,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
    path: impl AsRef<Path>,
) -> Result<DirNode, OxenError> {
    let path = path.as_ref().to_path_buf();
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.update(b"dir");
    hasher.update(path.to_str().unwrap().as_bytes());

    let mut num_bytes = 0;
    let mut data_type_counts: HashMap<String, usize> = HashMap::new();

    let children = get_children(entries, &path)?;
    // log::debug!("Aggregating dir {:?} with {:?} children", path, children);
    for child in children.iter() {
        let Some(vnodes) = entries.get(child) else {
            let err_msg = format!("No entries found for directory {:?}", path);
            return Err(OxenError::basic_str(err_msg));
        };

        for vnode in vnodes.iter() {
            for entry in vnode.entries.iter() {
                // log::debug!("Aggregating entry {:?} [{:?}]", entry.path, entry.data_type);
                match entry.data_type {
                    EntryDataType::Dir => {
                        log::debug!("No need to aggregate {:?}", entry.path);
                    }
                    _ => {
                        hasher.update(&entry.hash.to_le_bytes());
                        num_bytes += entry.num_bytes;

                        *data_type_counts
                            .entry(entry.data_type.to_string())
                            .or_insert(0) += 1;
                    }
                }
            }
        }
    }

    let hash = hasher.digest128();
    let file_name = path.file_name().unwrap_or_default().to_str().unwrap();

    let node = DirNode {
        dtype: MerkleTreeNodeType::Dir,
        name: file_name.to_owned(),
        hash,
        num_bytes,
        last_commit_id: commit_id,
        last_modified_seconds: 0,
        last_modified_nanoseconds: 0,
        data_type_counts,
    };
    Ok(node)
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    use crate::command;
    use crate::core::v0_19_0::index::merkle_tree::CommitMerkleTree;
    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::repositories;
    use crate::test;
    use crate::util;

    fn write_first_commit_entries(
        repo: &LocalRepository,
        num_files: u64,
        num_dirs: u64,
    ) -> Result<(), OxenError> {
        /*
        README.md
        files.csv
        files/
          file1.txt
          file2.txt
          ..
          fileN.txt
        */

        let readme_file = repo.path.join("README.md");
        util::fs::write_to_path(&readme_file, format!("Repo with {} files", num_files))?;

        command::add(&repo, &readme_file)?;

        // Write files.csv
        let files_csv = repo.path.join("files.csv");
        let mut file = File::create(&files_csv)?;
        file.write_all(b"file,label\n")?;
        for i in 0..num_files {
            let label = if i % 2 == 0 { "cat" } else { "dog" };
            file.write_all(format!("file{}.txt,{}\n", i, label).as_bytes())?;
        }
        file.flush()?;

        // Write files
        let files_dir = repo.path.join("files");
        util::fs::create_dir_all(&files_dir)?;
        for i in 0..num_files {
            // Create num_dirs directories
            let dir_num = i % num_dirs;
            let dir_path = files_dir.join(format!("dir_{}", dir_num));
            util::fs::create_dir_all(&dir_path)?;

            let file_file = dir_path.join(format!("file{}.txt", i));
            util::fs::write_to_path(&file_file, format!("File {}", i))?;
        }

        command::add(&repo, &files_csv)?;
        command::add(&repo, &files_dir)?;

        Ok(())
    }

    #[test]
    fn test_first_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = command::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            write_first_commit_entries(&repo, 10, 2)?;
            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            /*
            [Commit] 861d5cd233eff0940060bd76ce24f10a
              [Dir] ""
                [VNode]
                  [File] README.md
                  [File] files.csv
                  [Dir] files
                    [VNode]
                      [Dir] dir_0
                        [VNode]
                          [File] file4.txt
                          [File] file0.txt
                          [File] file2.txt
                          [File] file6.txt
                          [File] file8.txt
                      [Dir] dir_1
                        [VNode]
                          [File] file7.txt
                          [File] file3.txt
                          [File] file5.txt
                          [File] file1.txt
                          [File] file9.txt
            */

            // Make sure we have 4 vnodes
            let vnodes = tree.total_vnodes();
            assert_eq!(vnodes, 4);

            // Make sure the root is a commit node
            let root = &tree.root;
            let commit = root.commit();
            assert!(commit.is_ok());

            // Make sure the root commit has 1 child, the root dir node
            let root_commit_children = &root.children;
            assert_eq!(root_commit_children.len(), 1);

            let dir_node_data = root_commit_children.iter().next().unwrap();
            let dir_node = dir_node_data.dir();
            assert!(dir_node.is_ok());
            assert_eq!(dir_node.unwrap().name, "");

            // Make sure dir node has one child, the VNode
            let vnode_data = dir_node_data.children.iter().next().unwrap();
            let vnode = vnode_data.vnode();
            assert!(vnode.is_ok());

            // Make sure the vnode has 3 children, the 2 files and the dir
            let vnode_children = &vnode_data.children;
            assert_eq!(vnode_children.len(), 3);

            // Check that files.csv is in the merkle tree
            let has_files_csv = tree.has_file(&Path::new("files.csv"))?;
            assert!(has_files_csv);

            // Check that README.md is in the merkle tree
            let has_readme = tree.has_file(&Path::new("README.md"))?;
            assert!(has_readme);

            // Check that files/dir_0/file0.txt is in the merkle tree
            let has_file0 = tree.has_file(&Path::new("files/dir_0/file0.txt"))?;
            assert!(has_file0);

            Ok(())
        })
    }

    #[test]
    fn test_commit_only_dirs_at_top_level() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = command::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Add a new file to files/dir_0/
            let new_file = repo.path.join("all_files/dir_0/new_file.txt");
            util::fs::create_dir_all(&new_file.parent().unwrap())?;
            util::fs::write_to_path(&new_file, "New file")?;
            command::add(&repo, &repo.path)?;

            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            let has_file0 = tree.has_file(&Path::new("all_files/dir_0/new_file.txt"))?;
            assert!(has_file0);

            Ok(())
        })
    }

    #[test]
    fn test_commit_single_file_deep_in_dir() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = command::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Add a new file to files/dir_0/
            let new_file = repo.path.join("files/dir_0/new_file.txt");
            util::fs::create_dir_all(&new_file.parent().unwrap())?;
            util::fs::write_to_path(&new_file, "New file")?;
            command::add(&repo, &new_file)?;

            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            let has_file0 = tree.has_file(&Path::new("files/dir_0/new_file.txt"))?;
            assert!(has_file0);

            Ok(())
        })
    }

    #[test]
    fn test_second_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = command::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            write_first_commit_entries(&repo, 10, 3)?;
            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            // Add a new file to files/dir_1/
            let new_file = repo.path.join("files/dir_1/new_file.txt");
            util::fs::write_to_path(&new_file, "New file")?;
            command::add(&repo, &new_file)?;

            // Commit the data
            let commit = super::commit(&repo, "Second commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            Ok(())
        })
    }
}
