use futures::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::Duration;

use crate::api::client::commits::ChunkParams;
use crate::constants::AVG_CHUNK_SIZE;
use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core::progress::push_progress::PushProgress;
use crate::core::v_latest::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Branch, Commit, CommitEntry, LocalRepository, MerkleHash, RemoteRepository};
use crate::util::{self, concurrency};
use crate::{api, repositories};
use derive_more::FromStr;

// Struct to track node parents for dir-level sync
#[derive(Eq, Hash, PartialEq)]
pub struct EntryWithParent {
    pub commit_entry: Entry,
    pub parent_id: MerkleHash,
}

pub async fn push(repo: &LocalRepository) -> Result<Branch, OxenError> {
    let Some(current_branch) = repositories::branches::current_branch(repo)? else {
        log::debug!("Push, no current branch found");
        return Err(OxenError::must_be_on_valid_branch());
    };
    push_remote_branch(repo, DEFAULT_REMOTE_NAME, current_branch.name).await
}

pub async fn push_remote_branch(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch_name: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    // start a timer
    let start = std::time::Instant::now();

    let remote = remote.as_ref();
    let branch_name = branch_name.as_ref();

    let Some(local_branch) = repositories::branches::get_by_name(repo, branch_name)? else {
        return Err(OxenError::local_branch_not_found(branch_name));
    };

    println!(
        "🐂 oxen push {} {} -> {}",
        remote, local_branch.name, local_branch.commit_id
    );

    let remote = repo
        .get_remote(remote)
        .ok_or(OxenError::remote_not_set(remote))?;

    let remote_repo = match api::client::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    push_local_branch_to_remote_repo(repo, &remote_repo, &local_branch).await?;
    let duration = std::time::Duration::from_millis(start.elapsed().as_millis() as u64);
    println!(
        "🐂 push complete 🎉 took {}",
        humantime::format_duration(duration)
    );
    Ok(local_branch)
}

async fn push_local_branch_to_remote_repo(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    local_branch: &Branch,
) -> Result<(), OxenError> {
    // Get the commit from the branch
    let Some(commit) = repositories::commits::get_by_id(repo, &local_branch.commit_id)? else {
        return Err(OxenError::revision_not_found(
            local_branch.commit_id.clone().into(),
        ));
    };

    // Notify the server that we are starting a push
    api::client::repositories::pre_push(remote_repo, local_branch, &commit.id).await?;

    // Check if the remote branch exists, and either push to it or create a new one
    match api::client::branches::get_by_name(remote_repo, &local_branch.name).await? {
        Some(remote_branch) => {
            push_to_existing_branch(repo, &commit, remote_repo, &remote_branch).await?
        }
        None => push_to_new_branch(repo, remote_repo, local_branch, &commit).await?,
    }

    // Notify the server that we are done pushing
    api::client::repositories::post_push(remote_repo, local_branch, &commit.id).await?;

    Ok(())
}

async fn push_to_new_branch(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    branch: &Branch,
    commit: &Commit,
) -> Result<(), OxenError> {
    // We need to find all the commits that need to be pushed
    let history = repositories::commits::list_from(repo, &commit.id)?;

    // Find the latest remote commit to use as a base for filtering out existing nodes
    let latest_remote_commit = find_latest_remote_commit(repo, remote_repo).await?;

    // Push the commits
    push_commits(repo, remote_repo, latest_remote_commit, &history).await?;

    // Create the remote branch from the commit
    api::client::branches::create_from_commit(remote_repo, &branch.name, commit).await?;

    Ok(())
}

fn collect_missing_files(
    node: &MerkleTreeNode,
    hashes: &HashSet<MerkleHash>,
    entries: &mut HashSet<EntryWithParent>,
    total_bytes: &mut u64,
    total_children: &mut usize,
) -> Result<(), OxenError> {
    log::debug!(
        "collect_missing_files node: {} children: {}",
        node,
        node.children.len()
    );
    for child in &node.children {
        if let EMerkleTreeNode::File(file_node) = &child.node {
            if !hashes.contains(&child.hash) {
                continue;
            }
            *total_bytes += file_node.num_bytes();
            // Empty entries aren't pushed to the remote, so don't count them in total children
            if file_node.num_bytes() > 0 {
                *total_children += 1;
            }
            entries.insert(EntryWithParent {
                commit_entry: Entry::CommitEntry(CommitEntry {
                    commit_id: file_node.last_commit_id().to_string(),
                    path: PathBuf::from(file_node.name()),
                    hash: child.hash.to_string(),
                    num_bytes: file_node.num_bytes(),
                    last_modified_seconds: file_node.last_modified_seconds(),
                    last_modified_nanoseconds: file_node.last_modified_nanoseconds(),
                }),
                parent_id: node.hash,
            });
        }
    }
    Ok(())
}

async fn push_to_existing_branch(
    repo: &LocalRepository,
    commit: &Commit,
    remote_repo: &RemoteRepository,
    remote_branch: &Branch,
) -> Result<(), OxenError> {
    // Check if the latest commit on the remote is the same as the local branch
    if remote_branch.commit_id == commit.id {
        println!("Everything is up to date");
        return Ok(());
    }

    match repositories::commits::list_from(repo, &commit.id) {
        Ok(commits) => {
            if commits.iter().any(|c| c.id == remote_branch.commit_id) {
                //we're ahead

                let latest_remote_commit =
                    repositories::commits::get_by_id(repo, &remote_branch.commit_id)?.ok_or_else(
                        || OxenError::revision_not_found(remote_branch.commit_id.clone().into()),
                    )?;

                let mut commits =
                    repositories::commits::list_between(repo, &latest_remote_commit, commit)?;
                commits.reverse();

                push_commits(repo, remote_repo, Some(latest_remote_commit), &commits).await?;
                api::client::branches::update(remote_repo, &remote_branch.name, commit).await?;
            } else {
                //we're behind
                let err_str = format!(
                    "Branch {} is behind {} must pull.\n\nRun `oxen pull` to update your local branch",
                    remote_branch.name, remote_branch.commit_id
                );
                return Err(OxenError::basic_str(err_str));
            }
        }
        Err(err) => {
            return Err(err);
        }
    };

    Ok(())
}

async fn push_commits(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    latest_remote_commit: Option<Commit>,
    history: &[Commit],
) -> Result<(), OxenError> {
    // We need to find all the commits that need to be pushed
    let node_hashes = history
        .iter()
        .map(|c| c.hash().unwrap())
        .collect::<HashSet<MerkleHash>>();

    // Given the missing commits on the server, filter the history
    let missing_commit_hashes =
        api::client::commits::list_missing_hashes(remote_repo, node_hashes).await?;
    log::debug!(
        "push_commits missing_commit_hashes count: {}",
        missing_commit_hashes.len()
    );

    let missing_commits: Vec<Commit> = history
        .iter()
        .filter(|c| missing_commit_hashes.contains(&c.hash().unwrap()))
        .map(|c| c.to_owned())
        .collect();

    // Collect all the nodes that could be missing from the server
    let progress = Arc::new(PushProgress::new());
    progress.set_message("Collecting candidate nodes...");

    // Get the node hashes for the starting commit (if we have one)
    let mut starting_node_hashes = HashSet::new();
    if let Some(ref commit) = latest_remote_commit {
        repositories::tree::populate_starting_hashes(
            repo,
            commit,
            &None,
            &None,
            &mut starting_node_hashes,
        )?;
    }

    log::debug!("starting hashes: {:?}", starting_node_hashes.len());

    let mut shared_hashes = starting_node_hashes.clone();
    let mut unique_hashes = HashSet::new();

    let mut candidate_nodes: HashSet<MerkleTreeNode> = HashSet::new();
    for commit in &missing_commits {
        log::debug!("push_commits adding candidate nodes for commit: {}", commit);
        let Some(commit_node) = CommitMerkleTree::get_unique_children_for_commit(
            repo,
            commit,
            &mut shared_hashes,
            &mut unique_hashes,
        )?
        else {
            log::error!("push_commits commit node not found for commit: {}", commit);
            continue;
        };

        shared_hashes.extend(&unique_hashes);
        unique_hashes.clear();
        candidate_nodes.insert(commit_node.clone());

        commit_node.walk_tree_without_leaves(|node| {
            if !starting_node_hashes.contains(&node.hash) {
                candidate_nodes.insert(node.clone());
                progress.set_message(format!(
                    "Collecting candidate nodes... {}",
                    candidate_nodes.len()
                ));
            }
        });
    }
    log::debug!(
        "push_commits candidate_nodes count: {}",
        candidate_nodes.len()
    );

    // Check which of the candidate nodes are missing from the server (just use the hashes)
    let candidate_node_hashes = candidate_nodes
        .iter()
        .map(|n| n.hash)
        .collect::<HashSet<MerkleHash>>();
    progress.set_message(format!(
        "Considering {} nodes...",
        candidate_node_hashes.len()
    ));

    // log::debug!("Candidate Hashes: {candidate_node_hashes:?}");
    let missing_node_hashes =
        api::client::tree::list_missing_node_hashes(remote_repo, candidate_node_hashes).await?;
    log::debug!(
        "push_commits missing_node_hashes count: {:?}",
        missing_node_hashes.len()
    );

    // Separate the candidate nodes into present and missing nodes
    let mut missing_nodes: HashSet<MerkleTreeNode> = HashSet::new();
    let mut present_node_hashes: HashSet<MerkleHash> = HashSet::new();

    for node in candidate_nodes.into_iter() {
        if missing_node_hashes.contains(&node.hash) {
            missing_nodes.insert(node);
        } else {
            present_node_hashes.insert(node.hash);
        }
    }

    progress.set_message(format!("Pushing {} nodes...", missing_nodes.len()));
    api::client::tree::create_nodes(repo, remote_repo, missing_nodes.clone(), &progress).await?;

    // Create the dir hashes for the missing commits
    api::client::commits::post_commits_dir_hashes_to_server(repo, remote_repo, &missing_commits)
        .await?;

    progress.set_message("Checking for missing files...".to_string());

    starting_node_hashes.extend(present_node_hashes);
    let missing_file_hashes = api::client::tree::list_missing_file_hashes_from_nodes(
        repo,
        remote_repo,
        missing_commit_hashes.clone(),
        starting_node_hashes,
    )
    .await?;
    progress.set_message(format!("Pushing {} files...", missing_file_hashes.len()));
    let mut missing_files: HashSet<EntryWithParent> = HashSet::new();
    let mut total_bytes = 0;

    // Tracking variables for dir-level sync
    let mut node_parents: HashMap<MerkleHash, MerkleHash> = HashMap::new();
    let mut node_child_count: HashMap<MerkleHash, AtomicUsize> = HashMap::new();
    let mut total_files: usize = 0;

    for node in missing_nodes {
        collect_missing_files(
            &node,
            &missing_file_hashes,
            &mut missing_files,
            &mut total_bytes,
            &mut total_files,
        )?;

        log::debug!(
            "children for node {:?}: {:?}",
            node.hash,
            node.children.len()
        );

        // Track each dir/vnode's parents and children to determine when to mark as synced
        node_child_count.insert(node.hash, AtomicUsize::new(total_files));
        if let Some(parent_hash) = node.parent_id {
            node_parents.insert(node.hash, parent_hash);
        }

        total_files = 0;
    }

    // Add nodes with files to push to their parents' child count
    // Mark nodes without files to push as synced immediately

    // Note: This logic relies on all children appearing before their parents in missing_nodes
    // This should always happen with the current walk_tree_without_leaves implementation

    let mut empty_nodes: HashSet<MerkleHash> = HashSet::new();
    for (node_hash, parent_hash) in &node_parents {
        if let Some(child_count) = node_child_count.get(node_hash) {
            if child_count.load(Ordering::SeqCst) > 0 {
                if let Some(parent_count) = node_child_count.get(parent_hash) {
                    parent_count.fetch_add(1, Ordering::SeqCst);
                }
            } else {
                empty_nodes.insert(*node_hash);
            }
        }
    }

    api::client::tree::mark_nodes_as_synced(remote_repo, empty_nodes).await?;

    let missing_files: Vec<EntryWithParent> = missing_files.into_iter().collect();
    progress.finish();
    let progress = Arc::new(PushProgress::new_with_totals(
        missing_files.len() as u64,
        total_bytes,
    ));
    log::debug!("pushing {} entries", missing_files.len());

    let commit = &history.last().unwrap();
    let node_child_count = Arc::new(node_child_count);

    push_entries(
        repo,
        remote_repo,
        &node_child_count,
        &node_parents,
        &missing_files,
        commit,
        &progress,
    )
    .await?;

    // Mark commits as synced on the server
    api::client::commits::mark_commits_as_synced(remote_repo, missing_commit_hashes).await?;

    progress.finish();

    Ok(())
}

pub async fn push_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node_child_count: &Arc<HashMap<MerkleHash, AtomicUsize>>,
    node_parents: &HashMap<MerkleHash, MerkleHash>,
    entries: &[EntryWithParent],
    commit: &Commit,
    progress: &Arc<PushProgress>,
) -> Result<(), OxenError> {
    log::debug!(
        "PUSH ENTRIES {} -> {} -> '{}'",
        entries.len(),
        commit.id,
        commit.message
    );

    use tokio::time::sleep;
    type PieceOfWork = (
        Entry,
        MerkleHash,
        LocalRepository,
        Commit,
        RemoteRepository,
        Arc<reqwest::Client>,
    );

    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
    type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

    // Create a client for uploading chunks
    let client = Arc::new(api::client::builder_for_remote_repo(remote_repo)?.build()?);

    log::debug!(
        "Splitting {} entries into pieces of work for upload",
        entries.len()
    );
    let entries: Vec<PieceOfWork> = entries
        .iter()
        .map(|e| {
            (
                e.commit_entry.to_owned(),
                e.parent_id.to_owned(),
                local_repo.to_owned(),
                commit.to_owned(),
                remote_repo.to_owned(),
                client.clone(),
            )
        })
        .collect();

    if entries.is_empty() {
        log::debug!("No entries to push. Exiting immediately");
        return Ok(());
    }

    let queue = Arc::new(TaskQueue::new(entries.len()));
    let finished_queue = Arc::new(FinishedTaskQueue::new(entries.len()));
    for entry in entries.iter() {
        queue.try_push(entry.to_owned()).unwrap();
        finished_queue.try_push(false).unwrap();
    }

    let worker_count = concurrency::num_threads_for_items(entries.len());
    log::debug!(
        "worker_count {} entries len {}",
        worker_count,
        entries.len()
    );

    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let bar = Arc::clone(progress);
        let node_parents = node_parents.clone();

        let node_child_count_copy = Arc::clone(node_child_count);
        let mut small_entries_chunk: Vec<Entry> = vec![];
        let mut files_to_push: Vec<(MerkleHash, MerkleHash)> = vec![];
        let mut current_size: u64 = 0;
        let mut synced_nodes: HashSet<MerkleHash> = HashSet::new();

        let local_repo_copy = local_repo.clone();
        let remote_repo_copy = remote_repo.clone();
        let client_copy = client.clone();

        tokio::spawn(async move {
            loop {
                let Some((entry, parent_id, repo, commit, remote_repo, client)) = queue.try_pop()
                else {
                    break;
                };

                log::debug!("worker[{}] processing task...", worker);

                if entry.num_bytes() >= AVG_CHUNK_SIZE {
                    upload_large_file_chunks(
                        entry.clone(),
                        repo,
                        commit,
                        remote_repo,
                        AVG_CHUNK_SIZE,
                        &bar,
                    )
                    .await;

                    // Decrement child count for parent hash

                    let entry_hash = match MerkleHash::from_str(&entry.hash()) {
                        Ok(hash) => hash,
                        Err(_) => {
                            log::error!("{}", format_args!("Error: cannot get hash from entry {entry:?}. Skipping decrement"));
                            continue;
                        }
                    };

                    let to_decrement = vec![(entry_hash, parent_id)];
                    match decrement_child_count(
                        &node_child_count_copy,
                        &node_parents,
                        &to_decrement,
                        &mut synced_nodes,
                    ) {
                        Ok(_) => {}
                        // TODO: How to handle errors with this?
                        Err(e) => log::debug!("Error updating count: {}", e),
                    }

                    finished_queue.pop().await;
                    // synced_nodes.clear();
                } else {
                    // If the next entry would breach the average chunk size, push the current chunk
                    if current_size + entry.num_bytes() > AVG_CHUNK_SIZE {
                        match api::client::versions::multipart_batch_upload_with_retry(
                            &repo,
                            &remote_repo,
                            &small_entries_chunk,
                            &client,
                            &synced_nodes,
                        )
                        .await
                        {
                            Ok(_err_files) => {
                                // TODO: return err files info to the user
                                log::debug!("Successfully uploaded data!")
                            }
                            Err(e) => {
                                // TODO: Surface the error to the user
                                log::error!("Error uploading chunk: {:?}", e)
                            }
                        }

                        // Decrement child count for each parent hash
                        // TODO: Handle err_files differently; probably shouldn't decrement their parents' count until they're actually uploaded
                        synced_nodes.clear();
                        match decrement_child_count(
                            &node_child_count_copy,
                            &node_parents,
                            &files_to_push,
                            &mut synced_nodes,
                        ) {
                            Ok(_) => {}
                            // TODO: How to handle errors with this?
                            Err(e) => log::debug!("Error updating count: {}", e),
                        }

                        // Update progress bar
                        bar.add_bytes(current_size);
                        bar.add_files(small_entries_chunk.len() as u64);

                        // Update finished queue
                        for _ in 0..small_entries_chunk.len() {
                            finished_queue.pop().await;
                        }

                        // Reset tracking variables
                        small_entries_chunk.clear();
                        files_to_push.clear();
                        current_size = 0;
                    }

                    // Add the current entry
                    small_entries_chunk.push(entry.clone());
                    let entry_hash = match MerkleHash::from_str(&entry.hash()) {
                        Ok(hash) => hash,
                        Err(_) => {
                            log::error!("{}", format_args!("Error: cannot get hash from entry {entry:?}. Skipping decrement"));
                            continue;
                        }
                    };

                    files_to_push.push((entry_hash, parent_id));
                    current_size += entry.num_bytes();
                }
            }

            // Upload the remaining small entries
            if !small_entries_chunk.is_empty() || !synced_nodes.is_empty() {
                log::debug!(
                    "Worker[{}] uploading remaining small entries chunk...",
                    worker
                );

                // Decrement child_count before pushing to ensure all nodes get marked as synced
                match decrement_child_count(
                    &node_child_count_copy,
                    &node_parents,
                    &files_to_push,
                    &mut synced_nodes,
                ) {
                    Ok(_) => {}
                    Err(e) => log::debug!("Error updating count: {}", e),
                }
                log::debug!("synced_nodes for final upload: {synced_nodes:?}");
                match api::client::versions::multipart_batch_upload_with_retry(
                    &local_repo_copy,
                    &remote_repo_copy,
                    &small_entries_chunk,
                    &client_copy,
                    &synced_nodes.clone(),
                )
                .await
                {
                    Ok(_err_files) => {
                        log::debug!("Successfully uploaded remaining data!");
                    }
                    Err(e) => {
                        log::error!("Error uploading remaining chunk: {:?}", e);
                    }
                }

                // Update progress bar
                bar.add_bytes(current_size);
                bar.add_files(small_entries_chunk.len() as u64);

                // Update finished queue
                for _ in 0..small_entries_chunk.len() {
                    finished_queue.pop().await;
                }
            }
        });
    }

    while !finished_queue.is_empty() {
        // log::debug!("Before waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All file tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

/*
async fn chunk_and_send_large_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    commit: &Commit,
    chunk_size: u64,
    progress: &Arc<PushProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }

    use tokio::time::sleep;
    type PieceOfWork = (Entry, LocalRepository, Commit, RemoteRepository);
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
    type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

    log::debug!("Chunking and sending {} larger files", entries.len());
    let entries: Vec<PieceOfWork> = entries
        .iter()
        .map(|e| {
            (
                e.to_owned(),
                local_repo.to_owned(),
                commit.to_owned(),
                remote_repo.to_owned(),
            )
        })
        .collect();

    let queue = Arc::new(TaskQueue::new(entries.len()));
    let finished_queue = Arc::new(FinishedTaskQueue::new(entries.len()));
    for entry in entries.iter() {
        queue.try_push(entry.to_owned()).unwrap();
        finished_queue.try_push(false).unwrap();
    }

    let worker_count = concurrency::num_threads_for_items(entries.len());
    log::debug!(
        "worker_count {} entries len {}",
        worker_count,
        entries.len()
    );
    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let bar = Arc::clone(progress);
        tokio::spawn(async move {
            loop {
                let (entry, repo, commit, remote_repo) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);

                upload_large_file_chunks(entry, repo, commit, remote_repo, chunk_size, &bar).await;

                finished_queue.pop().await;
            }
        });
    }

    while !finished_queue.is_empty() {
        // log::debug!("Before waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All large file tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_millis(100)).await;

    Ok(())
}
    */

/// Chunk and send large file in parallel
async fn upload_large_file_chunks(
    entry: Entry,
    repo: LocalRepository,
    commit: Commit,
    remote_repo: RemoteRepository,
    chunk_size: u64,
    progress: &Arc<PushProgress>,
) {
    // Open versioned file
    let version_store = repo.version_store().unwrap();
    let file = version_store.open_version(&entry.hash()).unwrap();
    let mut reader = BufReader::new(file);
    // The version path is just being used for compatibility with the server endpoint,
    // we aren't using it to read the file.
    // TODO: This should be migrated to use the new versions API
    let version_path = util::fs::version_path_for_entry(&repo, &entry);

    // These variables are the same for every chunk
    // let is_compressed = false;
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let path = util::fs::path_relative_to_dir(&version_path, &hidden_dir).unwrap();
    let file_name = Some(String::from(path.to_str().unwrap()));

    // Calculate chunk sizes
    let total_bytes = entry.num_bytes();
    let total_chunks = ((total_bytes / chunk_size) + 1) as usize;
    let mut total_bytes_read = 0;
    let mut chunk_size = chunk_size;

    // Create a client for uploading chunks
    let client = Arc::new(
        api::client::builder_for_remote_repo(&remote_repo)
            .unwrap()
            .build()
            .unwrap(),
    );

    // Create queues for sending data to workers
    type PieceOfWork = (
        Vec<u8>,
        u64,   // chunk size
        usize, // chunk num
        usize, // total chunks
        u64,   // total size
        Arc<reqwest::Client>,
        RemoteRepository,
        String, // entry hash
        Commit,
        Option<String>, // filename
    );

    // In order to upload chunks in parallel
    // We should only read N chunks at a time so that
    // the whole file does not get read into memory
    let sub_chunk_size = concurrency::num_threads_for_items(total_chunks);

    let mut total_chunk_idx = 0;
    let mut processed_chunk_idx = 0;
    let num_sub_chunks = (total_chunks / sub_chunk_size) + 1;
    log::debug!(
        "upload_large_file_chunks {:?} processing file in {} subchunks of size {} from total {} chunk size {} file size {}",
        entry.path(),
        num_sub_chunks,
        sub_chunk_size,
        total_chunks,
        chunk_size,
        total_bytes
    );
    for i in 0..num_sub_chunks {
        log::debug!(
            "upload_large_file_chunks Start reading subchunk {i}/{num_sub_chunks} of size {sub_chunk_size} from total {total_chunks} chunk size {chunk_size} file size {total_bytes_read}/{total_bytes}"
        );
        // Read and send the subset of buffers sequentially
        let mut sub_buffers: Vec<Vec<u8>> = Vec::new();
        for _ in 0..sub_chunk_size {
            // If we have read all the bytes, break
            if total_bytes_read >= total_bytes {
                break;
            }

            // Make sure we read the last size correctly
            if (total_bytes_read + chunk_size) > total_bytes {
                chunk_size = total_bytes % chunk_size;
            }

            let percent_read = (total_bytes_read as f64 / total_bytes as f64) * 100.0;
            log::debug!("upload_large_file_chunks has read {total_bytes_read}/{total_bytes} = {percent_read}% about to read {chunk_size}");

            // Only read as much as you need to send so we don't blow up memory on large files
            let mut buffer = vec![0u8; chunk_size as usize];
            match reader.read_exact(&mut buffer) {
                Ok(_) => {}
                Err(err) => {
                    log::error!("upload_large_file_chunks Error reading file {:?} chunk {total_chunk_idx}/{total_chunks} chunk size {chunk_size} total_bytes_read: {total_bytes_read} total_bytes: {total_bytes} {:?}", entry.path(), err);
                    return;
                }
            }
            total_bytes_read += chunk_size;
            total_chunk_idx += 1;

            sub_buffers.push(buffer);
        }
        log::debug!(
            "upload_large_file_chunks Done, have read subchunk {}/{} subchunk {}/{} of size {}",
            processed_chunk_idx,
            total_chunks,
            i,
            num_sub_chunks,
            sub_chunk_size
        );

        // Then send sub_buffers over network in parallel
        // let queue = Arc::new(TaskQueue::new(sub_buffers.len()));
        // let finished_queue = Arc::new(FinishedTaskQueue::new(sub_buffers.len()));
        let mut tasks: Vec<PieceOfWork> = Vec::new();
        for buffer in sub_buffers.iter() {
            tasks.push((
                buffer.to_owned(),
                chunk_size,
                processed_chunk_idx, // Needs to be the overall chunk num
                total_chunks,
                total_bytes,
                client.clone(),
                remote_repo.to_owned(),
                entry.hash().to_owned(),
                commit.to_owned(),
                file_name.to_owned(),
            ));
            // finished_queue.try_push(false).unwrap();
            processed_chunk_idx += 1;
        }

        // Setup the stream chunks in parallel
        let bodies = stream::iter(tasks)
            .map(|item| async move {
                let (
                    buffer,
                    chunk_size,
                    chunk_num,
                    total_chunks,
                    total_size,
                    client,
                    remote_repo,
                    entry_hash,
                    _commit,
                    file_name,
                ) = item;
                let size = buffer.len() as u64;
                log::debug!(
                    "upload_large_file_chunks Streaming entry buffer {}/{} of size {}",
                    chunk_num,
                    total_chunks,
                    size
                );

                let params = ChunkParams {
                    chunk_num,
                    total_chunks,
                    total_size: total_size as usize,
                };

                let is_compressed = false;

                match api::client::commits::upload_data_chunk_to_server_with_retry(
                    &client,
                    &remote_repo,
                    &buffer,
                    &entry_hash,
                    &params,
                    is_compressed,
                    &file_name,
                )
                .await
                {
                    Ok(_) => {
                        log::debug!(
                            "upload_large_file_chunks Successfully uploaded subchunk overall chunk {}/{}",
                            chunk_num,
                            total_chunks
                        );
                        Ok(chunk_size)
                    }
                    Err(err) => {
                        log::error!("Error uploading chunk: {err}");
                        Err(err)
                    }
                }
            })
            .buffer_unordered(sub_chunk_size);

        // Wait for all requests to finish
        bodies
            .for_each(|b| async {
                match b {
                    Ok(_) => {
                        progress.add_bytes(chunk_size);
                    }
                    Err(err) => {
                        log::error!("Error uploading chunk: {err}")
                    }
                }
            })
            .await;

        log::debug!("upload_large_file_chunks Subchunk {i}/{num_sub_chunks} tasks done. :-)");
    }
    progress.add_files(1);
}

/*
/// Sends entries in tarballs of size ~chunk size
async fn bundle_and_send_small_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    commit: &Commit,
    avg_chunk_size: u64,
    progress: &Arc<PushProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }


    let total_size = repositories::entries::compute_generic_entries_size(&entries)?;
    let num_chunks = ((total_size / avg_chunk_size) + 1) as usize;

    let mut chunk_size = entries.len() / num_chunks;
    if num_chunks > entries.len() {
        chunk_size = entries.len();
    }

    // Create a client for uploading chunks
    let client = Arc::new(api::client::builder_for_remote_repo(remote_repo)?.build()?);

    // Split into chunks, zip up, and post to server
    use tokio::time::sleep;
    type PieceOfWork = (
        Vec<Entry>,
        LocalRepository,
        Commit,
        RemoteRepository,
        Arc<reqwest::Client>,
    );
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
    type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

    log::debug!("Creating {num_chunks} chunks from {total_size} bytes with size {chunk_size}");
    let chunks: Vec<PieceOfWork> = entries
        .chunks(chunk_size)
        .map(|c| {
            (
                c.to_owned(),
                local_repo.to_owned(),
                commit.to_owned(),
                remote_repo.to_owned(),
                client.clone(),
            )
        })
        .collect();

    let worker_count = concurrency::num_threads_for_items(chunks.len());
    let queue = Arc::new(TaskQueue::new(chunks.len()));
    let finished_queue = Arc::new(FinishedTaskQueue::new(chunks.len()));
    for chunk in chunks {
        queue.try_push(chunk).unwrap();
        finished_queue.try_push(false).unwrap();
    }

    // TODO: this needs some more robust error handling. What should we do if a single item fails?
    // Currently no way to bubble up that error.
    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let bar = Arc::clone(progress);
        tokio::spawn(async move {
            loop {
                let (chunk, repo, _commit, remote_repo, client) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);
                log::debug!("Chunk size {}", chunk.len());
                let chunk_size = match repositories::entries::compute_generic_entries_size(&chunk) {
                    Ok(size) => size,
                    Err(e) => {
                        log::error!("Failed to compute entries size: {}", e);
                        continue; // or break or decide on another error-handling strategy
                    }
                };

                let _synced_nodes = HashSet::new();
                match api::client::versions::multipart_batch_upload_with_retry(
                    &repo,
                    &remote_repo,
                    &chunk,
                    &client,
                    &_synced_nodes,
                )
                .await
                {
                    Ok(_err_files) => {
                        // TODO: return err files info to the user
                        log::debug!("Successfully uploaded data!")
                    }
                    Err(e) => {
                        // TODO: Surface the error to the user
                        log::error!("Error uploading chunk: {:?}", e)
                    }
                }

                bar.add_bytes(chunk_size);
                bar.add_files(chunk.len() as u64);
                finished_queue.pop().await;
            }
        });
    }
    while !finished_queue.is_empty() {
        // log::debug!("Waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_millis(100)).await;

    Ok(())
}
*/

async fn find_latest_remote_commit(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
) -> Result<Option<Commit>, OxenError> {
    // TODO: Revisit this and compute the latest commit from the LCA of the local and remote branches
    // Try to get remote branches
    let remote_branches = api::client::branches::list(remote_repo).await?;

    if remote_branches.is_empty() {
        // No remote branches exist - this is a new repo
        return Ok(None);
    }

    // First, try to find the default branch (main)
    let default_branch = remote_branches
        .iter()
        .find(|b| b.name == crate::constants::DEFAULT_BRANCH_NAME)
        .or_else(|| remote_branches.first());

    if let Some(remote_branch) = default_branch {
        // Get the commit from the remote branch
        if let Some(remote_commit) =
            repositories::commits::get_by_id(repo, &remote_branch.commit_id)?
        {
            // We have the remote commit locally, so use it
            Ok(Some(remote_commit))
        } else {
            // We don't have the remote commit locally - this shouldn't happen in normal flow
            // but can happen if we haven't fetched the remote branch
            Ok(None)
        }
    } else {
        // No branches found
        Ok(None)
    }
}

fn decrement_child_count(
    node_child_count: &Arc<HashMap<MerkleHash, AtomicUsize>>,
    parent_map: &HashMap<MerkleHash, MerkleHash>,
    pushed_files: &Vec<(MerkleHash, MerkleHash)>,
    synced_nodes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    for (_, dir_hash) in pushed_files {
        if let Some(count) = node_child_count.get(dir_hash) {
            // Atomically fetch and subtract from count
            let prev_count = count.fetch_sub(1, Ordering::SeqCst);
            // If prev_count is one, all the node's children have been pushed
            if prev_count == 1 {
                log::debug!("Dir node {:?} fully synced", dir_hash);
                synced_nodes.insert(*dir_hash);

                if let Some(dir_parent) = parent_map.get(dir_hash) {
                    let synced_dir: Vec<(MerkleHash, MerkleHash)> =
                        vec![(dir_hash.to_owned(), dir_parent.to_owned())];
                    return decrement_child_count(
                        node_child_count,
                        parent_map,
                        &synced_dir,
                        synced_nodes,
                    );
                }
            }
        } else {
            return Err(OxenError::basic_str(format!(
                "Parent hash {:?} not found in node_child_count",
                dir_hash
            )));
        }
    }

    Ok(())
}
