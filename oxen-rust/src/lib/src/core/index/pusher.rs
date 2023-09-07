//! Pushes commits and entries to the remote repository
//!

use crate::api::local::entries::compute_entries_size;
use crate::api::remote::commits::ChunkParams;
use crate::util::progress_bar::{oxen_progress_bar_with_msg, spinner_with_msg, ProgressBarType};

use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::io::{BufReader, Read};

use std::sync::Arc;

use tokio::time::Duration;

use crate::constants::{AVG_CHUNK_SIZE, NUM_HTTP_RETRIES};

use crate::core::index::{
    self, CommitDirEntryReader, CommitEntryReader, CommitReader, Merger, RefReader,
};
use crate::error::OxenError;
use crate::model::{Branch, Commit, CommitEntry, LocalRepository, RemoteBranch, RemoteRepository};

use crate::util::progress_bar::oxen_progress_bar;
use crate::{api, util};

pub struct UnsyncedCommitEntries {
    pub commit: Commit,
    pub entries: Vec<CommitEntry>,
}
pub async fn push(
    repo: &LocalRepository,
    rb: &RemoteBranch,
) -> Result<RemoteRepository, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branch = ref_reader.get_branch_by_name(&rb.branch)?;
    if branch.is_none() {
        return Err(OxenError::local_branch_not_found(&rb.branch));
    }

    let branch = branch.unwrap();

    println!(
        "🐂 Oxen push {} {} -> {}",
        rb.remote, branch.name, branch.commit_id
    );
    let remote = repo
        .get_remote(&rb.remote)
        .ok_or(OxenError::remote_not_set(&rb.remote))?;

    log::debug!("Pushing to remote {:?}", remote);
    // Repo should be created before this step
    let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    push_remote_repo(repo, remote_repo, branch).await
}

async fn validate_repo_is_pushable(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    branch: &Branch,
    commit_reader: &CommitReader,
    head_commit: &Commit,
) -> Result<(), OxenError> {
    // Make sure the remote branch is not ahead of the local branch
    if remote_is_ahead_of_local(remote_repo, commit_reader, branch).await? {
        return Err(OxenError::remote_ahead_of_local());
    }

    if cannot_push_incomplete_history(local_repo, remote_repo, head_commit, branch).await? {
        return Err(OxenError::incomplete_local_history());
    }

    Ok(())
}

pub async fn push_remote_repo(
    local_repo: &LocalRepository,
    remote_repo: RemoteRepository,
    branch: Branch,
) -> Result<RemoteRepository, OxenError> {
    // Lock the branch at the top, to avoid collisions from true simultaneous push

    // Returns a `remote_branch_locked` error if lock is already held
    api::remote::branches::lock(&remote_repo, &branch.name).await?;

    let commit_reader = CommitReader::new(local_repo)?;
    let head_commit = commit_reader
        .get_commit_by_id(&branch.commit_id)?
        .ok_or(OxenError::must_be_on_valid_branch())?;

    match validate_repo_is_pushable(
        local_repo,
        &remote_repo,
        &branch,
        &commit_reader,
        &head_commit,
    )
    .await
    {
        Ok(_) => {}
        Err(err) => {
            api::remote::branches::unlock(&remote_repo, &branch.name).await?;
            return Err(err);
        }
    }

    let branch_name = branch.name.clone();

    // Push the commits. If at any point during this process, we have errors or the user ctrl+c's, we release the branch lock.
    // TODO: Maybe we should only release the lock if we haven't yet started adding commits to the queue.
    // IF we've added commits to the queue, should we cede control of lock removal to when the queue is finished processing?
    tokio::select! {
        result = try_push_remote_repo(local_repo, &remote_repo, branch, &head_commit) => {
            match result {
                Ok(_) => {
                    // Unlock the branch
                    api::remote::branches::unlock(&remote_repo, &branch_name).await?;
                }
                Err(_err) => {
                    // Unlock the branch and handle error
                    api::remote::branches::unlock(&remote_repo, &branch_name).await?;
                    // handle the error
                }
            }
        },
        _ = tokio::signal::ctrl_c() => {
            // Ctrl+C was pressed
            println!("🐂 Received interrupt signal. Gracefully shutting down...");
            // Unlock the branch
            api::remote::branches::unlock(&remote_repo, &branch_name).await?;
            println!("🐂 Shutdown successful.");
            // Exit the process
            std::process::exit(0);
        }
    }

    Ok(remote_repo)
}

pub async fn try_push_remote_repo(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    branch: Branch,
    head_commit: &Commit,
) -> Result<(), OxenError> {
    let commits_to_sync =
        get_commit_objects_to_sync(local_repo, remote_repo, head_commit, &branch).await?;

    log::debug!(
        "push_remote_repo commit order after get_commit_objects_to_sync {:?}",
        commits_to_sync
    );

    let (unsynced_entries, total_size) =
        push_missing_commit_objects(local_repo, remote_repo, &commits_to_sync, &branch).await?;

    log::debug!("🐂 Identifying unsynced commits dbs...");
    let unsynced_db_commits =
        api::remote::commits::get_commits_with_unsynced_dbs(remote_repo, &branch).await?;

    push_missing_commit_dbs(local_repo, remote_repo, unsynced_db_commits).await?;

    // update the branch after everything else is synced
    log::debug!(
        "Updating remote branch {:?} to commit {:?}",
        &branch.name,
        &head_commit
    );
    log::debug!("🐂 Identifying commits with unsynced entries...");

    // Raise an OxenError for testing purposes

    // Get commits with unsynced entries
    let unsynced_entries_commits =
        api::remote::commits::get_commits_with_unsynced_entries(remote_repo, &branch).await?;

    log::debug!(
        "commits with unsynced entries before entries fn {:?}",
        unsynced_entries_commits
    );

    push_missing_commit_entries(
        local_repo,
        remote_repo,
        &branch,
        &unsynced_entries_commits,
        unsynced_entries,
        total_size,
    )
    .await?;

    api::remote::branches::update(remote_repo, &branch.name, head_commit).await?;

    // Remotely validate commit
    // This is an async process on the server so good to stall the user here so they don't push again
    // If they did push again before this is finished they would get a still syncing error
    let bar = oxen_progress_bar_with_msg(
        unsynced_entries_commits.len() as u64,
        "Remote validating commits",
    );
    poll_until_synced(remote_repo, head_commit, &bar).await?;
    bar.finish_and_clear();

    log::debug!("Just finished push.");

    Ok(())
}

async fn get_commit_objects_to_sync(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    local_commit: &Commit,
    branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    let remote_branch = api::remote::branches::get_by_name(remote_repo, &branch.name).await?;

    let mut commits_to_sync: Vec<Commit>;
    // TODO: If remote branch does not yet, recreates all commits regardless of shared history.
    // Not a huge deal performance-wise right now, but could be for very commit-heavy repos
    if let Some(remote_branch) = remote_branch {
        log::debug!(
            "get_commit_objects_to_sync found remote branch {:?}, calculating missing commits between local and remote heads", remote_branch
        );
        let remote_commit = api::remote::commits::get_by_id(remote_repo, &remote_branch.commit_id)
            .await?
            .unwrap();
        let commit_reader = CommitReader::new(local_repo)?;
        let merger = Merger::new(local_repo)?;
        commits_to_sync =
            merger.list_commits_between_commits(&commit_reader, &remote_commit, local_commit)?;

        let remote_history = api::remote::commits::list_commit_history(remote_repo, &branch.name)
            .await
            .unwrap_or_else(|_| vec![]);
        log::debug!(
            "get_commit_objects_to_sync calculated {} commits",
            commits_to_sync.len()
        );

        // Filter out any commits_to_sync that are in the remote_history
        commits_to_sync.retain(|commit| {
            !remote_history
                .iter()
                .any(|remote_commit| remote_commit.id == commit.id)
        });
    } else {
        // Branch does not exist on remote yet - get all commits?
        log::debug!("get_commit_objects_to_sync remote branch does not exist, getting all commits from local head");
        commits_to_sync = api::local::commits::list_from(local_repo, &local_commit.id)?;
    }

    // Order from BASE to HEAD
    commits_to_sync.reverse();

    Ok(commits_to_sync)
}

fn get_unsynced_entries_for_commit(
    local_repo: &LocalRepository,
    commit: &Commit,
    commit_reader: &CommitReader,
) -> Result<(Vec<UnsyncedCommitEntries>, u64), OxenError> {
    let mut unsynced_commits: Vec<UnsyncedCommitEntries> = Vec::new();
    let mut total_size: u64 = 0;

    if commit.parent_ids.is_empty() {
        unsynced_commits.push(UnsyncedCommitEntries {
            commit: commit.to_owned(),
            entries: vec![],
        });
    }
    for parent_id in commit.parent_ids.iter() {
        let local_parent = commit_reader
            .get_commit_by_id(parent_id)?
            .ok_or_else(|| OxenError::local_parent_link_broken(&commit.id))?;
        let entries = read_unsynced_entries(local_repo, &local_parent, commit)?;

        // Get size of these entries
        let entries_size = api::local::entries::compute_entries_size(&entries)?;
        total_size += entries_size;

        unsynced_commits.push(UnsyncedCommitEntries {
            commit: commit.to_owned(),
            entries,
        })
    }

    Ok((unsynced_commits, total_size))
}

async fn push_missing_commit_objects(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commits: &Vec<Commit>,
    branch: &Branch,
) -> Result<(Vec<UnsyncedCommitEntries>, u64), OxenError> {
    let mut unsynced_commits: Vec<UnsyncedCommitEntries> = Vec::new();

    let spinner = spinner_with_msg(format!(
        "🐂 Finding unsynced data from {} commits",
        commits.len()
    ));
    let commit_reader = CommitReader::new(local_repo)?;
    let mut total_size: u64 = 0;

    for commit in commits {
        let (commit_unsynced_commits, commit_size) =
            get_unsynced_entries_for_commit(local_repo, commit, &commit_reader)?;
        total_size += commit_size;
        unsynced_commits.extend(commit_unsynced_commits);
    }
    spinner.finish_and_clear();

    // Spin during async bulk create
    let spinner = spinner_with_msg(format!("🐂 Syncing {} commits", unsynced_commits.len()));

    api::remote::commits::post_commits_to_server(
        local_repo,
        remote_repo,
        &unsynced_commits,
        branch.name.clone(),
    )
    .await?;

    spinner.finish_and_clear();
    Ok((unsynced_commits, total_size))
}

async fn remote_is_ahead_of_local(
    remote_repo: &RemoteRepository,
    reader: &CommitReader,
    branch: &Branch,
) -> Result<bool, OxenError> {
    // Make sure that the branch has not progressed ahead of the commit
    let remote_branch = api::remote::branches::get_by_name(remote_repo, &branch.name).await?;

    if remote_branch.is_none() {
        // If the remote branch does not exist then it is not ahead
        return Ok(false);
    }

    // Meaning we do not have the remote branch commit in our history
    Ok(!reader.commit_id_exists(&remote_branch.unwrap().commit_id))
}

async fn cannot_push_incomplete_history(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    local_head: &Commit,
    branch: &Branch,
) -> Result<bool, OxenError> {
    log::debug!("Checking if we can push incomplete history.");
    match api::remote::commits::list_commit_history(remote_repo, &branch.name).await {
        Err(_) => {
            return Ok(!api::local::commits::commit_history_is_complete(
                local_repo, local_head,
            ));
        }
        Ok(remote_history) => {
            let remote_head = remote_history.first().unwrap();
            log::debug!(
                "Checking between local head {:?} and remote head {:?} on branch {}",
                local_head,
                remote_head,
                branch.name
            );

            let commit_reader = CommitReader::new(local_repo)?;
            let merger = Merger::new(local_repo)?;

            let commits_to_push =
                merger.list_commits_between_commits(&commit_reader, remote_head, local_head)?;

            let commits_to_push: Vec<Commit> = commits_to_push
                .into_iter()
                .filter(|commit| {
                    !remote_history
                        .iter()
                        .any(|remote_commit| remote_commit.id == commit.id)
                })
                .collect();

            log::debug!("Found the following commits_to_push: {:?}", commits_to_push);
            // Ensure all `commits_to_push` are synced
            for commit in commits_to_push {
                if !index::commit_sync_status::commit_is_synced(local_repo, &commit) {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

async fn poll_until_synced(
    remote_repo: &RemoteRepository,
    commit: &Commit,
    bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    let commits_to_sync = bar.length().unwrap();

    let head_commit_id = &commit.id;

    let mut retries = 0;

    loop {
        match api::remote::commits::latest_commit_synced(remote_repo, head_commit_id).await {
            Ok(sync_status) => {
                retries = 0;
                log::debug!("Got latest synced commit {:?}", sync_status.latest_synced);
                log::debug!("Got n unsynced commits {:?}", sync_status.num_unsynced);
                bar.set_position(commits_to_sync - sync_status.num_unsynced as u64);
                if sync_status.num_unsynced == 0 {
                    bar.finish_and_clear();
                    println!("🎉 Push successful");
                    return Ok(());
                }
            }
            Err(err) => {
                retries += 1;
                // Back off, but don't want to go all the way to 100s
                let sleep_time = 2 * retries;
                if retries >= NUM_HTTP_RETRIES {
                    bar.finish_and_clear();
                    return Err(err);
                }
                log::warn!(
                    "Server error encountered, retrying... ({}/{})",
                    retries,
                    NUM_HTTP_RETRIES
                );
                // Extra sleep time in error cases
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
}

async fn push_missing_commit_dbs(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    unsynced_commits: Vec<Commit>,
) -> Result<(), OxenError> {
    let pieces_of_work = unsynced_commits.len();

    if pieces_of_work == 0 {
        return Ok(());
    }

    let pb = oxen_progress_bar_with_msg(pieces_of_work as u64, "Syncing databases");

    // Compute size for this subset of entries
    let num_chunks = num_cpus::get();
    let mut chunk_size = pieces_of_work / num_chunks;
    if num_chunks > pieces_of_work {
        chunk_size = pieces_of_work;
    }

    // Split into chunks, process in parallel, and post to server
    use tokio::time::sleep;
    type PieceOfWork = (
        LocalRepository,
        RemoteRepository,
        Vec<Commit>,
        Arc<ProgressBar>,
    );
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
    type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

    log::debug!(
        "Creating {num_chunks} chunks from {pieces_of_work} commits with size {chunk_size}"
    );
    let chunks: Vec<PieceOfWork> = unsynced_commits
        .chunks(chunk_size)
        .map(|commits| {
            (
                local_repo.to_owned(),
                remote_repo.to_owned(),
                commits.to_owned(),
                pb.to_owned(),
            )
        })
        .collect();

    let worker_count: usize = num_cpus::get();
    let queue = Arc::new(TaskQueue::new(chunks.len()));
    let finished_queue = Arc::new(FinishedTaskQueue::new(unsynced_commits.len()));
    for chunk in chunks {
        queue.try_push(chunk).unwrap();
        finished_queue.try_push(false).unwrap();
    }

    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        tokio::spawn(async move {
            loop {
                let (local_repo, remote_repo, commits, bar) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);
                for commit in &commits {
                    match api::remote::commits::post_commit_db_to_server(
                        &local_repo,
                        &remote_repo,
                        commit,
                    )
                    .await
                    {
                        Ok(_) => {
                            log::debug!("worker[{}] posted commit to server", worker);
                            bar.inc(1);
                        }
                        Err(err) => {
                            log::error!(
                                "worker[{}] failed to post commit to server: {}",
                                worker,
                                err
                            );
                        }
                    }
                }
                finished_queue.pop().await;
            }
        });
    }
    while finished_queue.len() > 0 {
        // log::debug!("Waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_secs(1)).await;
    pb.finish_and_clear();
    Ok(())
}

async fn push_missing_commit_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    branch: &Branch,
    commits: &Vec<Commit>,
    mut unsynced_entries: Vec<UnsyncedCommitEntries>,
    mut total_size: u64,
) -> Result<(), OxenError> {
    // If no commits, nothing to do here. If no entries, but still commits to sync, need to do this step
    // TODO: maybe factor validation into a separate fourth step so that this can be skipped if no entries
    if commits.is_empty() {
        return Ok(());
    }

    log::debug!("push_missing_commit_entries num unsynced {}", commits.len());

    let spinner = spinner_with_msg(format!(
        "\n🐂 Collecting files for {} commits",
        commits.len()
    ));

    // Find the commits that still have unsynced entries (some might already be synced)
    // Collect them and calculate the new size to send
    let commit_reader = CommitReader::new(local_repo)?;

    for commit in commits {
        // Only if the commit is not already accounted for in unsynced entries - avoid double counting
        if !unsynced_entries.iter().any(|u| u.commit.id == commit.id) {
            let (commit_unsynced_commits, commit_size) =
                get_unsynced_entries_for_commit(local_repo, commit, &commit_reader)?;
            total_size += commit_size;
            unsynced_entries.extend(commit_unsynced_commits);
        }
    }

    let unsynced_entries: Vec<CommitEntry> = unsynced_entries
        .iter()
        .flat_map(|u: &UnsyncedCommitEntries| u.entries.clone())
        .collect();

    spinner.finish_and_clear();

    println!(
        "🐂 Pushing {}",
        bytesize::ByteSize::b(total_size)
    );

    // TODO - we can probably take commits out of this flow entirely, but it disrupts a bit rn so want to make sure this is stable first
    // For now, will send the HEAD commit through for logging purposes
    if !unsynced_entries.is_empty() {
        let all_entries = UnsyncedCommitEntries {
            commit: commits[0].clone(), // New head commit. Guaranteed to be here by earlier guard
            entries: unsynced_entries,
        };

        let bar = oxen_progress_bar(total_size, ProgressBarType::Bytes);
        push_entries(
            local_repo,
            remote_repo,
            &all_entries.entries,
            &all_entries.commit,
            &bar,
        )
        .await?;
    } else {
        println!("🐂 No entries to push");
    }

    // Even if there are no entries, there may still be commits we need to call post-push on (esp initial commits)
    // let old_to_new_commits: Vec<Commit> = commits.iter().rev().cloned().collect();
    api::remote::commits::bulk_post_push_complete(remote_repo, commits).await?;
    // Re-validate last commit to sent latest commit for Hub. TODO: do this non-duplicatively

    api::remote::commits::post_push_complete(remote_repo, branch, &commits.last().unwrap().id)
        .await?;

    log::debug!("push_missing_commit_entries done");

    Ok(())
}

pub fn read_unsynced_entries(
    local_repo: &LocalRepository,
    last_commit: &Commit,
    this_commit: &Commit,
) -> Result<Vec<CommitEntry>, OxenError> {
    // Find and compare all entries between this commit and last
    let this_entry_reader = CommitEntryReader::new(local_repo, this_commit)?;

    let this_entries = this_entry_reader.list_entries()?;
    let grouped = api::local::entries::group_entries_to_parent_dirs(&this_entries);
    log::debug!(
        "Checking {} entries in {} groups",
        this_entries.len(),
        grouped.len()
    );

    let mut entries_to_sync: Vec<CommitEntry> = vec![];
    for (dir, dir_entries) in grouped.iter() {
        log::debug!("Checking {} entries from {:?}", dir_entries.len(), dir);

        let last_entry_reader = CommitDirEntryReader::new(local_repo, &last_commit.id, dir)?;
        let mut entries: Vec<CommitEntry> = dir_entries
            .into_par_iter()
            .filter(|entry| {
                // If hashes are different, or it is a new entry, we'll keep it
                let filename = entry.path.file_name().unwrap().to_str().unwrap();
                match last_entry_reader.get_entry(filename) {
                    Ok(Some(old_entry)) => {
                        if old_entry.hash != entry.hash {
                            return true;
                        }
                    }
                    Ok(None) => {
                        return true;
                    }
                    Err(err) => {
                        panic!("Error filtering entries to sync: {}", err)
                    }
                }
                false
            })
            .map(|e| e.to_owned())
            .collect();
        entries_to_sync.append(&mut entries);
    }

    log::debug!("Got {} entries to sync", entries_to_sync.len());

    Ok(entries_to_sync)
}

async fn push_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: &[CommitEntry],
    commit: &Commit,
    bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    log::debug!(
        "PUSH ENTRIES {} -> {} -> '{}'",
        entries.len(),
        commit.id,
        commit.message
    );
    // Some files may be much larger than others....so we can't just zip them up and send them
    // since bodies will be too big. Hence we chunk and send the big ones, and bundle and send the small ones

    // For files smaller than AVG_CHUNK_SIZE, we are going to group them, zip them up, and transfer them
    let smaller_entries: Vec<CommitEntry> = entries
        .iter()
        .filter(|e| e.num_bytes < AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    // For files larger than AVG_CHUNK_SIZE, we are going break them into chunks and send the chunks in parallel
    let larger_entries: Vec<CommitEntry> = entries
        .iter()
        .filter(|e| e.num_bytes > AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    let large_entries_sync = chunk_and_send_large_entries(
        local_repo,
        remote_repo,
        larger_entries,
        commit,
        AVG_CHUNK_SIZE,
        bar,
    );
    let small_entries_sync = bundle_and_send_small_entries(
        local_repo,
        remote_repo,
        smaller_entries,
        commit,
        AVG_CHUNK_SIZE,
        bar,
    );

    match tokio::join!(large_entries_sync, small_entries_sync) {
        (Ok(_), Ok(_)) => {
            log::debug!("Moving on to post-push validation");
            Ok(())
        }
        (Err(err), Ok(_)) => {
            let err = format!("Error syncing large entries: {err}");
            Err(OxenError::basic_str(err))
        }
        (Ok(_), Err(err)) => {
            let err = format!("Error syncing small entries: {err}");
            Err(OxenError::basic_str(err))
        }
        _ => Err(OxenError::basic_str("Unknown error syncing entries")),
    }
}

async fn chunk_and_send_large_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: Vec<CommitEntry>,
    commit: &Commit,
    chunk_size: u64,
    bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }

    use tokio::time::sleep;
    type PieceOfWork = (
        CommitEntry,
        LocalRepository,
        Commit,
        RemoteRepository,
        Arc<ProgressBar>,
    );
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
                bar.to_owned(),
            )
        })
        .collect();

    let queue = Arc::new(TaskQueue::new(entries.len()));
    let finished_queue = Arc::new(FinishedTaskQueue::new(entries.len()));
    for entry in entries.iter() {
        queue.try_push(entry.to_owned()).unwrap();
        finished_queue.try_push(false).unwrap();
    }

    let worker_count: usize = if num_cpus::get() > entries.len() {
        entries.len()
    } else {
        num_cpus::get()
    };

    log::debug!(
        "worker_count {} entries len {}",
        worker_count,
        entries.len()
    );
    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        tokio::spawn(async move {
            loop {
                let (entry, repo, commit, remote_repo, bar) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);

                // Open versioned file
                let version_path = util::fs::version_path(&repo, &entry);
                let f = std::fs::File::open(&version_path).unwrap();
                let mut reader = BufReader::new(f);

                // Read chunks
                let total_size = entry.num_bytes;
                let num_chunks = ((total_size / chunk_size) + 1) as usize;
                let mut total_read = 0;
                let mut chunk_size = chunk_size;

                // TODO: We could probably upload chunks in parallel too
                for i in 0..num_chunks {
                    // Make sure we read the last size correctly
                    if (total_read + chunk_size) > total_size {
                        chunk_size = total_size % chunk_size;
                    }

                    // Only read as much as you need to send so we don't blow up memory on large files
                    let mut buffer = vec![0u8; chunk_size as usize];
                    reader.read_exact(&mut buffer).unwrap();
                    total_read += chunk_size;

                    let size = buffer.len() as u64;
                    log::debug!("Got entry buffer of size {}", size);

                    // Send data to server
                    let is_compressed = false;
                    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
                    let path = util::fs::path_relative_to_dir(&version_path, &hidden_dir).unwrap();
                    let file_name = Some(String::from(path.to_str().unwrap()));

                    let params = ChunkParams {
                        chunk_num: i,
                        total_chunks: num_chunks,
                        total_size: total_size as usize,
                    };

                    match api::remote::commits::upload_data_chunk_to_server_with_retry(
                        &remote_repo,
                        &commit,
                        &buffer,
                        &entry.hash,
                        &params,
                        is_compressed,
                        &file_name,
                    )
                    .await
                    {
                        Ok(_) => {
                            bar.inc(chunk_size);
                            log::debug!("Successfully uploaded chunk {}/{}", i, num_chunks)
                        }
                        Err(err) => {
                            log::error!("Error uploading chunk: {:?}", err)
                        }
                    }
                }

                finished_queue.pop().await;
            }
        });
    }

    while finished_queue.len() > 0 {
        // log::debug!("Before waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All large file tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Sends entries in tarballs of size ~chunk size
async fn bundle_and_send_small_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: Vec<CommitEntry>,
    commit: &Commit,
    avg_chunk_size: u64,
    bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }

    // Compute size for this subset of entries
    let total_size = api::local::entries::compute_entries_size(&entries)?;
    let num_chunks = ((total_size / avg_chunk_size) + 1) as usize;

    let mut chunk_size = entries.len() / num_chunks;
    if num_chunks > entries.len() {
        chunk_size = entries.len();
    }

    // Split into chunks, zip up, and post to server
    use tokio::time::sleep;
    type PieceOfWork = (
        Vec<CommitEntry>,
        LocalRepository,
        Commit,
        RemoteRepository,
        Arc<ProgressBar>,
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
                bar.to_owned(),
            )
        })
        .collect();

    let worker_count: usize = num_cpus::get();
    let queue = Arc::new(TaskQueue::new(chunks.len()));
    let finished_queue = Arc::new(FinishedTaskQueue::new(entries.len()));
    for chunk in chunks {
        queue.try_push(chunk).unwrap();
        finished_queue.try_push(false).unwrap();
    }

    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        tokio::spawn(async move {
            loop {
                let (chunk, repo, commit, remote_repo, bar) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);

                let enc = GzEncoder::new(Vec::new(), Compression::default());
                let mut tar = tar::Builder::new(enc);
                log::debug!("Chunk size {}", chunk.len());
                let chunk_size = match compute_entries_size(&chunk) {
                    Ok(size) => size,
                    Err(e) => {
                        log::error!("Failed to compute entries size: {}", e);
                        continue; // or break or decide on another error-handling strategy
                    }
                };

                log::debug!("got repo {:?}", &repo.path);
                for entry in chunk.into_iter() {
                    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
                    let version_path = util::fs::version_path(&repo, &entry);
                    let name = util::fs::path_relative_to_dir(&version_path, &hidden_dir).unwrap();

                    tar.append_path_with_name(version_path, name).unwrap();
                }

                let buffer = match tar.into_inner() {
                    Ok(gz_encoder) => match gz_encoder.finish() {
                        Ok(buffer) => {
                            let size = buffer.len() as u64;
                            log::debug!("Got tarball buffer of size {}", size);
                            buffer
                        }
                        Err(err) => {
                            panic!("Error creating tar.gz on entries: {}", err)
                        }
                    },
                    Err(err) => {
                        panic!("Error creating tar of entries: {}", err)
                    }
                };

                // Send tar.gz to server
                let is_compressed = true;
                let file_name = None;

                // TODO: Refactor where the bars are being passed so we don't need silent here
                let quiet_bar = Arc::new(ProgressBar::hidden());

                match api::remote::commits::post_data_to_server(
                    &remote_repo,
                    &commit,
                    buffer,
                    is_compressed,
                    &file_name,
                    quiet_bar,
                )
                .await
                {
                    Ok(_) => {
                        log::debug!("Successfully uploaded data!")
                    }
                    Err(err) => {
                        log::error!("Error uploading chunk: {:?}", err)
                    }
                }
                bar.inc(chunk_size);
                finished_queue.pop().await;
            }
        });
    }
    while finished_queue.len() > 0 {
        // log::debug!("Waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::core::index::pusher;
    use crate::core::index::CommitReader;
    use crate::error::OxenError;
    use crate::opts::RmOpts;
    use crate::util;

    use crate::test;

    #[tokio::test]
    async fn test_push_missing_commit_objects() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let remote_repo = api::remote::repositories::create(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            // Get commits to sync...
            let head_commit = api::local::commits::head_commit(&repo)?;
            let branch = api::local::branches::current_branch(&repo)?.unwrap();

            let unsynced_commits =
                pusher::get_commit_objects_to_sync(&repo, &remote_repo, &head_commit, &branch)
                    .await?;

            // Root commit is created w/ the repo, so there should be 1 unsynced commit (the follow-on)
            assert_eq!(unsynced_commits.len(), 1);

            // Push commit objects only
            pusher::push_missing_commit_objects(&repo, &remote_repo, &unsynced_commits, &branch)
                .await?;

            // There should be none unsynced
            let head_commit = api::local::commits::head_commit(&repo)?;
            let unsynced_commits =
                pusher::get_commit_objects_to_sync(&repo, &remote_repo, &head_commit, &branch)
                    .await?;

            assert_eq!(unsynced_commits.len(), 0);

            // Full push to clear out
            command::push(&repo).await?;

            // Modify README
            let readme_path = repo.path.join("README.md");
            let readme_path = test::modify_txt_file(readme_path, "I am the readme now.")?;
            command::add(&repo, readme_path)?;

            // Commit again
            let head_commit = command::commit(&repo, "Changed the readme")?;
            let unsynced_commits =
                pusher::get_commit_objects_to_sync(&repo, &remote_repo, &head_commit, &branch)
                    .await?;

            println!("Num unsynced {}", unsynced_commits.len());
            for commit in unsynced_commits.iter() {
                println!("FOUND UNSYNCED: {:?}", commit);
            }

            // Should be one more
            assert_eq!(unsynced_commits.len(), 1);

            // Push commit objects only
            pusher::push_missing_commit_objects(&repo, &remote_repo, &unsynced_commits, &branch)
                .await?;

            // There should be none unsynced
            let head_commit = api::local::commits::head_commit(&repo)?;
            let unsynced_commits =
                pusher::get_commit_objects_to_sync(&repo, &remote_repo, &head_commit, &branch)
                    .await?;

            assert_eq!(unsynced_commits.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_push_missing_commit_dbs() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let remote_repo = api::remote::repositories::create(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            // Get commits to sync...
            let head_commit = api::local::commits::head_commit(&repo)?;
            let branch = api::local::branches::current_branch(&repo)?.unwrap();

            // Create all commit objects
            let unsynced_commits =
                pusher::get_commit_objects_to_sync(&repo, &remote_repo, &head_commit, &branch)
                    .await?;
            pusher::push_missing_commit_objects(&repo, &remote_repo, &unsynced_commits, &branch)
                .await?;

            // Should have one missing commit db - root created on repo creation
            let unsynced_db_commits =
                api::remote::commits::get_commits_with_unsynced_dbs(&remote_repo, &branch).await?;
            assert_eq!(unsynced_db_commits.len(), 1);

            // Push to the remote
            pusher::push_missing_commit_dbs(&repo, &remote_repo, unsynced_db_commits).await?;

            // All commits should now have dbs
            let unsynced_db_commits =
                api::remote::commits::get_commits_with_unsynced_dbs(&remote_repo, &branch).await?;
            assert_eq!(unsynced_db_commits.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_push_missing_commit_entries() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let remote_repo = api::remote::repositories::create(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            // Get commits to sync...
            let head_commit = api::local::commits::head_commit(&repo)?;
            let branch = api::local::branches::current_branch(&repo)?.unwrap();

            // Get missing commit objects and push
            let unsynced_commits =
                pusher::get_commit_objects_to_sync(&repo, &remote_repo, &head_commit, &branch)
                    .await?;
            pusher::push_missing_commit_objects(&repo, &remote_repo, &unsynced_commits, &branch)
                .await?;

            // Get missing commit dbs and push
            let unsynced_db_commits =
                api::remote::commits::get_commits_with_unsynced_dbs(&remote_repo, &branch).await?;
            pusher::push_missing_commit_dbs(&repo, &remote_repo, unsynced_db_commits).await?;

            // 2 commit should be missing - commit object and db created on repo creation, but entries not synced
            let unsynced_entries_commits =
                api::remote::commits::get_commits_with_unsynced_entries(&remote_repo, &branch)
                    .await?;
            assert_eq!(unsynced_entries_commits.len(), 2);

            // Full push (to catch the final poll_until_synced)
            // Since the other two steps have already been enumerated above, this effectively just tests `push_missing_commit_entries` with the wrapup that sets CONTENT_IS_VALID
            command::push(&repo).await?;

            // All should now be synced
            let unsynced_entries_commits =
                api::remote::commits::get_commits_with_unsynced_entries(&remote_repo, &branch)
                    .await?;
            assert_eq!(unsynced_entries_commits.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_push_only_one_modified_file() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            // Get original branch
            let branch = api::local::branches::current_branch(&local_repo)?.unwrap();

            // Move the README to a new file name
            let readme_path = local_repo.path.join("README.md");
            let new_path = local_repo.path.join("README2.md");
            util::fs::rename(&readme_path, &new_path)?;

            command::add(&local_repo, new_path)?;
            let rm_opts = RmOpts::from_path("README.md");
            command::rm(&local_repo, &rm_opts).await?;
            let commit = command::commit(&local_repo, "Moved the readme")?;

            // All remote entries should by synced
            let unsynced_entries_commits =
                api::remote::commits::get_commits_with_unsynced_entries(&remote_repo, &branch)
                    .await?;
            assert_eq!(unsynced_entries_commits.len(), 0);

            let commit_reader = CommitReader::new(&local_repo)?;
            // We should only have one unsynced commit and one unsynced entry
            let (commit_unsynced_commits, _) =
                pusher::get_unsynced_entries_for_commit(&local_repo, &commit, &commit_reader)?;

            assert_eq!(commit_unsynced_commits.len(), 1);
            assert_eq!(commit_unsynced_commits[0].entries.len(), 1);

            command::push(&local_repo).await?;

            // All remote entries should by synced
            let unsynced_entries_commits =
                api::remote::commits::get_commits_with_unsynced_entries(&remote_repo, &branch)
                    .await?;
            assert_eq!(unsynced_entries_commits.len(), 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_move_entire_directory() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            // Get original branch
            let branch = api::local::branches::current_branch(&local_repo)?.unwrap();

            // Move the README to a new file name
            let train_images = local_repo.path.join("train");
            let new_path = local_repo.path.join("images").join("train");
            util::fs::create_dir_all(local_repo.path.join("images"))?;
            util::fs::rename(&train_images, &new_path)?;

            command::add(&local_repo, new_path)?;
            let mut rm_opts = RmOpts::from_path("train");
            rm_opts.recursive = true;
            command::rm(&local_repo, &rm_opts).await?;
            let commit =
                command::commit(&local_repo, "Moved all the train image files to images/")?;

            // All remote entries should by synced
            let unsynced_entries_commits =
                api::remote::commits::get_commits_with_unsynced_entries(&remote_repo, &branch)
                    .await?;
            assert_eq!(unsynced_entries_commits.len(), 0);

            let commit_reader = CommitReader::new(&local_repo)?;
            // We should have 5 unsynced entries
            let (commit_unsynced_commits, _) =
                pusher::get_unsynced_entries_for_commit(&local_repo, &commit, &commit_reader)?;

            assert_eq!(commit_unsynced_commits.len(), 1);
            assert_eq!(commit_unsynced_commits[0].entries.len(), 5);

            command::push(&local_repo).await?;

            // All remote entries should by synced
            let unsynced_entries_commits =
                api::remote::commits::get_commits_with_unsynced_entries(&remote_repo, &branch)
                    .await?;
            assert_eq!(unsynced_entries_commits.len(), 0);

            // Add a single new file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            command::add(&local_repo, new_file)?;
            let commit = command::commit(&local_repo, "Added a new file")?;

            // We should have 1 unsynced entry
            let (commit_unsynced_commits, _) =
                pusher::get_unsynced_entries_for_commit(&local_repo, &commit, &commit_reader)?;

            assert_eq!(commit_unsynced_commits.len(), 1);
            assert_eq!(commit_unsynced_commits[0].entries.len(), 1);

            command::push(&local_repo).await?;

            Ok(remote_repo)
        })
        .await
    }
}
