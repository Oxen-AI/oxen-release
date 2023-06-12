//! Pushes commits and entries to the remote repository
//!

use crate::api::remote::commits::ChunkParams;
use async_recursion::async_recursion;
use bytesize::ByteSize;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::io::{BufReader, Read};
use std::sync::Arc;

use crate::constants::AVG_CHUNK_SIZE;
use crate::core::index::{CommitDirEntryReader, CommitEntryReader, CommitReader, RefReader};
use crate::error::OxenError;
use crate::model::{Branch, Commit, CommitEntry, LocalRepository, RemoteBranch, RemoteRepository};
use crate::{api, util};

pub struct UnsyncedCommitEntries {
    commit: Commit,
    entries: Vec<CommitEntry>,
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

pub async fn push_remote_repo(
    local_repo: &LocalRepository,
    remote_repo: RemoteRepository,
    branch: Branch,
) -> Result<RemoteRepository, OxenError> {
    // Push unsynced commit db and history dbs
    let commit_reader = CommitReader::new(local_repo)?;
    let head_commit = commit_reader
        .get_commit_by_id(&branch.commit_id)?
        .ok_or(OxenError::must_be_on_valid_branch())?;

    // Make sure the remote branch is not ahead of the local branch
    if remote_is_ahead_of_local(&remote_repo, &commit_reader, &branch).await? {
        return Err(OxenError::remote_ahead_of_local());
    }

    // This method will check with server to find out what commits need to be pushed
    // will fill in commits that are not synced
    let mut unsynced_commits: VecDeque<UnsyncedCommitEntries> = VecDeque::new();
    rpush_missing_commit_objects(
        local_repo,
        &remote_repo,
        &head_commit,
        &mut unsynced_commits,
        &branch,
    )
    .await?;

    // If there are any unsynced commits, sync their entries
    if !unsynced_commits.is_empty() {
        log::debug!(
            "Push entries for {} unsynced commits",
            unsynced_commits.len()
        );

        // recursively check commits against remote head
        // and sync ones that have not been synced
        rpush_entries(local_repo, &remote_repo, &unsynced_commits).await?;

        // update the branch after everything else is synced
        log::debug!(
            "Updating remote branch {:?} to commit {:?}",
            &branch.name,
            &head_commit
        );

        // Remotely validate commit
        // This is an async process on the server so good to stall the user here so they don't push again
        // If they did push again before this is finished they would get a still syncing error
        poll_until_synced(&remote_repo, &head_commit).await?;
    }

    // Update the remote branch name last
    api::remote::branches::update(&remote_repo, &branch.name, &head_commit).await?;
    println!(
        "Updated remote branch {} -> {}",
        &branch.name, &head_commit.id
    );

    Ok(remote_repo)
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

async fn poll_until_synced(
    remote_repo: &RemoteRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    println!("Remote verifying commit...");
    let progress = ProgressBar::new_spinner();

    loop {
        progress.tick();
        match api::remote::commits::commit_is_synced(remote_repo, &commit.id).await {
            Ok(Some(sync_status)) => {
                if sync_status.is_valid {
                    progress.finish();
                    println!("✅ push successful\n");
                    return Ok(());
                }
            }
            Ok(None) => {
                progress.finish();
                return Err(OxenError::basic_str("Err: Commit never got pushed"));
            }
            Err(err) => {
                progress.finish();
                return Err(err);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
}

#[async_recursion]
async fn rpush_missing_commit_objects(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    local_commit: &Commit,
    unsynced_commits: &mut VecDeque<UnsyncedCommitEntries>,
    branch: &Branch,
) -> Result<(), OxenError> {
    log::debug!(
        "rpush_missing_commit_objects START, checking local {} -> '{}'",
        local_commit.id,
        local_commit.message
    );

    // check if commit exists on remote
    // if not, push the commit and it's dbs
    match api::remote::commits::commit_is_synced(remote_repo, &local_commit.id).await {
        Ok(Some(sync_status)) => {
            if sync_status.is_valid {
                // Commit has been synced
                let commit_reader = CommitReader::new(local_repo)?;
                let head_commit = commit_reader.head_commit()?;

                // We have remote commit, stop syncing
                log::debug!(
                    "rpush_missing_commit_objects STOP, we have remote parent {} -> '{}' head {} -> '{}'",
                    local_commit.id,
                    local_commit.message,
                    head_commit.id,
                    head_commit.message
                );
            } else if sync_status.is_processing {
                // Print that last commit is still processing (or we may be in still caching values on the server)
                println!("Commit is still processing on server {}", local_commit.id);
            } else {
                return Err(OxenError::basic_str(sync_status.status_description));
            }
        }
        Ok(None) => {
            log::debug!(
                "rpush_missing_commit_objects CONTINUE Didn't find remote parent: {} -> '{}'",
                local_commit.id,
                local_commit.message
            );
            // We don't have remote commit
            // Recursively find local parent and remote parents
            for parent_id in local_commit.parent_ids.iter() {
                // We should have a local parent if the local_commit has parent id
                let local_parent = api::local::commits::get_by_id(local_repo, parent_id)?
                    .ok_or_else(|| OxenError::local_parent_link_broken(&local_commit.id))?;

                // Compute the diff from the parent that we are going to sync
                let entries = read_unsynced_entries(local_repo, &local_parent, local_commit)?;
                let entries_size = api::local::entries::compute_entries_size(&entries)?;

                rpush_missing_commit_objects(
                    local_repo,
                    remote_repo,
                    &local_parent,
                    unsynced_commits,
                    branch,
                )
                .await?;

                // Unroll and post commits
                api::remote::commits::post_commit_to_server(
                    local_repo,
                    remote_repo,
                    local_commit,
                    entries_size,
                    branch.name.to_owned(),
                )
                .await?;

                log::debug!(
                    "rpush_missing_commit_objects unsynced_commits.push_back commit -> {:?} parent {:?}",
                    local_commit.id, local_parent.id
                );

                unsynced_commits.push_back(UnsyncedCommitEntries {
                    commit: local_commit.to_owned(),
                    entries,
                });
            }

            log::debug!(
                "rpush_missing_commit_objects stop, no more local parents {} -> '{}'",
                local_commit.id,
                local_commit.message
            );

            if local_commit.parent_ids.is_empty() {
                // Create the root commit
                api::remote::commits::post_commit_to_server(
                    local_repo,
                    remote_repo,
                    local_commit,
                    0, // No entries
                    branch.name.to_owned(),
                )
                .await?;
                log::debug!("unsynced_commits.push_back root {:?}", local_commit);
                unsynced_commits.push_back(UnsyncedCommitEntries {
                    commit: local_commit.to_owned(),
                    entries: vec![],
                });
            }
        }
        Err(err) => {
            let err = format!("{err}");
            return Err(OxenError::basic_str(err));
        }
    }

    Ok(())
}

async fn rpush_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    unsynced_commits: &VecDeque<UnsyncedCommitEntries>,
) -> Result<(), OxenError> {
    log::debug!("rpush_entries num unsynced {}", unsynced_commits.len());
    for unsynced in unsynced_commits.iter() {
        let commit = &unsynced.commit;
        let entries = &unsynced.entries;

        println!(
            "Pushing commit {} entries: {} -> '{}'",
            entries.len(),
            commit.id,
            commit.message
        );

        push_entries(local_repo, remote_repo, entries, commit).await?;
    }
    Ok(())
}

fn read_unsynced_entries(
    local_repo: &LocalRepository,
    last_commit: &Commit,
    this_commit: &Commit,
) -> Result<Vec<CommitEntry>, OxenError> {
    println!("Computing delta {} -> {}", last_commit.id, this_commit.id);
    // Find and compare all entries between this commit and last
    let this_entry_reader = CommitEntryReader::new(local_repo, this_commit)?;

    let this_entries = this_entry_reader.list_entries()?;
    let grouped = api::local::entries::group_entries_to_parent_dirs(&this_entries);
    log::debug!(
        "Checking {} entries in {} groups",
        this_entries.len(),
        grouped.len()
    );

    let bar = ProgressBar::new(this_entries.len() as u64);
    let mut entries_to_sync: Vec<CommitEntry> = vec![];
    for (dir, dir_entries) in grouped.iter() {
        log::debug!("Checking {} entries from {:?}", dir_entries.len(), dir);

        let last_entry_reader = CommitDirEntryReader::new(local_repo, &last_commit.id, dir)?;
        let mut entries: Vec<CommitEntry> = dir_entries
            .into_par_iter()
            .filter(|entry| {
                bar.inc(1);
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
    bar.finish();

    log::debug!("Got {} entries to sync", entries_to_sync.len());

    Ok(entries_to_sync)
}

async fn push_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: &[CommitEntry],
    commit: &Commit,
) -> Result<(), OxenError> {
    log::debug!(
        "PUSH ENTRIES {} -> {} -> '{}'",
        entries.len(),
        commit.id,
        commit.message
    );

    println!("🐂 push computing size...");
    let total_size = api::local::entries::compute_entries_size(entries)?;

    if !entries.is_empty() {
        println!(
            "Pushing {} files with size {}",
            entries.len(),
            ByteSize::b(total_size)
        );
    }

    let bar = Arc::new(ProgressBar::new(total_size));

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
        &bar,
    );
    let small_entries_sync = bundle_and_send_small_entries(
        local_repo,
        remote_repo,
        smaller_entries,
        commit,
        AVG_CHUNK_SIZE,
        &bar,
    );

    match tokio::join!(large_entries_sync, small_entries_sync) {
        (Ok(_), Ok(_)) => api::remote::commits::post_push_complete(remote_repo, &commit.id).await,
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

    use tokio::time::{sleep, Duration};
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
                            bar.inc(buffer.len() as u64);
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
    use tokio::time::{sleep, Duration};
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
                match api::remote::commits::post_data_to_server(
                    &remote_repo,
                    &commit,
                    buffer,
                    is_compressed,
                    &file_name,
                    bar,
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
                finished_queue.pop().await;
            }
        });
    }
    while finished_queue.len() > 0 {
        // log::debug!("Waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_millis(1)).await;
    }
    log::debug!("All tasks done. :-)");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::core::index::pusher;
    use crate::core::index::pusher::UnsyncedCommitEntries;
    use crate::error::OxenError;

    use crate::test;

    #[tokio::test]
    async fn test_rpush_missing_commit_objects() -> Result<(), OxenError> {
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

            // Make a few more commits, and then make sure the total count is correct to push
            let head_commit = api::local::commits::head_commit(&repo)?;
            let branch = api::local::branches::current_branch(&repo)?.unwrap();
            let mut unsynced_commits: VecDeque<UnsyncedCommitEntries> = VecDeque::new();
            pusher::rpush_missing_commit_objects(
                &repo,
                &remote_repo,
                &head_commit,
                &mut unsynced_commits,
                &branch,
            )
            .await?;

            // The initial commit and the one after
            assert_eq!(unsynced_commits.len(), 2);

            // Push to the remote
            command::push(&repo).await?;

            // There should be none unsynced
            let head_commit = api::local::commits::head_commit(&repo)?;
            let mut unsynced_commits: VecDeque<UnsyncedCommitEntries> = VecDeque::new();
            pusher::rpush_missing_commit_objects(
                &repo,
                &remote_repo,
                &head_commit,
                &mut unsynced_commits,
                &branch,
            )
            .await?;

            // The initial commit and the one after
            assert_eq!(unsynced_commits.len(), 0);

            // Modify README
            let readme_path = repo.path.join("README.md");
            let readme_path = test::modify_txt_file(readme_path, "I am the readme now.")?;
            command::add(&repo, readme_path)?;

            // Commit again
            let head_commit = command::commit(&repo, "Changed the readme")?;
            let mut unsynced_commits: VecDeque<UnsyncedCommitEntries> = VecDeque::new();
            pusher::rpush_missing_commit_objects(
                &repo,
                &remote_repo,
                &head_commit,
                &mut unsynced_commits,
                &branch,
            )
            .await?;

            println!("Num unsynced {}", unsynced_commits.len());
            for commit in unsynced_commits.iter() {
                println!("FOUND UNSYNCED: {:?}", commit.commit);
            }

            // Should be one more
            assert_eq!(unsynced_commits.len(), 1);

            Ok(())
        })
        .await
    }
}
