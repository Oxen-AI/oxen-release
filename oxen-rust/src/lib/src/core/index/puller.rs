//! Pulls commits and entries from the remote repository
//!

use std::path::{Path, PathBuf};
use std::sync::Arc;

use indicatif::ProgressBar;

use crate::api;
use crate::constants::AVG_CHUNK_SIZE;
use crate::error::OxenError;
use crate::model::{CommitEntry, RemoteRepository};
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{current_function, util};

type SmallEntryPathFn = dyn Fn(&[CommitEntry], &Path) -> Vec<(String, PathBuf)>;
type LargeEntryPathFn = dyn Fn(&[CommitEntry], &Path) -> Vec<PathBuf>;

pub async fn pull_entries(
    remote_repo: &RemoteRepository,
    entries: &Vec<CommitEntry>,
    dst: impl AsRef<Path>,
    get_small_entry_paths: &SmallEntryPathFn,
    get_large_entry_paths: &LargeEntryPathFn,
    on_complete: &dyn Fn(),
) -> Result<(), OxenError> {
    log::debug!("🐂 {} entries.len() {}", current_function!(), entries.len());

    if entries.is_empty() {
        return Ok(());
    }

    let missing_entries = get_missing_commit_entries(entries, &dst);

    log::debug!(
        "Pull entries {} missing_entries.len() {}",
        current_function!(),
        missing_entries.len()
    );

    if missing_entries.is_empty() {
        return Ok(());
    }

    let total_size = api::local::entries::compute_entries_size(&missing_entries)?;
    println!(
        "Downloading {} files ({})",
        missing_entries.len(),
        bytesize::ByteSize::b(total_size)
    );

    // Some files may be much larger than others....so we can't just download them within a single body
    // Hence we chunk and send the big ones, and bundle and download the small ones

    // For files smaller than AVG_CHUNK_SIZE, we are going to group them, zip them up, and transfer them
    let smaller_entries: Vec<CommitEntry> = missing_entries
        .iter()
        .filter(|e| e.num_bytes < AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    // For files larger than AVG_CHUNK_SIZE, we are going break them into chunks and download the chunks in parallel
    let larger_entries: Vec<CommitEntry> = missing_entries
        .iter()
        .filter(|e| e.num_bytes > AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    // Progress bar to be shared between small and large entries
    let bar = oxen_progress_bar(total_size, ProgressBarType::Bytes);

    let small_entry_paths = get_small_entry_paths(&smaller_entries, dst.as_ref());
    let large_entry_paths = get_large_entry_paths(&larger_entries, dst.as_ref());

    let large_entries_sync =
        pull_large_entries(remote_repo, larger_entries, &dst, large_entry_paths, &bar);
    let small_entries_sync =
        pull_small_entries(remote_repo, smaller_entries, &dst, small_entry_paths, &bar);

    match tokio::join!(large_entries_sync, small_entries_sync) {
        (Ok(_), Ok(_)) => {
            log::debug!("Successfully synced entries!");
            bar.finish_and_clear();
            on_complete();
        }
        (Err(err), Ok(_)) => {
            let err = format!("Error syncing large entries: {err}");
            return Err(OxenError::basic_str(err));
        }
        (Ok(_), Err(err)) => {
            let err = format!("Error syncing small entries: {err}");
            return Err(OxenError::basic_str(err));
        }
        _ => return Err(OxenError::basic_str("Unknown error syncing entries")),
    }

    Ok(())
}

fn get_missing_commit_entries(entries: &[CommitEntry], dst: impl AsRef<Path>) -> Vec<CommitEntry> {
    let dst = dst.as_ref();
    let mut missing_entries: Vec<CommitEntry> = vec![];

    for entry in entries {
        let version_path = util::fs::version_path_from_dst(dst, entry);
        if !version_path.exists() {
            missing_entries.push(entry.to_owned())
        }
    }

    missing_entries
}

async fn pull_small_entries(
    remote_repo: &RemoteRepository,
    entries: Vec<CommitEntry>,
    dst: impl AsRef<Path>,
    content_ids: Vec<(String, PathBuf)>,
    bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    if content_ids.is_empty() {
        return Ok(());
    }

    let total_size = api::local::entries::compute_entries_size(&entries)?;

    // Compute num chunks
    let num_chunks = ((total_size / AVG_CHUNK_SIZE) + 1) as usize;

    let mut chunk_size = entries.len() / num_chunks;
    if num_chunks > entries.len() {
        chunk_size = entries.len();
    }

    log::debug!(
        "pull_entries_for_commit got {} missing content IDs",
        content_ids.len()
    );

    // Split into chunks, zip up, and post to server
    use tokio::time::{sleep, Duration};
    type PieceOfWork = (
        RemoteRepository,
        Vec<(String, PathBuf)>,
        PathBuf,
        Arc<ProgressBar>,
    );
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
    type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

    log::debug!("pull_small_entries creating {num_chunks} chunks from {total_size} bytes with size {chunk_size}");
    let chunks: Vec<PieceOfWork> = content_ids
        .chunks(chunk_size)
        .map(|chunk| {
            (
                remote_repo.to_owned(),
                chunk.to_owned(),
                dst.as_ref().to_owned(),
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
                let (remote_repo, chunk, path, bar) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);

                match api::remote::entries::download_data_from_version_paths(
                    &remote_repo,
                    &chunk,
                    &path,
                )
                .await
                {
                    Ok(download_size) => {
                        bar.inc(download_size);
                    }
                    Err(err) => {
                        log::error!("Could not download entries... {}", err)
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

// async fn pull_small_entries_to_working_dir(
//     remote_repo: &RemoteRepository,
//     entries: Vec<CommitEntry>,
//     dst: impl AsRef<Path>,
//     bar: &Arc<ProgressBar>,
// ) -> Result<(), OxenError> {
//     let content_ids = version_paths_from_entries(&entries, dst.as_ref());

//     pull_small_entries(remote_repo, entries, dst, content_ids, bar).await?;

//     Ok(())
// }

// async fn pull_small_entries_to_versions_dir(
//     remote_repo: &RemoteRepository,
//     entries: Vec<CommitEntry>,
//     dst: impl AsRef<Path>,
//     bar: &Arc<ProgressBar>,
// ) -> Result<(), OxenError> {
//     let content_ids = version_dir_paths_from_entries(&entries, dst.as_ref());

//     pull_small_entries(remote_repo, entries, dst, content_ids, bar).await?;
//     Ok(())
// }

/// Returns a mapping from content_id -> entry.path
fn working_dir_paths_from_small_entries(
    entries: &[CommitEntry],
    dst: &Path,
) -> Vec<(String, PathBuf)> {
    let mut content_ids: Vec<(String, PathBuf)> = vec![];

    for entry in entries.iter() {
        let version_path = util::fs::version_path_from_dst(dst, entry);
        let version_path = util::fs::path_relative_to_dir(&version_path, dst).unwrap();

        content_ids.push((
            String::from(version_path.to_str().unwrap()).replace('\\', "/"),
            entry.path.to_owned(),
        ));
    }

    content_ids
}

// This one redundantly is just going to pass in two copies of
// the version path so we don't have to change download_data_from_version_paths
fn version_dir_paths_from_small_entries(
    entries: &[CommitEntry],
    dst: &Path,
) -> Vec<(String, PathBuf)> {
    let mut content_ids: Vec<(String, PathBuf)> = vec![];
    for entry in entries.iter() {
        let version_path = util::fs::version_path_from_dst(dst, entry);
        let version_path = util::fs::path_relative_to_dir(&version_path, dst).unwrap();

        content_ids.push((
            String::from(version_path.to_str().unwrap()).replace('\\', "/"),
            version_path.to_owned(),
        ))
    }
    content_ids
}

async fn pull_large_entries(
    remote_repo: &RemoteRepository,
    entries: Vec<CommitEntry>,
    dst: impl AsRef<Path>,
    download_paths: Vec<PathBuf>,
    bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }
    // Pull the large entries in parallel
    use tokio::time::{sleep, Duration};
    type PieceOfWork = (
        RemoteRepository,
        CommitEntry,
        PathBuf,
        PathBuf,
        Arc<ProgressBar>,
    );
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
    type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

    log::debug!("Chunking and sending {} larger files", entries.len());
    let entries: Vec<PieceOfWork> = entries
        .iter()
        .zip(download_paths.iter())
        .map(|(e, path)| {
            (
                remote_repo.to_owned(),
                e.to_owned(),
                dst.as_ref().to_owned(),
                path.to_owned(),
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
    let tmp_dir = util::fs::oxen_hidden_dir(dst).join("tmp").join("pulled");
    log::debug!("Backing up pulls to tmp dir: {:?}", &tmp_dir);
    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        tokio::spawn(async move {
            loop {
                let (remote_repo, entry, _dst, download_path, bar) = queue.pop().await;

                log::debug!("worker[{}] processing task...", worker);

                // Chunk and individual files
                let remote_path = &entry.path;

                // let download_path = path.join(&entry.path);

                // Download to the tmp path, then copy over to the entries dir
                match api::remote::entries::download_large_entry(
                    &remote_repo,
                    &remote_path,
                    &download_path,
                    &entry.commit_id,
                    entry.num_bytes,
                    bar,
                )
                .await
                {
                    Ok(_) => {
                        log::debug!("Downloaded large entry {:?} to versions dir", remote_path);
                    }
                    Err(err) => {
                        log::error!("Could not download chunk... {}", err)
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

fn version_dir_paths_from_large_entries(entries: &[CommitEntry], dst: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = vec![];
    for entry in entries.iter() {
        let version_path = util::fs::version_path_from_dst(dst, entry);
        paths.push(version_path);
    }
    paths
}

fn working_dir_paths_from_large_entries(entries: &[CommitEntry], dst: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = vec![];
    for entry in entries.iter() {
        let working_path = dst.join(&entry.path);
        paths.push(working_path);
    }
    paths
}

// async fn pull_large_entries_to_working_dir(
//     remote_repo: &RemoteRepository,
//     entries: Vec<CommitEntry>,
//     dst: impl AsRef<Path>,
//     bar: &Arc<ProgressBar>,
// ) -> Result<(), OxenError> {
//     if entries.is_empty() {
//         return Ok(());
//     }
//     // Pre-calculate paths
//     let working_paths: Vec<PathBuf> = entries.iter().map(|e| dst.as_ref().join(&e.path)).collect();
//     // Pull the entries
//     pull_large_entries(remote_repo, entries, dst, working_paths, bar).await?;

//     Ok(())
// }

// async fn pull_large_entries_to_versions_dir(
//     remote_repo: &RemoteRepository,
//     entries: Vec<CommitEntry>,
//     dst: impl AsRef<Path>,
//     bar: &Arc<ProgressBar>,
// ) -> Result<(), OxenError> {
//     if entries.is_empty() {
//         return Ok(());
//     }

//     // Pre-calculate paths
//     let version_paths: Vec<PathBuf> = entries.iter().map(|e| util::fs::version_path_from_dst(&dst, e)).collect();

//     // Pull the entries
//     pull_large_entries(remote_repo, entries, dst, version_paths, bar).await?;

//     Ok(())
// }

pub async fn pull_entries_to_versions_dir(
    remote_repo: &RemoteRepository,
    entries: &Vec<CommitEntry>,
    dst: impl AsRef<Path>,
    on_complete: &dyn Fn(),
) -> Result<(), OxenError> {
    pull_entries(
        remote_repo,
        entries,
        dst,
        &version_dir_paths_from_small_entries,
        &version_dir_paths_from_large_entries,
        &on_complete,
    )
    .await?;
    Ok(())
}

pub async fn pull_entries_to_working_dir(
    remote_repo: &RemoteRepository,
    entries: &Vec<CommitEntry>,
    dst: impl AsRef<Path>,
    on_complete: &dyn Fn(),
) -> Result<(), OxenError> {
    pull_entries(
        remote_repo,
        entries,
        dst,
        &working_dir_paths_from_small_entries,
        &working_dir_paths_from_large_entries,
        &on_complete,
    )
    .await?;
    Ok(())
}
