

pub async fn download_dir(
    remote_repo: &RemoteRepository,
    entry: &MetadataEntry,
    local_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // Download the commit db for the given commit id or branch
    let commit_id = &entry.resource.as_ref().unwrap().commit.as_ref().unwrap().id;
    let home_dir = util::fs::oxen_tmp_dir()?;
    let repo_dir = home_dir
        .join(&remote_repo.namespace)
        .join(&remote_repo.name);
    let repo_cache_dir = repo_dir.join(OXEN_HIDDEN_DIR);
    api::client::commits::download_dir_hashes_db_to_path(remote_repo, commit_id, &repo_cache_dir)
        .await?;

    let local_objects_dir = repo_cache_dir.join(OBJECTS_DIR);
    let tmp_objects_dir =
        api::client::commits::download_objects_db_to_path(remote_repo, &repo_dir).await?;
    log::debug!(
        "trying to merge tmp_objects_dir {:?} into local objects dir {:?}",
        tmp_objects_dir,
        local_objects_dir
    );


    merge_objects_dbs(&local_objects_dir, &tmp_objects_dir)?;

    // Merge it in with the (probably not already extant) local objects db

    let object_reader = ObjectDBReader::new_from_path(repo_dir.clone(), commit_id)?;

    let commit_reader = CommitEntryReader::new_from_path(&repo_dir, commit_id, object_reader)?;
    let entries =
        commit_reader.list_directory(Path::new(&entry.resource.as_ref().unwrap().path))?;

    // Convert entries to [Entry]
    let entries: Vec<Entry> = entries.into_iter().map(Entry::from).collect();

    // Pull all the entries
    let pull_progress = PullProgress::new();
    puller::pull_entries_to_working_dir(remote_repo, &entries, local_path, &pull_progress).await?;

    Ok(())
}

fn merge_objects_dbs(repo_objects_dir: &Path, tmp_objects_dir: &Path) -> Result<(), OxenError> {

    let repo_dirs_dir = repo_objects_dir.join(OBJECT_DIRS_DIR);
    let repo_files_dir = repo_objects_dir.join(OBJECT_FILES_DIR);
    let repo_schemas_dir = repo_objects_dir.join(OBJECT_SCHEMAS_DIR);
    let repo_vnodes_dir = repo_objects_dir.join(OBJECT_VNODES_DIR);

    let new_dirs_dir = tmp_objects_dir.join(OBJECT_DIRS_DIR);
    let new_files_dir = tmp_objects_dir.join(OBJECT_FILES_DIR);
    let new_schemas_dir = tmp_objects_dir.join(OBJECT_SCHEMAS_DIR);
    let new_vnodes_dir = tmp_objects_dir.join(OBJECT_VNODES_DIR);

    log::debug!("opening tmp dirs");
    let opts = db::key_val::opts::default();
    let new_dirs_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, new_dirs_dir, false)?;
    let new_files_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, new_files_dir, false)?;
    let new_schemas_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, new_schemas_dir, false)?;
    let new_vnodes_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, new_vnodes_dir, false)?;

    // Create if missing for the local repo dirs - useful in case of remote download to cache dir without full repo

    log::debug!("opening repo dirs");
    let repo_dirs_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, repo_dirs_dir)?;
    let repo_files_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, repo_files_dir)?;
    let repo_schemas_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, repo_schemas_dir)?;
    let repo_vnodes_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, repo_vnodes_dir)?;

    //

    let new_dirs: Vec<TreeObject> = path_db::list_entries(&new_dirs_db)?;
    for dir in new_dirs {
        tree_db::put_tree_object(&repo_dirs_db, dir.hash(), &dir)?;
    }

    let new_files: Vec<TreeObject> = path_db::list_entries(&new_files_db)?;
    for file in new_files {
        tree_db::put_tree_object(&repo_files_db, file.hash(), &file)?;
    }

    let new_schemas: Vec<TreeObject> = path_db::list_entries(&new_schemas_db)?;
    for schema in new_schemas {
        tree_db::put_tree_object(&repo_schemas_db, schema.hash(), &schema)?;
    }

    let new_vnodes: Vec<TreeObject> = path_db::list_entries(&new_vnodes_db)?;
    for vnode in new_vnodes {
        tree_db::put_tree_object(&repo_vnodes_db, vnode.hash(), &vnode)?;
    }

    Ok(())
}

async fn pull_entries_to_working_dir(
    remote_repo: &RemoteRepository,
    entries: &[Entry],
    dst: impl AsRef<Path>,
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    let to_working_dir = true;
    pull_entries(remote_repo, entries, dst, to_working_dir, progress_bar).await?;
    Ok(())
}


pub async fn pull_entries(
    remote_repo: &RemoteRepository,
    entries: &[Entry],
    dst: impl AsRef<Path>,
    to_working_dir: bool,
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    log::debug!("{} entries.len() {}", current_function!(), entries.len());

    if entries.is_empty() {
        return Ok(());
    }

    let missing_entries = get_missing_entries(entries, &dst);
    // log::debug!("Pulling missing entries {:?}", missing_entries);

    if missing_entries.is_empty() {
        return Ok(());
    }

    // Some files may be much larger than others....so we can't just download them within a single body
    // Hence we chunk and send the big ones, and bundle and download the small ones

    // For files smaller than AVG_CHUNK_SIZE, we are going to group them, zip them up, and transfer them
    let smaller_entries: Vec<Entry> = missing_entries
        .iter()
        .filter(|e| e.num_bytes() < AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    // For files larger than AVG_CHUNK_SIZE, we are going break them into chunks and download the chunks in parallel
    let larger_entries: Vec<Entry> = missing_entries
        .iter()
        .filter(|e| e.num_bytes() > AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    // Either download to the working directory or the versions directory
    let (small_entry_paths, large_entry_paths) = {
        let small_entry_paths =
            working_dir_paths_from_small_entries(&smaller_entries, dst.as_ref());
        let large_entry_paths = working_dir_paths_from_large_entries(&larger_entries, dst.as_ref());
        (small_entry_paths, large_entry_paths)
    };

    let large_entries_sync = pull_large_entries(
        remote_repo,
        larger_entries,
        &dst,
        large_entry_paths,
        progress_bar,
    );

    let small_entries_sync = pull_small_entries(
        remote_repo,
        smaller_entries,
        &dst,
        small_entry_paths,
        progress_bar,
    );

    match tokio::join!(large_entries_sync, small_entries_sync) {
        (Ok(_), Ok(_)) => {
            log::debug!("Successfully synced entries!");
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

//
fn get_missing_entries(entries: &[Entry], dst: impl AsRef<Path>) -> Vec<Entry> {
    let dst = dst.as_ref();
    let mut missing_entries: Vec<Entry> = vec![];

    for entry in entries {
        let version_path = util::fs::version_path_from_dst_generic(dst, entry);
        if !version_path.exists() {
            missing_entries.push(entry.to_owned())
        }
    }

    missing_entries
}


async fn pull_large_entries(
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    dst: impl AsRef<Path>,
    download_paths: Vec<PathBuf>,
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }
    // Pull the large entries in parallel
    use tokio::time::{sleep, Duration};
    type PieceOfWork = (RemoteRepository, Entry, PathBuf, PathBuf);
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
    let tmp_dir = util::fs::oxen_hidden_dir(dst).join("tmp").join("pulled");
    log::debug!("Backing up pulls to tmp dir: {:?}", &tmp_dir);
    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let progress_bar = Arc::clone(progress_bar);
        tokio::spawn(async move {
            loop {
                let (remote_repo, entry, _dst, download_path) = queue.pop().await;

                log::debug!("worker[{}] processing task...", worker);

                // Chunk and individual files
                let remote_path = &entry.path();

                // Download to the tmp path, then copy over to the entries dir
                match api::client::entries::download_large_entry(
                    &remote_repo,
                    &remote_path,
                    &download_path,
                    &entry.commit_id(),
                    entry.num_bytes(),
                )
                .await
                {
                    Ok(_) => {
                        // log::debug!("Downloaded large entry {:?} to versions dir", remote_path);
                        progress_bar.add_bytes(entry.num_bytes());
                        progress_bar.add_files(1);
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

async fn pull_small_entries(
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    dst: impl AsRef<Path>,
    content_ids: Vec<(String, PathBuf)>,
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    if content_ids.is_empty() {
        return Ok(());
    }

    let total_size = repositories::entries::compute_generic_entries_size(&entries)?;

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
    type PieceOfWork = (RemoteRepository, Vec<(String, PathBuf)>, PathBuf);
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
            )
        })
        .collect();

    let worker_count = concurrency::num_threads_for_items(entries.len());
    let queue = Arc::new(TaskQueue::new(chunks.len()));
    let finished_queue = Arc::new(FinishedTaskQueue::new(entries.len()));
    for chunk in chunks {
        queue.try_push(chunk).unwrap();
        finished_queue.try_push(false).unwrap();
    }

    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let progress_bar = Arc::clone(progress_bar);
        tokio::spawn(async move {
            loop {
                let (remote_repo, chunk, path) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);

                match api::client::entries::download_data_from_version_paths(
                    &remote_repo,
                    &chunk,
                    &path,
                )
                .await
                {
                    Ok(download_size) => {
                        progress_bar.add_bytes(download_size);
                        progress_bar.add_files(chunk.len() as u64);
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


/// Returns a mapping from content_id -> entry.path
fn working_dir_paths_from_small_entries(entries: &[Entry], dst: &Path) -> Vec<(String, PathBuf)> {
    let mut content_ids: Vec<(String, PathBuf)> = vec![];

    for entry in entries.iter() {
        let version_path = util::fs::version_path_from_dst_generic(dst, entry);
        let version_path = util::fs::path_relative_to_dir(&version_path, dst).unwrap();

        content_ids.push((
            String::from(version_path.to_str().unwrap()).replace('\\', "/"),
            entry.path().to_owned(),
        ));
    }

    content_ids
}


fn working_dir_paths_from_large_entries(entries: &[Entry], dst: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = vec![];
    for entry in entries.iter() {
        let working_path = dst.join(entry.path());
        paths.push(working_path);
    }
    paths
}
