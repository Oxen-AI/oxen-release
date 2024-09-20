
pub async fn download_dir(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entry: &MetadataEntry,
    local_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // Download the commit db for the given commit id or branch
    // TODO: This might not be necessary for v19
    let commit_id = &entry.resource.as_ref().unwrap().commit.as_ref().unwrap().id;
    let home_dir = util::fs::oxen_tmp_dir()?;
    let repo_dir = home_dir
        .join(&remote_repo.namespace)
        .join(&remote_repo.name);
    let repo_cache_dir = repo_dir.join(OXEN_HIDDEN_DIR);
    api::client::commits::download_dir_hashes_db_to_path(remote_repo, commit_id, &repo_cache_dir)
        .await?;

    // Find dir node in remote repo
    let dir_hash = // TODO: How do we get the dir hash for download node?

    // TODO: Does download_node actually download all its children recursively?
    // Download dir node onto local machine 
    let dir_node = api::client::tree::download_node(repo, remote_repo, dir_hash)?;
    
    // Pull all the entries
    let pull_progress = PullProgress::new();

    // Create local directory to pull entries into 
    let directory = PathBuf::from(local_path);

    // Recursively pull entries
    r_download_entries(
        repo,
        &remote_repo,
        dir_node, 
        &directory,
        &pull_progress,
    )?;

    Ok(())
}

async fn r_download_entries(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node: &MerkleTreeNode,
    directory: &PathBuf,
    pull_progress: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    for child in &node.children {
        let mut new_directory = directory.clone();
        if let EMerkleTreeNode::Directory(dir_node) = &child.node {
            new_directory.push(&dir_node.name);
        }

        if child.has_children() {
            Box::pin(r_download_entries(
                repo,
                remote_repo,
                child,
                &new_directory,
                pull_progress,
            ))
            .await?;
        }
    }

    if let EMerkleTreeNode::VNode(_) = &node.node {
        let mut entries: Vec<Entry> = vec![];

        for child in &node.children {
            if let EMerkleTreeNode::File(file_node) = &child.node {

                entries.push(Entry::CommitEntry(CommitEntry {
                    commit_id: file_node.last_commit_id.to_string(),
                    path: directory.join(&file_node.name),
                    hash: child.hash.to_string(),
                    num_bytes: file_node.num_bytes,
                    last_modified_seconds: file_node.last_modified_seconds,
                    last_modified_nanoseconds: file_node.last_modified_nanoseconds,
                }));
            }
        }

        core::v0_10_0::index::puller::pull_entries_to_working_dir(
            remote_repo,
            &entries,
            &repo.path,
            pull_progress,
        )
        .await?;
    }

    Ok(())
}
