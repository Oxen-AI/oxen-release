use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::api;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::core;
use crate::core::refs::RefWriter;
use crate::core::v0_10_0::index::versioner;
use crate::error::OxenError;
use crate::model::entries::commit_entry::Entry;
use crate::model::CommitEntry;
use crate::model::{
    LocalRepository, MerkleHash, MerkleTreeNodeType, RemoteBranch, RemoteRepository,
};
use crate::opts::PullOpts;
use crate::repositories;
use crate::util;
use crate::view::repository::RepositoryDataTypesView;

use crate::core::v0_19_0::index::merkle_tree::node::MerkleTreeNodeData;

pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let rb = RemoteBranch::default();
    pull_remote_branch(repo, &rb.remote, &rb.branch, false).await
}

pub async fn pull_shallow(repo: &LocalRepository) -> Result<(), OxenError> {
    todo!()
}

pub async fn pull_all(repo: &LocalRepository) -> Result<(), OxenError> {
    todo!()
}

/// Pull a specific remote and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch: impl AsRef<str>,
    all: bool,
) -> Result<(), OxenError> {
    let remote = remote.as_ref();
    let branch = branch.as_ref();
    println!("🐂 Oxen pull {} {}", remote, branch);

    let remote = repo
        .get_remote(&remote)
        .ok_or(OxenError::remote_not_set(&remote))?;

    let remote_data_view = match api::client::repositories::get_repo_data_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    println!(
        "{} ({}) contains {} files",
        remote_data_view.name,
        bytesize::ByteSize::b(remote_data_view.size),
        remote_data_view.total_files()
    );

    println!(
        "\n  {}\n",
        RepositoryDataTypesView::data_types_str(&remote_data_view.data_types)
    );

    let rb = RemoteBranch {
        remote: remote.to_string(),
        branch: branch.to_string(),
    };

    let remote_repo = RemoteRepository::from_data_view(&remote_data_view, &remote);
    pull_remote_repo(
        repo,
        &remote_repo,
        &rb,
        &PullOpts {
            should_pull_all: all,
            should_update_head: true,
        },
    )
    .await?;

    Ok(())
}

pub async fn pull_remote_repo(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    remote_branch: &RemoteBranch,
    opts: &PullOpts,
) -> Result<(), OxenError> {
    // Start the timer
    let start = std::time::Instant::now();

    // Find the head commit on the remote branch
    let Some(remote_branch) =
        api::client::branches::get_by_name(remote_repo, &remote_branch.branch).await?
    else {
        return Err(OxenError::remote_branch_not_found(&remote_branch.branch));
    };

    // Download the dir hashes
    // Must do this before downloading the commit node
    // because the commit node references the dir hashes
    let repo_hidden_dir = repo.path.join(OXEN_HIDDEN_DIR);
    api::client::commits::download_dir_hashes_db_to_path(
        remote_repo,
        &remote_branch.commit_id,
        &repo_hidden_dir,
    )
    .await?;

    // Download the latest commit node
    let hash = MerkleHash::from_str(&remote_branch.commit_id)?;
    let commit_node = api::client::tree::download_tree(repo, remote_repo, &hash).await?;

    // Download the commit history
    // Check what our HEAD commit is locally
    if let Some(head_commit) = repositories::commits::head_commit_maybe(&repo)? {
        // Download the commits between the head commit and the remote branch commit
        let base_commit_id = head_commit.id;
        let head_commit_id = &remote_branch.commit_id;
        api::client::tree::download_commits_between(
            repo,
            remote_repo,
            &base_commit_id,
            &head_commit_id,
        )
        .await?;
    } else {
        // Download the commits from the remote branch commit to the first commit
        api::client::tree::download_commits_from(repo, remote_repo, &remote_branch.commit_id)
            .await?;
    }

    // Keep track of how many bytes we have downloaded
    let byte_counter = Arc::new(AtomicU64::new(0));
    let file_counter = Arc::new(AtomicU64::new(0));
    let progress_bar = Arc::new(ProgressBar::new_spinner());
    progress_bar.set_style(ProgressStyle::default_spinner());
    progress_bar.enable_steady_tick(std::time::Duration::from_millis(100));

    // Recursively download the entries
    let directory = PathBuf::from("");
    r_download_entries(
        repo,
        remote_repo,
        &commit_node,
        &directory,
        &byte_counter,
        &file_counter,
        &progress_bar,
    )
    .await?;

    let ref_writer = RefWriter::new(&repo)?;
    if opts.should_update_head {
        // Make sure head is pointing to that branch
        ref_writer.set_head(&remote_branch.name);
    }
    ref_writer.set_branch_commit_id(&remote_branch.name, &remote_branch.commit_id)?;
    progress_bar.finish_and_clear();
    let duration = std::time::Duration::from_millis(start.elapsed().as_millis() as u64);

    println!(
        "🐂 Oxen downloaded {} ({} files) in {}",
        bytesize::ByteSize::b(byte_counter.load(Ordering::Relaxed)),
        file_counter.load(Ordering::Relaxed),
        humantime::format_duration(duration).to_string()
    );

    Ok(())
}

async fn r_download_entries(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node: &MerkleTreeNodeData,
    directory: &PathBuf,
    bytes_downloaded: &Arc<AtomicU64>,
    files_downloaded: &Arc<AtomicU64>,
    progress_bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    for child in &node.children {
        let mut new_directory = directory.clone();
        if node.dtype == MerkleTreeNodeType::Dir {
            let dir_node = node.dir()?;
            new_directory.push(dir_node.name);
        }

        if child.has_children() {
            Box::pin(r_download_entries(
                repo,
                remote_repo,
                &child,
                &new_directory,
                bytes_downloaded,
                files_downloaded,
                progress_bar,
            ))
            .await?;
        }
    }

    if node.dtype == MerkleTreeNodeType::VNode {
        // Figure out which entries need to be downloaded
        let mut missing_entries: Vec<Entry> = vec![];
        let missing_hashes = repositories::tree::list_missing_file_hashes(repo, &node.hash)?;

        for child in &node.children {
            if child.dtype == MerkleTreeNodeType::File {
                if !missing_hashes.contains(&child.hash) {
                    continue;
                }

                let file_node = child.file()?;
                missing_entries.push(Entry::CommitEntry(CommitEntry {
                    commit_id: file_node.last_commit_id.to_string(),
                    path: directory.join(file_node.name),
                    hash: child.hash.to_string(),
                    num_bytes: file_node.num_bytes,
                    last_modified_seconds: file_node.last_modified_seconds,
                    last_modified_nanoseconds: file_node.last_modified_nanoseconds,
                }));
            }
        }

        core::v0_10_0::index::puller::pull_entries_to_versions_dir(
            remote_repo,
            &missing_entries,
            &repo.path,
            bytes_downloaded,
            files_downloaded,
            progress_bar,
        )
        .await?;

        unpack_entries(
            repo,
            &missing_entries,
            bytes_downloaded,
            files_downloaded,
            progress_bar,
        )?;
    }

    Ok(())
}

fn unpack_entries(
    repo: &LocalRepository,
    entries: &[Entry],
    bytes_downloaded: &Arc<AtomicU64>,
    files_downloaded: &Arc<AtomicU64>,
    progress_bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    let repo = repo.clone();
    entries.par_iter().for_each(|entry| {
        let filepath = repo.path.join(entry.path());
        // log::debug!(
        //     "unpack_version_files_to_working_dir found filepath {:?}",
        //     filepath
        // );
        if versioner::should_unpack_entry(entry, &filepath) {
            // log::debug!(
            //     "unpack_version_files_to_working_dir unpack! {:?}",
            //     entry.path()
            // );
            let version_path = util::fs::version_path_for_entry(&repo, entry);
            match util::fs::copy_mkdir(version_path, &filepath) {
                Ok(_) => {
                    let total_bytes = bytes_downloaded.load(Ordering::Relaxed);
                    let total_files = files_downloaded.load(Ordering::Relaxed);
                    files_downloaded.fetch_add(1, Ordering::Relaxed);
                    progress_bar.set_message(format!(
                        "🐂 downloaded {} ({} files)",
                        bytesize::ByteSize::b(total_bytes),
                        total_files
                    ));
                }
                Err(err) => {
                    log::error!("pull_entries_for_commit unpack error: {}", err);
                }
            }
        } else {
            log::debug!(
                "unpack_version_files_to_working_dir do not unpack :( {:?}",
                entry.path()
            );
        }
    });

    Ok(())
}
