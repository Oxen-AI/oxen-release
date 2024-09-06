use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

use crate::api;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::core;
use crate::core::refs::RefWriter;
use crate::core::v0_10_0::index::versioner;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::{Commit, CommitEntry};
use crate::model::{LocalRepository, MerkleHash, RemoteBranch, RemoteRepository};
use crate::opts::PullOpts;
use crate::repositories;
use crate::util;

use crate::core::v0_19_0::index::commit_merkle_tree::CommitMerkleTree;
use crate::core::v0_19_0::structs::pull_progress::PullProgress;
use crate::model::merkle_tree::node::MerkleTreeNode;

use std::str::FromStr;

use crate::model::merkle_tree::node::EMerkleTreeNode;

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
    println!("🐂 oxen pull {} {}", remote, branch);

    let remote = repo
        .get_remote(remote)
        .ok_or(OxenError::remote_not_set(remote))?;

    let remote_data_view = match api::client::repositories::get_repo_data_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

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
    if let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? {
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
    let pull_progress = PullProgress::new();

    // Recursively download the entries
    let directory = PathBuf::from("");
    r_download_entries(repo, remote_repo, &commit_node, &directory, &pull_progress).await?;

    let ref_writer = RefWriter::new(repo)?;
    if opts.should_update_head {
        // Make sure head is pointing to that branch
        ref_writer.set_head(&remote_branch.name);
    }
    ref_writer.set_branch_commit_id(&remote_branch.name, &remote_branch.commit_id)?;
    pull_progress.finish();
    let duration = std::time::Duration::from_millis(start.elapsed().as_millis() as u64);

    println!(
        "🐂 oxen pulled {} ({} files) in {}",
        bytesize::ByteSize::b(pull_progress.get_num_bytes()),
        pull_progress.get_num_files(),
        humantime::format_duration(duration)
    );

    Ok(())
}

pub async fn maybe_pull_missing_entries(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    // If we don't have a remote, there are no missing entries, so return
    let rb = RemoteBranch::default();
    let remote = repo.get_remote(&rb.remote);
    let Some(remote) = remote else {
        log::debug!("No remote, no missing entries to fetch");
        return Ok(());
    };

    let commit_merkle_tree = CommitMerkleTree::from_commit(repo, commit)?;

    let remote_repo = match api::client::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    // TODO: what should we print here? If there is nothing to pull, we
    // shouldn't show the PullProgress

    // Keep track of how many bytes we have downloaded
    let pull_progress = PullProgress::new();

    // Recursively download the entries
    let directory = PathBuf::from("");
    r_download_entries(
        repo,
        &remote_repo,
        &commit_merkle_tree.root,
        &directory,
        &pull_progress,
    )
    .await?;

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
        // Figure out which entries need to be downloaded
        let mut missing_entries: Vec<Entry> = vec![];
        let missing_hashes = repositories::tree::list_missing_file_hashes(repo, &node.hash)?;

        for child in &node.children {
            if let EMerkleTreeNode::File(file_node) = &child.node {
                if !missing_hashes.contains(&child.hash) {
                    continue;
                }

                missing_entries.push(Entry::CommitEntry(CommitEntry {
                    commit_id: file_node.last_commit_id.to_string(),
                    path: directory.join(&file_node.name),
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
            pull_progress,
        )
        .await?;

        unpack_entries(repo, &missing_entries)?;
    }

    Ok(())
}

fn unpack_entries(repo: &LocalRepository, entries: &[Entry]) -> Result<(), OxenError> {
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
                Ok(_) => {}
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
