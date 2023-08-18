use crate::api::remote::client;
use crate::constants::{
    COMMITS_DIR, DEFAULT_PAGE_NUM, DIRS_DIR, FILES_DIR, HISTORY_DIR, SCHEMAS_DIR,
};
use crate::core::db;
use crate::core::index::pusher::UnsyncedCommitEntries;
use crate::core::index::{CommitDBReader, CommitWriter};
use crate::error::OxenError;
use crate::model::commit::CommitWithBranchName;
use crate::model::{Branch, Commit, LocalRepository, RemoteRepository};
use crate::opts::{PaginateOpts, PrintOpts};
use crate::util::hasher::hash_buffer;
use crate::view::commit::CommitSyncStatusResponse;
use crate::{api, constants};
use crate::{current_function, util};
// use crate::util::ReadProgress;
use crate::view::{
    CommitResponse, IsValidStatusMessage, ListCommitResponse, PaginatedCommits, StatusMessage,
};

use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;
use std::time;

use async_compression::futures::bufread::GzipDecoder;
use async_tar::Archive;
use bytesize::ByteSize;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::TryStreamExt;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use rocksdb::{DBWithThreadMode, MultiThreaded};

pub struct ChunkParams {
    pub chunk_num: usize,
    pub total_chunks: usize,
    pub total_size: usize,
}

pub async fn get_by_id(
    repository: &RemoteRepository,
    commit_id: &str,
) -> Result<Option<Commit>, OxenError> {
    let uri = format!("/commits/{commit_id}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("remote::commits::get_by_id {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        if res.status() == 404 {
            return Ok(None);
        }

        let body = client::parse_json_body(&url, res).await?;
        log::debug!("api::remote::commits::get_by_id Got response {}", body);
        let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(j_res.commit)),
            Err(err) => Err(OxenError::basic_str(format!(
                "get_commit_by_id() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str("get_commit_by_id() Request failed"))
    }
}

pub async fn list_commit_history(
    remote_repo: &RemoteRepository,
    revision: &str,
) -> Result<Vec<Commit>, OxenError> {
    let mut all_commits: Vec<Commit> = Vec::new();
    let mut page_num = DEFAULT_PAGE_NUM;
    let page_size = 100;

    println!("🐂 Getting commit history...");

    // Init bar then set length once we know it
    let bar = ProgressBar::new_spinner();
    bar.set_style(ProgressStyle::default_spinner());

    loop {
        let page_opts = PaginateOpts {
            page_num,
            page_size,
        };
        match list_commit_history_paginated(remote_repo, revision, &page_opts).await {
            Ok(paginated_commits) => {
                if page_num == DEFAULT_PAGE_NUM {
                    bar.set_length(paginated_commits.pagination.total_entries as u64);
                    bar.set_style(ProgressStyle::default_bar());
                }
                let n_commits = paginated_commits.commits.len();
                all_commits.extend(paginated_commits.commits);
                bar.inc(n_commits as u64);
                if page_num < paginated_commits.pagination.total_pages {
                    page_num += 1;
                } else {
                    break;
                }
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
    bar.finish();

    Ok(all_commits)
}

async fn list_commit_history_paginated(
    remote_repo: &RemoteRepository,
    revision: &str,
    page_opts: &PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    let page_num = page_opts.page_num;
    let page_size = page_opts.page_size;
    let uri = format!("/commits/{revision}/history?page={page_num}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<PaginatedCommits, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(j_res) => Ok(j_res),
                Err(err) => Err(OxenError::basic_str(format!(
                    "list_commit_history() Could not deserialize response [{err}]\n{body}"
                ))),
            }
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "list_commit_history() Request failed: {err}"
        ))),
    }
}

pub async fn commit_is_synced(
    remote_repo: &RemoteRepository,
    commit_id: &str,
) -> Result<Option<IsValidStatusMessage>, OxenError> {
    let uri = format!("/commits/{commit_id}/is_synced");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("commit_is_synced checking URL: {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        log::debug!("commit_is_synced Got response [{}]", res.status());
        if res.status() == 404 {
            return Ok(None);
        }

        let body = client::parse_json_body(&url, res).await?;
        log::debug!("commit_is_synced got response body: {}", body);
        let response: Result<IsValidStatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(j_res)),
            Err(err) => {
                log::debug!("Error getting remote commit {}", err);
                Err(OxenError::basic_str(
                    "commit_is_synced() unable to parse body",
                ))
            }
        }
    } else {
        Err(OxenError::basic_str("commit_is_synced() Request failed"))
    }
}

pub async fn latest_commit_synced(
    remote_repo: &RemoteRepository,
    commit_id: &str,
) -> Result<CommitSyncStatusResponse, OxenError> {
    let uri = format!("/commits/{commit_id}/latest_synced");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("latest_commit_synced checking URL: {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        log::debug!("latest_commit_synced Got response [{}]", res.status());
        if res.status() == 404 {
            return Err(OxenError::basic_str("No synced commits found"));
        }

        let body = client::parse_json_body(&url, res).await?;
        log::debug!("latest_commit_synced got response body: {}", body);
        // Sync status response
        let response: Result<CommitSyncStatusResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(result) => Ok(result),
            Err(err) => {
                log::debug!("Error getting remote commit {}", err);
                Err(OxenError::basic_str(
                    "latest_commit_synced() unable to parse body",
                ))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "latest_commit_synced() Request failed",
        ))
    }
}

/// Download the database of all the commits in a repository
pub async fn download_commits_db_to_repo(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
) -> Result<PathBuf, OxenError> {
    // Download to tmp path, then merge with existing commits db
    let tmp_path = util::fs::oxen_hidden_dir(&local_repo.path).join("tmp");
    let new_path = download_commits_db_to_path(remote_repo, tmp_path).await?;
    log::debug!(
        "download_commits_db_to_repo downloaded db to {:?}",
        new_path
    );

    // Merge with existing commits db
    let opts = db::opts::default();
    let new_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, &new_path, false)?;
    let new_commits = CommitDBReader::list_all(&new_db)?;
    log::debug!(
        "download_commits_db_to_repo got {} new commits",
        new_commits.len()
    );

    let writer = CommitWriter::new(local_repo)?;
    for commit in new_commits {
        if writer.get_commit_by_id(&commit.id)?.is_some() {
            continue;
        }

        log::debug!(
            "download_commits_db_to_repo Adding new commit to db {}",
            commit
        );
        writer.add_commit_to_db(&commit)?;
    }

    // Remove the tmp db
    util::fs::remove_dir_all(&new_path)?;

    Ok(writer.commits_db.path().to_path_buf())
}

pub async fn download_commits_db_to_path(
    remote_repo: &RemoteRepository,
    dst: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let uri = "/commits_db".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("{} downloading from {}", current_function!(), url);

    let client = client::new_for_url(&url)?;
    let res = client.get(url).send().await?;

    let dst = dst.as_ref();
    let reader = res
        .bytes_stream()
        .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
        .into_async_read();
    let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
    let archive = Archive::new(decoder);

    // On the server we pack up the data in a directory called "commits", so that is where it gets unpacked to
    let unpacked_path = dst.join(COMMITS_DIR);
    // If the directory already exists, remove it
    if unpacked_path.exists() {
        log::debug!(
            "{} removing existing {:?}",
            current_function!(),
            unpacked_path
        );
        util::fs::remove_dir_all(&unpacked_path)?;
    }

    log::debug!("{} writing to {:?}", current_function!(), dst);
    archive.unpack(dst).await?;

    Ok(unpacked_path)
}

/// Download the database of all the entries given a commit
pub async fn download_commit_entries_db_to_repo(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: &str,
) -> Result<PathBuf, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(&local_repo.path);
    download_commit_entries_db_to_path(remote_repo, commit_id, hidden_dir).await
}

pub async fn download_commit_entries_db_to_path(
    remote_repo: &RemoteRepository,
    commit_id: &str,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let uri = format!("/commits/{commit_id}/commit_db");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("{} downloading from {}", current_function!(), url);
    let client = client::new_for_url(&url)?;
    match client.get(url).send().await {
        Ok(res) => {
            let path = path.as_ref();
            let reader = res
                .bytes_stream()
                .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read();
            let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
            let archive = Archive::new(decoder);
            archive.unpack(path).await?;
            log::debug!("{} writing to {:?}", current_function!(), path);

            Ok(path.to_path_buf())
        }
        Err(err) => {
            let error = format!("Error fetching commit db: {}", err);
            Err(OxenError::basic_str(error))
        }
    }
}

pub async fn get_remote_parent(
    remote_repo: &RemoteRepository,
    commit_id: &str,
) -> Result<Vec<Commit>, OxenError> {
    let uri = format!("/commits/{commit_id}/parents");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<ListCommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(j_res.commits),
            Err(err) => Err(OxenError::basic_str(format!(
                "get_remote_parent() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str("get_remote_parent() Request failed"))
    }
}

pub async fn post_push_complete(
    remote_repo: &RemoteRepository,
    branch: &Branch,
    // we need to pass in the commit id because we might be pushing multiple commits from the same branch
    commit_id: impl AsRef<str>,
) -> Result<(), OxenError> {
    use serde_json::json;
    let commit_id = commit_id.as_ref();
    let uri = format!("/commits/{}/complete", commit_id);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("post_push_complete: {}", url);
    let body = serde_json::to_string(&json!({
        "branch": {
            "name": branch.name,
            "commit_id": commit_id,
        }
    }))
    .unwrap();

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).body(body).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(_) => Ok(()),
            Err(err) => Err(OxenError::basic_str(format!(
                "post_push_complete() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str("post_push_complete() Request failed"))
    }
}

// Commits in oldest-to-newest-order
pub async fn bulk_post_push_complete(
    remote_repo: &RemoteRepository,
    commits: &Vec<Commit>,
) -> Result<(), OxenError> {
    use serde_json::json;

    let uri = "/commits/complete".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("bulk_post_push_complete: {}", url);
    let body = serde_json::to_string(&json!(commits)).unwrap();

    log::debug!("Sending this body... {:?}", body);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).body(body).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(_) => Ok(()),
            Err(err) => Err(OxenError::basic_str(format!(
                "bulk_post_push_complete() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "bulk_post_push_complete() Request failed",
        ))
    }
}

pub async fn get_commits_with_unsynced_dbs(
    remote_repo: &RemoteRepository,
    branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    let revision = branch.commit_id.clone();

    let uri = format!("/commits/{revision}/db_status");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<ListCommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(commit_response) => Ok(commit_response.commits),
            Err(err) => Err(OxenError::basic_str(format!(
                "get_commits_with_unsynced_dbs() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "get_commits_with_unsynced_dbs() Request failed",
        ))
    }
}

pub async fn get_commits_with_unsynced_entries(
    remote_repo: &RemoteRepository,
    branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    let commit_id = branch.commit_id.clone();

    let uri = format!("/commits/{commit_id}/entries_status");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<ListCommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(commit_response) => Ok(commit_response.commits),
            Err(err) => Err(OxenError::basic_str(format!(
                "get_commits_with_unsynced_entries() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "get_commits_with_unsynced_entries() Request failed",
        ))
    }
}

pub async fn post_commits_to_server(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commits: &Vec<UnsyncedCommitEntries>,
    branch_name: String,
) -> Result<(), OxenError> {
    let mut commits_with_size: Vec<CommitWithBranchName> = Vec::new();
    for commit_with_entries in commits {
        let commit_history_dir = util::fs::oxen_hidden_dir(&local_repo.path)
            .join(HISTORY_DIR)
            .join(&commit_with_entries.commit.id);
        let entries_size = api::local::entries::compute_entries_size(&commit_with_entries.entries)?;
        let size = fs_extra::dir::get_size(commit_history_dir).unwrap() + entries_size;

        let commit_with_size = CommitWithBranchName::from_commit(
            &commit_with_entries.commit,
            size,
            branch_name.clone(),
        );

        commits_with_size.push(commit_with_size);
    }

    bulk_create_commit_obj_on_server(remote_repo, &commits_with_size).await?;
    Ok(())
}

pub async fn complete_commits(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commits: &Vec<UnsyncedCommitEntries>,
    branch_name: String,
) -> Result<(), OxenError> {
    let mut commits_with_size: Vec<CommitWithBranchName> = Vec::new();
    for commit_with_entries in commits {
        let commit_history_dir = util::fs::oxen_hidden_dir(&local_repo.path)
            .join(HISTORY_DIR)
            .join(&commit_with_entries.commit.id);
        let entries_size = api::local::entries::compute_entries_size(&commit_with_entries.entries)?;
        let size = fs_extra::dir::get_size(commit_history_dir).unwrap() + entries_size;

        let commit_with_size = CommitWithBranchName::from_commit(
            &commit_with_entries.commit,
            size,
            branch_name.clone(),
        );

        commits_with_size.push(commit_with_size);
    }

    bulk_create_commit_obj_on_server(remote_repo, &commits_with_size).await?;
    Ok(())
}

pub async fn post_commit_db_to_server(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    let commit_dir = util::fs::oxen_hidden_dir(&local_repo.path)
        .join(HISTORY_DIR)
        .join(commit.id.clone());

    log::debug!("Commit dir {:?}", commit_dir);

    // This will be the subdir within the tarball
    let tar_subdir = Path::new(HISTORY_DIR).join(commit.id.clone());

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    // Don't send any errantly downloaded local cache files (from old versions of oxen clone)
    let dirs_to_compress = vec![DIRS_DIR, FILES_DIR, SCHEMAS_DIR];

    log::debug!("setting up tar stuff");
    for dir in &dirs_to_compress {
        let full_path = commit_dir.join(dir);
        let tar_path = tar_subdir.join(dir);
        if full_path.exists() {
            tar.append_dir_all(&tar_path, full_path)?;
        }
    }

    tar.finish()?;

    log::debug!("finished with tar stuff");

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;

    // Quiet mode for progress bar depending on print_opts

    let is_compressed = true;
    let filename = None;
    log::debug!("About to send data to server");

    // Pass in silent bar - TODO: should post_data_to_server take an Option<ProgressBar> - sometimes silent sometimes not

    let quiet_bar = Arc::new(ProgressBar::hidden());

    post_data_to_server(
        remote_repo,
        commit,
        buffer,
        is_compressed,
        &filename,
        quiet_bar,
    )
    .await
}

pub async fn post_commit_to_server(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit: &Commit,
    unsynced_entries_size: u64,
    branch_name: String,
    print_opts: &PrintOpts,
) -> Result<(), OxenError> {
    // Compute the size of the commit
    let commit_history_dir = util::fs::oxen_hidden_dir(&local_repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id);
    let size = fs_extra::dir::get_size(commit_history_dir).unwrap() + unsynced_entries_size;

    // First create commit on server with size
    let commit_w_size = CommitWithBranchName::from_commit(commit, size, branch_name);
    create_commit_obj_on_server(remote_repo, &commit_w_size).await?;

    // Then zip up and send the history db
    if print_opts.verbose {
        println!("Compressing commit {}", commit.id);
    }

    // zip up the rocksdb in history dir, and post to server
    let commit_dir = util::fs::oxen_hidden_dir(&local_repo.path)
        .join(HISTORY_DIR)
        .join(commit.id.clone());

    log::debug!("Commit dir {:?}", commit_dir);

    // This will be the subdir within the tarball
    let tar_subdir = Path::new(HISTORY_DIR).join(commit.id.clone());

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    tar.append_dir_all(&tar_subdir, commit_dir)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    if print_opts.verbose {
        println!(
            "Syncing commit {} with size {}",
            commit.id,
            ByteSize::b(buffer.len() as u64)
        );
    }

    let bar = Arc::new(ProgressBar::new(buffer.len() as u64));

    // Quiet mode for progress bar depending on print_opts
    if !print_opts.verbose {
        bar.set_draw_target(ProgressDrawTarget::hidden())
    }

    let is_compressed = true;
    let filename = None;
    post_data_to_server(remote_repo, commit, buffer, is_compressed, &filename, bar).await
}

async fn create_commit_obj_on_server(
    remote_repo: &RemoteRepository,
    commit: &CommitWithBranchName,
) -> Result<CommitResponse, OxenError> {
    let url = api::endpoint::url_from_repo(remote_repo, "/commits")?;
    log::debug!("create_commit_obj_on_server {}\n{:?}", url, commit);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).json(commit).send().await {
        let body = client::parse_json_body(&url, res).await?;
        log::debug!("create_commit_obj_on_server got response {}", body);
        let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(response),
            Err(_) => Err(OxenError::basic_str(format!(
                "create_commit_obj_on_server Err deserializing \n\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "create_commit_obj_on_server error sending data from file",
        ))
    }
}

pub async fn bulk_create_commit_obj_on_server(
    remote_repo: &RemoteRepository,
    commits: &Vec<CommitWithBranchName>,
) -> Result<ListCommitResponse, OxenError> {
    let url = api::endpoint::url_from_repo(remote_repo, "/commits/bulk")?;
    log::debug!("bulk_create_commit_obj_on_server {}\n{:?}", url, commits);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).json(commits).send().await {
        let body = client::parse_json_body(&url, res).await?;
        log::debug!("bulk_create_commit_obj_on_server got response {}", body);
        let response: Result<ListCommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(response),
            Err(_) => Err(OxenError::basic_str(format!(
                "bulk_create_commit_obj_on_server Err deserializing \n\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "bulk_create_commit_obj_on_server error sending data from file",
        ))
    }
}

pub async fn post_data_to_server(
    remote_repo: &RemoteRepository,
    commit: &Commit,
    buffer: Vec<u8>,
    is_compressed: bool,
    filename: &Option<String>,
    bar: Arc<ProgressBar>,
) -> Result<(), OxenError> {
    let chunk_size: usize = constants::AVG_CHUNK_SIZE as usize;
    log::debug!("in post_data_to_server");
    if buffer.len() > chunk_size {
        log::debug!("about to upload data to server in chunks");
        upload_data_to_server_in_chunks(
            remote_repo,
            commit,
            &buffer,
            chunk_size,
            is_compressed,
            filename,
        )
        .await?;
    } else {
        log::debug!("about to upload data to server in single tarball");
        upload_single_tarball_to_server_with_retry(remote_repo, commit, &buffer, bar).await?;
    }
    log::debug!("made it out of post_data_to_server");
    Ok(())
}

pub async fn upload_single_tarball_to_server_with_retry(
    remote_repo: &RemoteRepository,
    commit: &Commit,
    buffer: &[u8],
    bar: Arc<ProgressBar>,
) -> Result<(), OxenError> {
    let mut total_tries = 0;
    log::debug!("server upload try {}", total_tries);

    while total_tries < constants::NUM_HTTP_RETRIES {
        match upload_single_tarball_to_server(remote_repo, commit, buffer, bar.to_owned()).await {
            Ok(_) => {
                log::debug!("successfully uploaded tarball");
                return Ok(());
            }
            Err(err) => {
                total_tries += 1;
                // Exponentially back off
                let sleep_time = total_tries * total_tries;
                log::debug!(
                    "upload_single_tarball_to_server_with_retry upload failed sleeping {}: {:?}",
                    sleep_time,
                    err
                );
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
    }

    Err(OxenError::basic_str("Upload retry failed."))
}

async fn upload_single_tarball_to_server(
    remote_repo: &RemoteRepository,
    commit: &Commit,
    buffer: &[u8],
    bar: Arc<ProgressBar>,
) -> Result<CommitResponse, OxenError> {
    let uri = format!("/commits/{}/data", commit.id);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::builder_for_url(&url)?
        .timeout(time::Duration::from_secs(120))
        .build()?;

    let size = buffer.len() as u64;
    log::debug!("About to try the post request");
    match client.post(&url).body(buffer.to_owned()).send().await {
        Ok(res) => {
            log::debug!("in happy path of post request before parsing body");
            let body = client::parse_json_body(&url, res).await?;

            log::debug!("upload_single_tarball_to_server got response {}", body);
            let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(response) => {
                    bar.inc(size);
                    Ok(response)
                }
                Err(_) => Err(OxenError::basic_str(format!(
                    "upload_single_tarball_to_server Err deserializing \n\n{body}"
                ))),
            }
        }
        Err(e) => {
            let err_str = format!("Err upload_single_tarball_to_server: {e:?}");
            Err(OxenError::basic_str(err_str))
        }
    }
}

async fn upload_data_to_server_in_chunks(
    remote_repo: &RemoteRepository,
    commit: &Commit,
    buffer: &[u8],
    chunk_size: usize,
    is_compressed: bool,
    filename: &Option<String>,
) -> Result<(), OxenError> {
    let total_size = buffer.len();
    log::debug!(
        "upload_data_to_server_in_chunks chunking data {} ...",
        total_size
    );
    let chunks: Vec<&[u8]> = buffer.chunks(chunk_size).collect();
    let hash = hash_buffer(buffer);
    log::debug!(
        "upload_data_to_server_in_chunks got {} chunks from {}",
        chunks.len(),
        ByteSize::b(total_size as u64)
    );

    for (i, chunk) in chunks.iter().enumerate() {
        log::debug!(
            "upload_data_to_server_in_chunks uploading chunk {} of size {}",
            i,
            ByteSize::b(chunks.len() as u64)
        );

        let params = ChunkParams {
            chunk_num: i,
            total_chunks: chunks.len(),
            total_size,
        };
        match upload_data_chunk_to_server_with_retry(
            remote_repo,
            commit,
            chunk,
            &hash,
            &params,
            is_compressed,
            filename,
        )
        .await
        {
            Ok(_) => {
                log::debug!("Success uploading chunk!")
            }
            Err(err) => {
                log::error!("Err uploading chunk: {}", err)
            }
        }
    }
    Ok(())
}

pub async fn upload_data_chunk_to_server_with_retry(
    remote_repo: &RemoteRepository,
    commit: &Commit,
    chunk: &[u8],
    hash: &str,
    params: &ChunkParams,
    is_compressed: bool,
    filename: &Option<String>,
) -> Result<(), OxenError> {
    let mut total_tries = 0;
    let mut last_error = String::from("");
    while total_tries < constants::NUM_HTTP_RETRIES {
        match upload_data_chunk_to_server(
            remote_repo,
            commit,
            chunk,
            hash,
            params,
            is_compressed,
            filename,
        )
        .await
        {
            Ok(_) => {
                return Ok(());
            }
            Err(err) => {
                total_tries += 1;
                // Exponentially back off
                let sleep_time = total_tries * total_tries;
                log::debug!(
                    "upload_data_chunk_to_server_with_retry upload failed sleeping {}: {:?}",
                    sleep_time,
                    err
                );
                last_error = format!("{:?}", err);
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
    }

    Err(OxenError::basic_str(format!(
        "Upload chunk retry failed. {}",
        last_error
    )))
}

async fn upload_data_chunk_to_server(
    remote_repo: &RemoteRepository,
    commit: &Commit,
    chunk: &[u8],
    hash: &str,
    params: &ChunkParams,
    is_compressed: bool,
    filename: &Option<String>,
) -> Result<CommitResponse, OxenError> {
    let maybe_filename = if !is_compressed {
        format!(
            "&filename={}",
            urlencoding::encode(
                filename
                    .as_ref()
                    .expect("Must provide filename if !compressed")
            )
        )
    } else {
        String::from("")
    };

    let uri = format!(
        "/commits/{}/upload_chunk?chunk_num={}&total_size={}&hash={}&total_chunks={}&is_compressed={}{}",
        commit.id, params.chunk_num, params.total_size, hash, params.total_chunks, is_compressed, maybe_filename
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let total_size = chunk.len() as u64;
    log::debug!(
        "upload_data_chunk_to_server posting {} to url {}",
        ByteSize::b(total_size),
        url
    );

    let client = client::builder_for_url(&url)?
        .timeout(time::Duration::from_secs(120))
        .build()?;

    match client.post(&url).body(chunk.to_owned()).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;

            log::debug!("upload_data_chunk_to_server got response {}", body);
            let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(_) => Err(OxenError::basic_str(format!(
                    "upload_data_chunk_to_server Err deserializing\n\n{body}"
                ))),
            }
        }
        Err(e) => {
            let err_str = format!("Err upload_data_chunk_to_server: {e:?}");
            Err(OxenError::basic_str(err_str))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::COMMITS_DIR;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core::db;
    use crate::core::index::pusher::UnsyncedCommitEntries;
    use crate::core::index::CommitDBReader;
    use crate::error::OxenError;
    use crate::model::CommitEntry;
    use crate::opts::PrintOpts;
    use crate::test;
    use rocksdb::{DBWithThreadMode, MultiThreaded};

    use std::thread;

    #[tokio::test]
    async fn test_remote_commits_post_commit_to_server() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, remote_repo| async move {
            // Track the annotations dir
            // has format
            //   annotations/
            //     train/
            //       one_shot.csv
            //       annotations.txt
            //     test/
            //       annotations.txt
            let annotations_dir = local_repo.path.join("annotations");
            command::add(&local_repo, &annotations_dir)?;
            // Commit the directory
            let commit = command::commit(
                &local_repo,
                "Adding annotations data dir, which has two levels",
            )?;
            let branch = api::local::branches::current_branch(&local_repo)?.unwrap();

            // Post commit
            let entries_size = 1000; // doesn't matter, since we aren't verifying size in tests
            let print_opts = PrintOpts { verbose: true };
            api::remote::commits::post_commit_to_server(
                &local_repo,
                &remote_repo,
                &commit,
                entries_size,
                branch.name.clone(),
                &print_opts,
            )
            .await?;

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_commits_post_commits_to_server() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, remote_repo| async move {
            // Track the annotations dir
            // has format
            //   annotations/
            //     train/
            //       one_shot.csv
            //       annotations.txt
            //     test/
            //       annotations.txt
            let train_dir = local_repo.path.join("annotations/train");
            command::add(&local_repo, &train_dir)?;
            // Commit the directory
            let commit1 = command::commit(&local_repo, "Adding 1")?;

            let test_dir = local_repo.path.join("annotations/test");
            command::add(&local_repo, &test_dir)?;
            // Commit the directory
            let commit2 = command::commit(&local_repo, "Adding 2")?;

            let branch = api::local::branches::current_branch(&local_repo)?.unwrap();

            // Post commit

            let unsynced_commits = vec![
                UnsyncedCommitEntries {
                    commit: commit1,
                    entries: Vec::<CommitEntry>::new(),
                },
                UnsyncedCommitEntries {
                    commit: commit2,
                    entries: Vec::<CommitEntry>::new(),
                },
            ];

            api::remote::commits::post_commits_to_server(
                &local_repo,
                &remote_repo,
                &unsynced_commits,
                branch.name.clone(),
            )
            .await?;

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_commits_commit_is_valid() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|local_repo| async move {
            let mut local_repo = local_repo;
            let commit_history = api::local::commits::list(&local_repo)?;
            let commit = commit_history.first().unwrap();

            // Set the proper remote
            let name = local_repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = api::remote::repositories::create(
                &local_repo,
                constants::DEFAULT_NAMESPACE,
                &local_repo.dirname(),
                test::test_host(),
            )
            .await?;

            // Push it
            command::push(&local_repo).await?;

            // We unzip in a background thread, so give it a second
            thread::sleep(std::time::Duration::from_secs(1));

            let is_synced = api::remote::commits::commit_is_synced(&remote_repo, &commit.id)
                .await?
                .unwrap();
            assert!(is_synced.is_valid);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_commits_is_not_valid() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, remote_repo| async move {
            // Track the annotations dir
            // has format
            //   annotations/
            //     train/
            //       one_shot.csv
            //       annotations.txt
            //     test/
            //       annotations.txt
            let annotations_dir = local_repo.path.join("annotations");
            command::add(&local_repo, &annotations_dir)?;
            // Commit the directory
            let commit = command::commit(
                &local_repo,
                "Adding annotations data dir, which has two levels",
            )?;
            let branch = api::local::branches::current_branch(&local_repo)?.unwrap();

            // Post commit but not the actual files
            let print_opts = PrintOpts { verbose: true };
            let entries_size = 1000; // doesn't matter, since we aren't verifying size in tests
            api::remote::commits::post_commit_to_server(
                &local_repo,
                &remote_repo,
                &commit,
                entries_size,
                branch.name.clone(),
                &print_opts,
            )
            .await?;

            // Should not be synced because we didn't actually post the files
            let is_synced =
                api::remote::commits::commit_is_synced(&remote_repo, &commit.id).await?;
            // We never kicked off the background processes
            assert!(is_synced.is_none());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_remote_commits_all() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|local_repo| async move {
            let mut local_repo = local_repo;
            let commit_history = api::local::commits::list(&local_repo)?;
            let num_local_commits = commit_history.len();

            // Set the proper remote
            let name = local_repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = api::remote::repositories::create(
                &local_repo,
                constants::DEFAULT_NAMESPACE,
                &local_repo.dirname(),
                test::test_host(),
            )
            .await?;

            // Push it
            command::push(&local_repo).await?;

            // We unzip in a background thread, so give it a second
            thread::sleep(std::time::Duration::from_secs(1));

            // List the remote commits
            let remote_commits =
                api::remote::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(remote_commits.len(), num_local_commits);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_remote_commits_base_head() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let local_repo = local_repo;
            // There should be >= 7 commits here
            let commit_history = api::local::commits::list(&local_repo)?;
            assert!(commit_history.len() >= 7);

            // Log comes out in reverse order, so we want the 5th commit as the base,
            // and will end up with the 2nd,3rd,4th commits (3 commits total)
            let head_commit = &commit_history[2];
            let base_commit = &commit_history[5];

            let revision = format!("{}..{}", base_commit.id, head_commit.id);

            // List the remote commits
            let remote_commits =
                api::remote::commits::list_commit_history(&remote_repo, &revision).await?;

            for commit in remote_commits.iter() {
                println!("got commit: {} -> {}", commit.id, commit.message);
            }

            assert_eq!(remote_commits.len(), 3);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_commits_db() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let local_commit_history = api::local::commits::list(&local_repo)?;
            let remote_clone = remote_repo.clone();

            test::run_empty_dir_test_async(|new_dir| async move {
                // Download the db
                let dst = api::remote::commits::download_commits_db_to_path(&remote_repo, &new_dir)
                    .await?;

                let db_dir = new_dir.join(COMMITS_DIR);
                assert_eq!(dst, db_dir);
                assert!(db_dir.exists());

                let opts = db::opts::default();
                let db: DBWithThreadMode<MultiThreaded> =
                    DBWithThreadMode::open_for_read_only(&opts, &db_dir, false)?;
                let commits = CommitDBReader::list_all(&db)?;

                assert_eq!(commits.len(), local_commit_history.len());

                // Then on clone
                // 1) add a --all flag
                // 2) first pull the commit db
                // 3) then add a progress bar if we are doing the full pull as we grab each commit entry db
                // 4) make sure to fully test --shallow vs regular (one revision) vs --all
                // 5) document the default, advantage, and differences of each approach.

                Ok(new_dir)
            })
            .await?;

            Ok(remote_clone)
        })
        .await
    }
}
