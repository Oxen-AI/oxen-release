use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository, RemoteEntry, RemoteRepository};
use crate::util;
use crate::util::ReadProgress;
use crate::view::{PaginatedEntries, RemoteEntryResponse};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use std::fs;
use std::io::prelude::*;
use std::io::Cursor;
use std::sync::Arc;
use tar::Archive;

const DEFAULT_PAGE_SIZE: usize = 10;

pub fn create(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entry: &CommitEntry,
) -> Result<RemoteEntry, OxenError> {
    let config = AuthConfig::default()?;
    let fullpath = util::fs::version_path(local_repo, entry);
    log::debug!("Creating remote entry: {:?} -> {:?}", entry.path, fullpath);

    if !fullpath.exists() {
        return Err(OxenError::local_file_not_found(fullpath));
    }

    let file = fs::File::open(&fullpath)?;
    let client = reqwest::blocking::Client::new();
    let uri = format!("/entries?{}", entry.to_uri_encoded());
    let url = api::endpoint::url_from_repo(remote_repo, &uri);
    log::debug!("create entry: {}", url);
    match client
        .post(url)
        .body(file)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        Ok(res) => {
            let status = res.status();
            let body = res.text()?;
            log::debug!("api::remote::entries::create {}", body);
            let response: Result<RemoteEntryResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(result) => Ok(result.entry),
                Err(_) => Err(OxenError::basic_str(&format!(
                    "Error serializing EntryResponse: status_code[{}] \n\n{}",
                    status, body
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::entries::create err: {}", err);
            Err(OxenError::basic_str(&err))
        }
    }
}

pub fn first_page(
    repository: &RemoteRepository,
    commit_id: &str,
) -> Result<PaginatedEntries, OxenError> {
    let page_num = 1;
    list_page(repository, commit_id, page_num, DEFAULT_PAGE_SIZE)
}

pub fn nth_page(
    repository: &RemoteRepository,
    commit_id: &str,
    page_num: usize,
) -> Result<PaginatedEntries, OxenError> {
    list_page(repository, commit_id, page_num, DEFAULT_PAGE_SIZE)
}

pub fn list_page(
    remote_repo: &RemoteRepository,
    commit_id: &str,
    page_num: usize,
    page_size: usize,
) -> Result<PaginatedEntries, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!(
        "/commits/{}/entries?page_num={}&page_size={}",
        commit_id, page_num, page_size
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let status = res.status();
        let body = res.text()?;
        // log::debug!("list_page got body: {}", body);
        let response: Result<PaginatedEntries, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val),
            Err(_) => Err(OxenError::basic_str(&format!(
                "api::entries::list_page {} Err status_code[{}] \n\n{}",
                url, status, body
            ))),
        }
    } else {
        let err = format!("api::entries::list_page Err request failed: {}", url);
        Err(OxenError::basic_str(&err))
    }
}

pub fn download_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: &str,
    page_num: &usize,
    page_size: &usize,
) -> Result<(), OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!(
        "/commits/{}/download_entries?page_num={}&page_size={}",
        commit_id, page_num, page_size
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let status = res.status();
        if reqwest::StatusCode::OK == status {
            let mut archive = Archive::new(GzDecoder::new(res));
            archive.unpack(&local_repo.path)?;

            Ok(())
        } else {
            let err = format!(
                "api::entries::download_entries Err request failed [{}] {}",
                status, url
            );
            Err(OxenError::basic_str(&err))
        }
    } else {
        let err = format!("api::entries::download_entries Err request failed: {}", url);
        Err(OxenError::basic_str(&err))
    }
}

pub fn download_content_ids(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: &str,
    content_ids: &[String],
    download_progress: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/commits/{}/download_content_ids", commit_id);

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for content_id in content_ids.iter() {
        let line = format!("{}\n", content_id);
        encoder.write_all(line.as_bytes())?;
    }
    let body = encoder.finish()?;

    let url = api::endpoint::url_from_repo(remote_repo, &uri);

    let client = reqwest::blocking::Client::new()
        .post(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .body(body);

    let mut source = ReadProgress {
        progress_bar: download_progress.clone(),
        inner: client.send()?,
    };

    let mut buffer: Vec<u8> = vec![];
    std::io::copy(&mut source, &mut buffer)?;

    let cursor = Cursor::new(buffer);
    let mut archive = Archive::new(GzDecoder::new(cursor));

    archive.unpack(&local_repo.path)?;

    Ok(())
}

/// Returns true if we downloaded the entry, and false if it already exists
pub fn download_entry(
    repository: &LocalRepository,
    entry: &CommitEntry,
) -> Result<bool, OxenError> {
    let remote = repository.remote().ok_or_else(OxenError::remote_not_set)?;
    let config = AuthConfig::default()?;
    let fpath = repository.path.join(&entry.path);
    log::debug!("download_remote_entry entry {:?}", entry.path);

    let filename = entry.path.to_str().unwrap();
    let url = format!(
        "{}/commits/{}/entries/{}",
        remote.url, entry.commit_id, filename
    );
    log::debug!("download_entry {}", url);

    let client = reqwest::blocking::Client::new();
    let mut response = client
        .get(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()?;

    if let Some(parent) = fpath.parent() {
        if !parent.exists() {
            log::debug!("Create parent dir {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }

    let status = response.status();
    if 200 == status {
        // Copy to working dir
        let mut dest = { fs::File::create(&fpath)? };
        response.copy_to(&mut dest)?;

        // Copy to versions dir
        let version_path = util::fs::version_path(repository, entry);

        if let Some(parent) = version_path.parent() {
            if !parent.exists() {
                log::debug!("Create version parent dir {:?}", parent);
                std::fs::create_dir_all(parent)?;
            }
        }

        std::fs::copy(fpath, version_path)?;
    } else {
        let err = format!("Could not download entry status: {}", status);
        return Err(OxenError::basic_str(&err));
    }

    Ok(true)
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::index::CommitEntryReader;
    use crate::test;
    use crate::util;

    #[test]
    fn test_create_entry() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, remote_repo| {
            // Track an image
            let image_file = local_repo.path.join("train").join("dog_1.jpg");
            command::add(local_repo, &image_file)?;
            // Commit the directory
            let commit = command::commit(local_repo, "Adding image")?.unwrap();

            let committer = CommitEntryReader::new(local_repo, &commit)?;
            let entries = committer.list_entries()?;
            assert!(!entries.is_empty());

            let entry = entries.last().unwrap();
            let result = api::remote::entries::create(local_repo, remote_repo, entry);
            println!("{:?}", result);
            assert!(result.is_ok());

            Ok(())
        })
    }

    #[test]
    fn test_list_entries_all_in_one_page() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, remote_repo| {
            // Make mutable copy so we can set the remote
            let mut repo = local_repo.clone();

            // Track train directory
            let train_dir = repo.path.join("train");
            let num_files = util::fs::rcount_files_in_dir(&train_dir);
            command::add(&repo, &train_dir)?;
            // Commit the directory
            let commit = command::commit(&repo, "Adding image")?.unwrap();

            // Set the proper remote
            let remote = test::repo_url_from(&repo.name);
            command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push everything
            command::push(&repo)?;

            let entries = api::remote::entries::list_page(remote_repo, &commit.id, 1, num_files)?;
            assert_eq!(entries.total_entries, num_files);
            assert_eq!(entries.entries.len(), num_files);

            Ok(())
        })
    }

    #[test]
    fn test_list_entries_first_page_of_two() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, remote_repo| {
            // Make mutable copy so we can set the remote
            let mut repo = local_repo.clone();

            // Track train directory
            let train_dir = repo.path.join("train");
            let num_files = util::fs::rcount_files_in_dir(&train_dir);
            command::add(&repo, &train_dir)?;
            // Commit the directory
            let commit = command::commit(&repo, "Adding image")?.unwrap();

            // Set the proper remote
            let remote = test::repo_url_from(&repo.name);
            command::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push
            command::push(&repo)?;

            let page_size = 3;
            let entries = api::remote::entries::list_page(remote_repo, &commit.id, 1, page_size)?;
            assert_eq!(entries.total_entries, num_files);
            assert_eq!(entries.page_size, page_size);
            assert_eq!(entries.total_pages, 2);
            assert_eq!(entries.entries.len(), page_size);

            Ok(())
        })
    }
}
