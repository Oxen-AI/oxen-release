use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository, RemoteEntry, RemoteRepository};
use crate::util;
// use crate::util::ReadProgress;
use crate::view::RemoteEntryResponse;

// use flate2::read::GzDecoder;
use async_compression::futures::bufread::GzipDecoder;
use async_std::prelude::*;
use async_tar::Archive;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::TryStreamExt;
use std::fs;
use std::io::prelude::*;
use std::io::Cursor;
use tokio_util::codec::{BytesCodec, FramedRead};

pub async fn create(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entry: &CommitEntry,
) -> Result<RemoteEntry, OxenError> {
    let fullpath = util::fs::version_path(local_repo, entry);
    log::debug!("Creating remote entry: {:?} -> {:?}", entry.path, fullpath);

    if !fullpath.exists() {
        return Err(OxenError::file_does_not_exist(fullpath));
    }

    let file = tokio::fs::File::open(&fullpath).await?;
    let stream = FramedRead::new(file, BytesCodec::new());
    let body = reqwest::Body::wrap_stream(stream);

    let uri = format!("/entries?{}", entry.to_uri_encoded());
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("create entry: {}", url);
    let client = client::new_for_url(&url)?;
    match client.post(url).body(body).send().await {
        Ok(res) => {
            let status = res.status();
            let body = res.text().await?;
            log::debug!("api::remote::entries::create {}", body);
            let response: Result<RemoteEntryResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(result) => Ok(result.entry),
                Err(_) => Err(OxenError::basic_str(&format!(
                    "Error deserializing EntryResponse: status_code[{}] \n\n{}",
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

pub async fn download_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: &str,
    page_num: &usize,
    page_size: &usize,
) -> Result<(), OxenError> {
    let uri = format!(
        "/commits/{}/download_entries?page_num={}&page_size={}",
        commit_id, page_num, page_size
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let status = res.status();
        if reqwest::StatusCode::OK == status {
            let reader = res
                .bytes_stream()
                .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read();
            let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
            let archive = Archive::new(decoder);
            archive.unpack(&local_repo.path).await?;

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

pub async fn download_content_by_ids(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    content_ids: &[String],
) -> Result<u64, OxenError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for content_id in content_ids.iter() {
        let line = format!("{}\n", content_id);
        encoder.write_all(line.as_bytes())?;
    }
    let body = encoder.finish()?;
    let url = api::endpoint::url_from_repo(remote_repo, "/versions")?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).body(body).send().await {
        let reader = res
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read();
        let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
        let archive = Archive::new(decoder);

        let mut size: u64 = 0;
        // For debug if you want to see each file we are unpacking...
        let mut entries = archive.entries()?;
        while let Some(file) = entries.next().await {
            let mut file = file?;
            let path = file.path()?.to_path_buf();

            let fullpath = local_repo.path.join(&path);
            if let Some(parent) = fullpath.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }
            }

            // log::debug!("Unpacking into path {:?}", path);
            file.unpack_in(&local_repo.path).await?;

            let metadata = std::fs::metadata(&fullpath)?;
            size += metadata.len();
            log::debug!("Unpacking {} bytes {:?}", metadata.len(), path);
        }

        Ok(size)
    } else {
        let err = format!(
            "api::entries::download_content_by_ids Err request failed: {}",
            url
        );
        Err(OxenError::basic_str(&err))
    }
}

/// Returns true if we downloaded the entry, and false if it already exists
pub async fn download_entry(
    repository: &LocalRepository,
    entry: &CommitEntry,
) -> Result<bool, OxenError> {
    let remote = repository.remote().ok_or_else(OxenError::remote_not_set)?;
    let fpath = repository.path.join(&entry.path);
    log::debug!("download_remote_entry entry {:?}", entry.path);

    let filename = entry.path.to_str().unwrap();
    let url = format!(
        "{}/commits/{}/entries/{}",
        remote.url, entry.commit_id, filename
    );
    log::debug!("download_entry {}", url);

    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;

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
        let mut content = Cursor::new(response.bytes().await?);
        std::io::copy(&mut content, &mut dest)?;

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
    // use crate::constants;
    use crate::error::OxenError;
    use crate::index::CommitDirReader;
    use crate::test;
    // use crate::util;

    #[tokio::test]
    async fn test_create_entry() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, remote_repo| async move {
            // Track an image
            let image_file = local_repo.path.join("train").join("dog_1.jpg");
            command::add(&local_repo, &image_file)?;
            // Commit the directory
            let commit = command::commit(&local_repo, "Adding image")?.unwrap();

            let committer = CommitDirReader::new(&local_repo, &commit)?;
            let entries = committer.list_entries()?;
            assert!(!entries.is_empty());

            let entry = entries.last().unwrap();
            let result = api::remote::entries::create(&local_repo, &remote_repo, entry).await;
            println!("{:?}", result);
            assert!(result.is_ok());

            Ok(remote_repo)
        })
        .await
    }
}
