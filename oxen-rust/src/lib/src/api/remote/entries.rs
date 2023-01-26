use crate::api::remote::client;
use crate::constants::AVG_CHUNK_SIZE;
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository, RemoteEntry, RemoteRepository};
use crate::util;
use crate::{api, constants};
// use crate::util::ReadProgress;
use crate::view::RemoteEntryResponse;

// use flate2::read::GzDecoder;
use async_compression::futures::bufread::GzipDecoder;
use async_std::prelude::*;
use async_tar::Archive;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::TryStreamExt;
use indicatif::ProgressBar;
use std::fs;
use std::io::prelude::*;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
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
    match client.post(&url).body(body).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("api::remote::entries::create {}", body);
            let response: Result<RemoteEntryResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(result) => Ok(result.entry),
                Err(_) => Err(OxenError::basic_str(format!(
                    "Error deserializing EntryResponse: \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::entries::create err: {err}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn download_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: &str,
    page: &usize,
    page_size: &usize,
) -> Result<(), OxenError> {
    let uri = format!("/commits/{commit_id}/download_entries?page={page}&page_size={page_size}");
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
            let err = format!("api::entries::download_entries Err request failed [{status}] {url}");
            Err(OxenError::basic_str(err))
        }
    } else {
        let err = format!("api::entries::download_entries Err request failed: {url}");
        Err(OxenError::basic_str(err))
    }
}

pub async fn download_large_entry(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entry: &CommitEntry,
    bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    // Read chunks
    let chunk_size = AVG_CHUNK_SIZE;
    let total_size = entry.num_bytes;
    let num_chunks = ((total_size / chunk_size) + 1) as usize;
    let mut total_read = 0;
    let mut chunk_size = chunk_size;

    // Write files to .oxen/tmp/HASH/chunk_0..N
    let hidden_dir = util::fs::oxen_hidden_dir(&local_repo.path);
    let tmp_dir = Path::new(&hidden_dir).join("tmp").join(&entry.hash);

    // TODO: We could probably upload chunks in parallel too
    for i in 0..num_chunks {
        // Make sure we read the last size correctly
        if (total_read + chunk_size) > total_size {
            chunk_size = total_size % chunk_size;
        }

        let filename = format!("chunk_{i}");
        let tmp_file = tmp_dir.join(filename);

        try_download_entry_chunk(remote_repo, entry, &tmp_file, total_read, chunk_size).await?;

        bar.inc(chunk_size);

        total_read += chunk_size;
    }

    // Once all downloaded, recombine file and delete temp dir
    let full_path = local_repo.path.join(&entry.path);
    log::debug!("Unpack to {:?}", full_path);
    if let Some(parent) = full_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    match std::fs::File::create(&full_path) {
        Ok(mut combined_file) => {
            for i in 0..num_chunks {
                let filename = format!("chunk_{i}");
                let tmp_file = tmp_dir.join(filename);

                log::debug!("Reading file bytes {:?}", tmp_file);
                match std::fs::File::open(&tmp_file) {
                    Ok(mut chunk_file) => {
                        let mut buffer: Vec<u8> = Vec::new();
                        chunk_file
                            .read_to_end(&mut buffer)
                            .expect("Could not read tmp file to end...");

                        match combined_file.write_all(&buffer) {
                            Ok(_) => {
                                log::debug!("Unpack successful! {:?}", full_path);
                                std::fs::remove_file(tmp_file)?;
                            }
                            Err(err) => {
                                log::error!("Could not write all data to disk {:?}", err);
                            }
                        }
                    }
                    Err(err) => {
                        let err = format!("Could not read chunk file {tmp_file:?}: {err}");
                        return Err(OxenError::basic_str(err));
                    }
                }
            }
        }
        Err(err) => {
            let err = format!("Could not write combined file {full_path:?}: {err}");
            return Err(OxenError::basic_str(err));
        }
    }

    // Copy to version path
    let version_path = util::fs::version_path(local_repo, entry);
    log::debug!("Copying to version path {:?}", version_path);
    if let Some(parent) = version_path.parent() {
        if !parent.exists() {
            log::debug!("Creating parent {:?}", parent);
            let err = format!("Could not create version dir path {parent:?}");
            std::fs::create_dir_all(parent).expect(&err);
        }
    }
    match std::fs::copy(&full_path, &version_path) {
        Ok(_) => {}
        Err(err) => {
            let err = format!("Could not copy file {full_path:?} to {version_path:?}: {err}");
            return Err(OxenError::basic_str(err));
        }
    }

    Ok(())
}

async fn try_download_entry_chunk(
    remote_repo: &RemoteRepository,
    entry: &CommitEntry,
    dest: &Path,
    chunk_start: u64,
    chunk_size: u64,
) -> Result<(), OxenError> {
    let mut try_num = 0;
    while try_num < constants::NUM_HTTP_RETRIES {
        match download_entry_chunk(remote_repo, entry, dest, chunk_start, chunk_size).await {
            Ok(_) => {
                log::debug!("Downloaded chunk {:?}", dest);
                return Ok(());
            }
            Err(err) => {
                log::error!("Error trying to download chunk: {}", err);
                try_num += 1;
                let sleep_time = try_num * try_num;
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
    }
    Err(OxenError::basic_str("Retry download chunk failed"))
}

/// Downloads a chunk of a file
async fn download_entry_chunk(
    remote_repo: &RemoteRepository,
    entry: &CommitEntry,
    dest: &Path,
    chunk_start: u64,
    chunk_size: u64,
) -> Result<(), OxenError> {
    log::debug!("download_entry_chunk entry {:?}", entry.path);

    let filename = entry.path.to_str().unwrap();
    let uri = format!(
        "/chunk/{}/{}?chunk_start={}&chunk_size={}",
        entry.commit_id, filename, chunk_start, chunk_size
    );

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("download_entry_chunk {}", url);

    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;

    if let Some(parent) = dest.parent() {
        if !parent.exists() {
            log::debug!("Create parent dir {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }

    let status = response.status();
    if reqwest::StatusCode::OK == status {
        // Copy to file
        let mut dest = { fs::File::create(dest)? };
        let mut content = Cursor::new(response.bytes().await?);
        std::io::copy(&mut content, &mut dest)?;
        Ok(())
    } else {
        let err = format!("Could not download entry status: {status}");
        Err(OxenError::basic_str(err))
    }
}

pub async fn download_data_from_version_paths(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    content_ids: &[String],
) -> Result<u64, OxenError> {
    let total_retries = constants::NUM_HTTP_RETRIES;
    let mut num_retries = 0;

    while num_retries < total_retries {
        match try_download_data_from_version_paths(local_repo, remote_repo, content_ids).await {
            Ok(val) => return Ok(val),
            Err(OxenError::Authentication(val)) => return Err(OxenError::Authentication(val)),
            Err(err) => {
                num_retries += 1;
                // Exponentially back off
                let sleep_time = num_retries * num_retries;
                log::warn!(
                    "Could not download content {:?} sleeping {}",
                    err,
                    sleep_time
                );
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
    }

    let err = format!(
        "Err: Failed to download {} files after {} retries",
        content_ids.len(),
        total_retries
    );
    Err(OxenError::basic_str(err))
}

pub async fn try_download_data_from_version_paths(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    content_ids: &[String],
) -> Result<u64, OxenError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for content_id in content_ids.iter() {
        let line = format!("{content_id}\n");
        encoder.write_all(line.as_bytes())?;
    }
    let body = encoder.finish()?;
    let url = api::endpoint::url_from_repo(remote_repo, "/versions")?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).body(body).send().await {
        if reqwest::StatusCode::UNAUTHORIZED == res.status() {
            let err = "Err: unauthorized request to download data".to_string();
            log::error!("{}", err);
            return Err(OxenError::authentication(err));
        }

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
        let err =
            format!("api::entries::download_data_from_version_paths Err request failed: {url}");
        Err(OxenError::basic_str(err))
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
    if reqwest::StatusCode::OK == status {
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
        let err = format!("Could not download entry status: {status}");
        return Err(OxenError::basic_str(err));
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
            println!("{result:?}");
            assert!(result.is_ok());

            Ok(remote_repo)
        })
        .await
    }
}
