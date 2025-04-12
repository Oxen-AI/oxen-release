


use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{MerkleHash, RemoteRepository};
use crate::view::StatusMessage;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use http::header::CONTENT_LENGTH;
use rand::{thread_rng, Rng};
use tokio_util::codec::{BytesCodec, FramedRead};

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use std::time::Duration;
use std::io::SeekFrom;

use crate::util;

// Multipart upload strategy, based off of AWS S3 Multipart Upload
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
const CHUNK_SIZE: u64 = 10_485_760; // 10 MiB
const BASE_WAIT_TIME: usize = 300;
const MAX_WAIT_TIME: usize = 10_000;
const MAX_FILES: usize = 64;
const PARALLEL_FAILURES: usize = 63;
const MAX_RETRIES: usize = 5;


#[derive(Clone)]
pub enum MultipartLargeFileUploadStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Clone)]
pub struct MultipartLargeFileUpload {
    pub path: PathBuf, // Path to the file on the local filesystem
    pub dst_path: PathBuf, // Path to upload the file to on the server
    pub hash: MerkleHash, // Unique identifier for the file
    pub size: u64, // Size of the file in bytes
    pub status: MultipartLargeFileUploadStatus, // Status of the upload
    pub reason: Option<String>, // Reason for the upload failure
}


/// Check if a file exists in the remote repository by version id
pub async fn has_version(
    repository: &RemoteRepository,
    version_id: MerkleHash,
) -> Result<bool, OxenError> {
    let uri = format!("/versions/{version_id}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("api::client::versions::has_version {}", url);

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    if res.status() == 404 {
        return Ok(false);
    }

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("api::client::versions::has_version Got response {}", body);
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(_) => Ok(true),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::versions::has_version() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

/// Uploads a large file to the server in parallel and unpacks it in the versions directory
/// Returns the `MultipartLargeFileUpload` struct for the created upload
pub async fn multipart_large_file_upload(
    remote_repo: &RemoteRepository,
    file_path: impl AsRef<Path>,
) -> Result<MultipartLargeFileUpload, OxenError> {
    log::debug!("multipart_large_file_upload path: {:?}", file_path.as_ref());
    let mut upload = create_multipart_large_file_upload(file_path).await?;
    log::debug!("multipart_large_file_upload upload: {:?}", upload.hash);
    let results = upload_chunks(&remote_repo, &mut upload, CHUNK_SIZE, MAX_FILES, PARALLEL_FAILURES, MAX_RETRIES).await?;
    log::debug!("multipart_large_file_upload results length: {:?}", results.len());
    complete_multipart_large_file_upload(&remote_repo, upload, results).await
}

/// Creates a new multipart large file upload
/// Will reject the upload if the hash already exists on the server. 
/// The rejection helps prevent duplicate uploads or parallel uploads of the same file.
/// Returns the `MultipartLargeFileUpload` struct for the created upload
async fn create_multipart_large_file_upload(
    file_path: impl AsRef<Path>,
) -> Result<MultipartLargeFileUpload, OxenError> {
    let file_path = file_path.as_ref();

    // Figure out how many parts we need to upload
    let Ok(metadata) = file_path.metadata() else {
        return Err(OxenError::path_does_not_exist(file_path));
    };
    let file_size = metadata.len();
    let hash = MerkleHash::from_str(&util::hasher::hash_file_contents(file_path)?)?;
    
    Ok(MultipartLargeFileUpload {
        path: file_path.to_path_buf(),
        dst_path: PathBuf::new(),
        hash,
        size: file_size,
        status: MultipartLargeFileUploadStatus::Pending,
        reason: None,
    })
}

async fn complete_multipart_large_file_upload(
    remote_repo: &RemoteRepository,
    upload: MultipartLargeFileUpload,
    results: Vec<HashMap<String, String>>,
) -> Result<MultipartLargeFileUpload, OxenError> {
    
    let file_hash = &upload.hash.to_string();

    let uri = format!("/versions/{file_hash}/chunks");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("complete_multipart_large_file_upload {}", url);
    let client = client::new_for_url(&url)?;

    let body = serde_json::to_string(&results)?;
    let response = client.post(&url).body(body).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    log::debug!("complete_multipart_large_file_upload got body: {}", body);
    Ok(upload)
}

async fn upload_chunks(
    remote_repo: &RemoteRepository,
    upload: &mut MultipartLargeFileUpload,
    chunk_size: u64,
    max_files: usize,
    parallel_failures: usize,
    max_retries: usize,
) -> Result<Vec<HashMap<String, String>>, OxenError> {
    let file_path = &upload.path;
    let client = reqwest::Client::new();

    let mut handles = FuturesUnordered::new();
    let semaphore = Arc::new(Semaphore::new(max_files));
    let parallel_failures_semaphore = Arc::new(Semaphore::new(parallel_failures));

    // Figure out how many parts we need to upload
    let Ok(metadata) = file_path.metadata() else {
        return Err(OxenError::path_does_not_exist(file_path));
    };
    let file_size = metadata.len();
    let num_chunks = (file_size + chunk_size - 1) / chunk_size;

    for chunk_number in 0..num_chunks {
        let remote_repo = remote_repo.clone();
        let upload = upload.clone();
        let client = client.clone();

        let start = chunk_number * chunk_size;
        let semaphore = semaphore.clone();
        let parallel_failures_semaphore = parallel_failures_semaphore.clone();
        handles.push(tokio::spawn(async move {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|err| OxenError::basic_str(format!("Error acquiring semaphore: {err}")))?;
                    let mut chunk = upload_chunk(&client, &remote_repo, &upload, chunk_number, start, chunk_size).await;
                    let mut i = 0;
                    if parallel_failures > 0 {
                        while let Err(ul_err) = chunk {
                            if i >= max_retries {
                                return Err(OxenError::basic_str(format!(
                                    "Failed after too many retries ({max_retries}): {ul_err}"
                                )));
                            }

                            let parallel_failure_permit = parallel_failures_semaphore.clone().try_acquire_owned().map_err(|err| {
                                OxenError::basic_str(format!(
                                    "Failed too many failures in parallel ({parallel_failures}): {ul_err} ({err})"
                                ))
                            })?;

                            let wait_time = exponential_backoff(BASE_WAIT_TIME, i, MAX_WAIT_TIME);
                            sleep(Duration::from_millis(wait_time as u64)).await;

                            chunk = upload_chunk(&client, &remote_repo, &upload, chunk_number, start, chunk_size).await;
                            i += 1;
                            drop(parallel_failure_permit);
                        }
                    }
                    drop(permit);
                    chunk
                    .map_err(|e| OxenError::basic_str(format!("Upload error {e}")))
                    .map(|chunk| (chunk_number, chunk, chunk_size))
                }));
    }

    let mut results: Vec<HashMap<String, String>> = vec![HashMap::default(); num_chunks as usize];

    while let Some(result) = handles.next().await {
        match result {
            Ok(Ok((chunk_number, headers, size))) => {
                log::debug!("Uploaded part {chunk_number} with size {size}");
                results[chunk_number as usize] = headers;
            }
            Ok(Err(py_err)) => {
                return Err(py_err);
            }
            Err(err) => {
                return Err(OxenError::basic_str(format!(
                    "Error occurred while uploading: {err}"
                )));
            }
        }
    }

    Ok(results)
}

async fn upload_chunk(
    client: &reqwest::Client,
    remote_repo: &RemoteRepository,
    upload: &MultipartLargeFileUpload,
    chunk_number: u64,
    start: u64,
    chunk_size: u64,
) -> Result<HashMap<String, String>, OxenError> {
    let path = &upload.path;
    let mut options = OpenOptions::new();
    let mut file = options.read(true).open(path).await?;
    let file_size = file.metadata().await?.len();
    let bytes_transferred = std::cmp::min(file_size - start, chunk_size);

    file.seek(SeekFrom::Start(start)).await?;
    let chunk = file.take(chunk_size);

    let file_hash = &upload.hash.to_string();

    let uri = format!("/versions/{file_hash}/chunks/{chunk_number}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let response = client
        .put(url)
        .header(CONTENT_LENGTH, bytes_transferred)
        .body(reqwest::Body::wrap_stream(FramedRead::new(
            chunk,
            BytesCodec::new(),
        )))
        .send()
        .await?;
    let response = response.error_for_status()?;
    let mut headers = HashMap::new();
    for (name, value) in response.headers().into_iter() {
        headers.insert(name.to_string(), value.to_str().unwrap().to_owned());
    }
    Ok(headers)
}

pub fn exponential_backoff(base_wait_time: usize, n: usize, max: usize) -> usize {
    (base_wait_time + n.pow(2) + jitter()).min(max)
}

fn jitter() -> usize {
    thread_rng().gen_range(0..=500)
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::api;
    use crate::test;

    #[tokio::test]
    async fn test_upload_large_file_in_chunks() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = test::test_100k_parquet();

            let result = api::client::versions::multipart_large_file_upload(
                &remote_repo,
                path,
            )
            .await;
            assert!(result.is_ok());

            let has_version = api::client::versions::has_version(
                &remote_repo,
                result.unwrap().hash,
            )
            .await?;
            assert!(has_version);

            Ok(remote_repo)
        })
        .await
    }
}