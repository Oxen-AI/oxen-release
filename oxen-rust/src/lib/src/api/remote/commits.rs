use crate::api;
use crate::config::UserConfig;
use crate::constants::HISTORY_DIR;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, RemoteRepository};
use crate::util;
// use crate::util::ReadProgress;
use crate::view::{CommitParentsResponse, CommitResponse, IsValidStatusMessage};

use std::path::Path;
use std::str;
use std::time;

use async_compression::futures::bufread::GzipDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::TryStreamExt;
use indicatif::ProgressBar;
// use std::io::Cursor;
use async_tar::Archive;
use std::sync::Arc;

pub async fn get_by_id(
    repository: &RemoteRepository,
    commit_id: &str,
) -> Result<Option<Commit>, OxenError> {
    let config = UserConfig::default()?;
    let uri = format!("/commits/{}", commit_id);
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("remote::commits::get_by_id {}", url);

    let client = reqwest::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        if res.status() == 404 {
            return Ok(None);
        }

        let body = res.text().await?;
        log::debug!("api::remote::commits::get_by_id Got response {}", body);
        let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(j_res.commit)),
            Err(err) => Err(OxenError::basic_str(&format!(
                "get_commit_by_id() Could not serialize response [{}]\n{}",
                err, body
            ))),
        }
    } else {
        Err(OxenError::basic_str("get_commit_by_id() Request failed"))
    }
}

pub async fn commit_is_synced(
    remote_repo: &RemoteRepository,
    commit_id: &str,
    num_entries: usize,
) -> Result<bool, OxenError> {
    let config = UserConfig::default()?;
    let uri = format!("/commits/{}/is_synced?size={}", commit_id, num_entries);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("commit_is_synced checking URL: {}", url);
    let client = reqwest::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        let body = res.text().await?;
        log::debug!("commit_is_synced got response body: {}", body);
        let response: Result<IsValidStatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(j_res.is_valid),
            Err(err) => {
                log::debug!("Error getting remote commit {}", err);
                Ok(false)
            }
        }
    } else {
        Err(OxenError::basic_str("commit_is_synced() Request failed"))
    }
}

pub async fn download_commit_db_by_id(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: &str,
) -> Result<(), OxenError> {
    let config = UserConfig::default()?;
    let uri = format!("/commits/{}/commit_db", commit_id);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = reqwest::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        // Unpack tarball to our hidden dir
        let hidden_dir = util::fs::oxen_hidden_dir(&local_repo.path);

        let reader = res
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read();
        let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
        let archive = Archive::new(decoder);
        archive.unpack(hidden_dir).await?;

        Ok(())
    } else {
        Err(OxenError::basic_str(
            "download_commit_db_by_id() Request failed",
        ))
    }
}

pub async fn get_remote_parent(
    remote_repo: &RemoteRepository,
    commit_id: &str,
) -> Result<Vec<Commit>, OxenError> {
    let config = UserConfig::default()?;
    let uri = format!("/commits/{}/parents", commit_id);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = reqwest::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        let body = res.text().await?;
        let response: Result<CommitParentsResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(j_res.parents),
            Err(err) => Err(OxenError::basic_str(&format!(
                "get_remote_parent() Could not serialize response [{}]\n{}",
                err, body
            ))),
        }
    } else {
        Err(OxenError::basic_str("get_remote_parent() Request failed"))
    }
}

pub async fn post_commit_to_server(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit: &Commit,
) -> Result<CommitResponse, OxenError> {
    // First create commit on server
    create_commit_obj_on_server(remote_repo, commit).await?;

    // Then zip up and send the history db
    println!("Compressing commit {}", commit.id);

    // zip up the rocksdb in history dir, and post to server
    let commit_dir = util::fs::oxen_hidden_dir(&local_repo.path)
        .join(HISTORY_DIR)
        .join(commit.id.clone());
    // This will be the subdir within the tarball
    let tar_subdir = Path::new("history").join(commit.id.clone());

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    tar.append_dir_all(&tar_subdir, commit_dir)?;
    tar.finish()?;

    println!("Syncing commit {}", commit.id);
    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let pb = Arc::new(ProgressBar::new(buffer.len() as u64));
    let response = post_tarball_to_server(remote_repo, commit, buffer, &pb).await?;
    Ok(response)
}

async fn create_commit_obj_on_server(
    remote_repo: &RemoteRepository,
    commit: &Commit,
) -> Result<CommitResponse, OxenError> {
    let config = UserConfig::default()?;
    let client = reqwest::Client::new();

    let url = api::endpoint::url_from_repo(remote_repo, "/commits")?;

    let body = serde_json::to_string(&commit).unwrap();
    log::debug!("create_commit_obj_on_server {}", url);
    if let Ok(res) = client
        .post(url)
        .body(reqwest::Body::from(body))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        let status = res.status();
        let body = res.text().await?;
        log::debug!("create_commit_obj_on_server got response {}", body);
        let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(response),
            Err(_) => Err(OxenError::basic_str(&format!(
                "create_commit_obj_on_server Err serializing status_code[{}] \n\n{}",
                status, body
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "create_commit_obj_on_server error sending data from file",
        ))
    }
}

pub async fn post_tarball_to_server(
    remote_repo: &RemoteRepository,
    commit: &Commit,
    buffer: Vec<u8>,
    upload_progress: &Arc<ProgressBar>,
) -> Result<CommitResponse, OxenError> {
    // use tokio_util::codec::{BytesCodec, FramedRead};

    let config = UserConfig::default()?;

    let uri = format!("/commits/{}/data", commit.id);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    // println!("Uploading {}", ByteSize::b(buffer.len() as u64));
    // let cursor = Cursor::new(Vec::from(buffer));
    // let upload_source = ReadProgress {
    //     progress_bar: upload_progress.clone(),
    //     inner: cursor,
    // };

    // let stream = FramedRead::new(buffer, BytesCodec::new());
    // let stream = futures_util::stream::iter(buffer);
    let size = buffer.len() as u64;

    let client = reqwest::Client::builder()
        .timeout(time::Duration::from_secs(120))
        .build()?;

    if let Ok(res) = client
        .post(url)
        // .body(reqwest::Body::wrap_stream(stream))
        .body(buffer)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        let status = res.status();
        let body = res.text().await?;

        upload_progress.inc(size);

        log::debug!("post_tarball_to_server got response {}", body);
        let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(response),
            Err(_) => Err(OxenError::basic_str(&format!(
                "post_tarball_to_server Err serializing status_code[{}] \n\n{}",
                status, body
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "post_tarball_to_server error sending data from file",
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::index::CommitDirReader;
    use crate::test;

    use std::thread;

    #[tokio::test]
    async fn test_remote_commits_post_commit_to_server() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, remote_repo| async move {
            // Track the annotations dir
            // has format
            //   annotations/
            //     train/
            //       one_shot.txt
            //       annotations.txt
            //     test/
            //       annotations.txt
            let annotations_dir = local_repo.path.join("annotations");
            command::add(&local_repo, &annotations_dir)?;
            // Commit the directory
            let commit = command::commit(
                &local_repo,
                "Adding annotations data dir, which has two levels",
            )?
            .unwrap();

            // Post commit
            let result_commit =
                api::remote::commits::post_commit_to_server(&local_repo, &remote_repo, &commit)
                    .await?;
            assert_eq!(result_commit.commit.id, commit.id);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_commits_commit_is_valid() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|local_repo| async move {
            let mut local_repo = local_repo;
            let commit_history = command::log(&local_repo)?;
            let commit = commit_history.first().unwrap();

            // Set the proper remote
            let name = local_repo.dirname();
            let remote = test::repo_url_from(&name);
            command::add_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = command::create_remote(
                &local_repo,
                constants::DEFAULT_NAMESPACE,
                &local_repo.dirname(),
                test::TEST_HOST,
            )
            .await?;

            // Push it
            command::push(&local_repo).await?;

            let commit_entry_reader = CommitDirReader::new(&local_repo, commit)?;
            let num_entries = commit_entry_reader.num_entries()?;

            // We unzip in a background thread, so give it a second
            thread::sleep(std::time::Duration::from_secs(1));

            let is_synced =
                api::remote::commits::commit_is_synced(&remote_repo, &commit.id, num_entries)
                    .await?;
            assert!(is_synced);

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
            //       one_shot.txt
            //       annotations.txt
            //     test/
            //       annotations.txt
            let annotations_dir = local_repo.path.join("annotations");
            command::add(&local_repo, &annotations_dir)?;
            // Commit the directory
            let commit = command::commit(
                &local_repo,
                "Adding annotations data dir, which has two levels",
            )?
            .unwrap();

            // Post commit but not the actual files
            let result_commit =
                api::remote::commits::post_commit_to_server(&local_repo, &remote_repo, &commit)
                    .await?;
            assert_eq!(result_commit.commit.id, commit.id);
            let commit_entry_reader = CommitDirReader::new(&local_repo, &commit)?;
            let num_entries = commit_entry_reader.num_entries()?;

            // We unzip in a background thread, so give it a second
            thread::sleep(std::time::Duration::from_secs(1));

            // Should not be synced because we didn't actually post the files
            let is_synced =
                api::remote::commits::commit_is_synced(&remote_repo, &commit.id, num_entries)
                    .await?;
            assert!(!is_synced);

            Ok(remote_repo)
        })
        .await
    }
}
