use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::constants::HISTORY_DIR;
use crate::error::OxenError;
use crate::model::{Commit, CommitStats, LocalRepository, RemoteRepository};
use crate::util;
use crate::view::{CommitParentsResponse, CommitResponse, RemoteRepositoryHeadResponse};
use std::path::Path;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use tar::Archive;

pub fn get_stats(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<Option<CommitStats>, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/commits/{}/stats", commit.id);
    let repository = RemoteRepository::from_local(repository);
    let url = api::endpoint::url_from_repo(&repository, &uri);

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let body = res.text()?;
        let response: Result<RemoteRepositoryHeadResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(j_res.head),
            Err(err) => Err(OxenError::basic_str(&format!(
                "get_remote_head() Could not serialize response [{}]\n{}",
                err, body
            ))),
        }
    } else {
        Err(OxenError::basic_str("get_remote_head() Request failed"))
    }
}

pub fn get_by_id(
    repository: &LocalRepository,
    commit_id: &str,
) -> Result<Option<Commit>, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/commits/{}", commit_id);
    let repository = RemoteRepository::from_local(repository);
    let url = api::endpoint::url_from_repo(&repository, &uri);

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        if res.status() == 404 {
            return Ok(None);
        }

        let body = res.text()?;
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

pub fn download_commit_db_by_id(
    repository: &LocalRepository,
    commit_id: &str,
) -> Result<(), OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/commits/{}/commit_db", commit_id);
    let remote_repo = RemoteRepository::from_local(repository);
    let url = api::endpoint::url_from_repo(&remote_repo, &uri);

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        // Unpack tarball to our hidden dir
        let hidden_dir = util::fs::oxen_hidden_dir(&repository.path);
        let mut archive = Archive::new(GzDecoder::new(res));
        archive.unpack(hidden_dir)?;

        Ok(())
    } else {
        Err(OxenError::basic_str(
            "download_commit_db_by_id() Request failed",
        ))
    }
}

pub fn get_remote_parent(
    repository: &LocalRepository,
    commit_id: &str,
) -> Result<Vec<Commit>, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/commits/{}/parents", commit_id);
    let remote_repo = RemoteRepository::from_local(repository);
    let url = api::endpoint::url_from_repo(&remote_repo, &uri);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let body = res.text()?;
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

pub fn post_commit_to_server(
    repository: &LocalRepository,
    branch: &str,
    commit: &Commit,
) -> Result<CommitResponse, OxenError> {
    // First create commit on server
    create_commit_obj_on_server(repository, branch, commit)?;

    // Then zip up and send the history db
    println!("Compressing commit {}", commit.id);

    // zip up the rocksdb in history dir, and post to server
    let commit_dir = util::fs::oxen_hidden_dir(&repository.path)
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
    post_tarball_to_server(repository, commit, &buffer)
}

fn create_commit_obj_on_server(
    repository: &LocalRepository,
    branch_name: &str,
    commit: &Commit,
) -> Result<CommitResponse, OxenError> {
    let config = AuthConfig::default()?;
    let client = reqwest::blocking::Client::new();

    let uri = format!("/branches/{}/commits", branch_name);

    let remote_repo = RemoteRepository::from_local(repository);
    let url = api::endpoint::url_from_repo(&remote_repo, &uri);

    let body = serde_json::to_string(&commit).unwrap();
    log::debug!("create_commit_obj_on_server {}", url);
    if let Ok(res) = client
        .post(url)
        .body(reqwest::blocking::Body::from(body))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let status = res.status();
        let body = res.text()?;
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

fn post_tarball_to_server(
    repository: &LocalRepository,
    commit: &Commit,
    buffer: &[u8],
) -> Result<CommitResponse, OxenError> {
    let config = AuthConfig::default()?;
    let client = reqwest::blocking::Client::new();

    let uri = format!("/commits/{}", commit.id);
    let remote_repo = RemoteRepository::from_local(repository);
    let url = api::endpoint::url_from_repo(&remote_repo, &uri);

    log::debug!("post_tarball_to_server {}", url);
    if let Ok(res) = client
        .post(url)
        .body(reqwest::blocking::Body::from(buffer.to_owned()))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let status = res.status();
        let body = res.text()?;
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
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_get_empty_remote_head() -> Result<(), OxenError> {
        test::run_empty_sync_repo_test(|local_repo, _remote_repo| {
            let commit = command::head_commit(local_repo)?;
            let remote_head_result = api::remote::commits::get_stats(local_repo, &commit);
            assert!(remote_head_result.is_ok());
            Ok(())
        })
    }

    #[test]
    fn test_post_commit_to_server() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, _remote_repo| {
            // Track the annotations dir
            // has format
            //   annotations/
            //     train/
            //       one_shot.txt
            //       annotations.txt
            //     test/
            //       annotations.txt
            let annotations_dir = local_repo.path.join("annotations");
            command::add(local_repo, &annotations_dir)?;
            let branch = command::current_branch(local_repo)?.unwrap();
            // Commit the directory
            let commit = command::commit(
                local_repo,
                "Adding annotations data dir, which has two levels",
            )?
            .unwrap();

            // Post commit
            let result_commit =
                api::remote::commits::post_commit_to_server(local_repo, &branch.name, &commit)?;
            assert_eq!(result_commit.commit.id, commit.id);

            Ok(())
        })
    }
}
