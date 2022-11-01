use liboxen::api;
use liboxen::command;
use liboxen::constants::HISTORY_DIR;
use liboxen::error::OxenError;
use liboxen::index::{CommitValidator, CommitWriter};
use liboxen::media::tabular;
use liboxen::media::DFOpts;
use liboxen::model::{Commit, LocalRepository};
use liboxen::util;
use liboxen::view::http::{MSG_RESOURCE_CREATED, MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{
    CommitParentsResponse, CommitResponse, IsValidStatusMessage, ListCommitResponse, StatusMessage,
};

use crate::app_data::OxenAppData;

use actix_web::{web, Error, HttpRequest, HttpResponse};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::stream::StreamExt as _;
use serde::Deserialize;
use std::path::Path;
use tar::Archive;

#[derive(Deserialize, Debug)]
pub struct SizeQuery {
    size: usize,
}

// List commits for a repository
pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");
    let repo_name: Option<&str> = req.match_info().get("repo_name");

    if let (Some(namespace), Some(repo_name)) = (namespace, repo_name) {
        let repo_dir = app_data.path.join(namespace).join(repo_name);
        match p_index(&repo_dir) {
            Ok(response) => HttpResponse::Ok().json(response),
            Err(err) => {
                log::error!("api err: {}", err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Could not find `name` param...";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

// List history for a branch or commit
pub async fn commit_history(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");
    let repo_name: Option<&str> = req.match_info().get("repo_name");
    let commit_or_branch: Option<&str> = req.match_info().get("commit_or_branch");

    if let (Some(namespace), Some(repo_name), Some(commit_or_branch)) =
        (namespace, repo_name, commit_or_branch)
    {
        let repo_dir = app_data.path.join(namespace).join(repo_name);
        match p_index_commit_or_branch_history(&repo_dir, commit_or_branch) {
            Ok(response) => HttpResponse::Ok().json(response),
            Err(err) => {
                let msg = format!("api err: {}", err);
                HttpResponse::NotFound().json(StatusMessage::error(&msg))
            }
        }
    } else {
        let msg = "Must supply `namespace`, `repo_name` and `commit_or_branch` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn show(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    let commit_id: Option<&str> = req.match_info().get("commit_id");
    if let (Some(namespace), Some(name), Some(commit_id)) = (namespace, name, commit_id) {
        match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(repository)) => match api::local::commits::get_by_id(&repository, commit_id) {
                Ok(Some(commit)) => HttpResponse::Ok().json(CommitResponse {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_FOUND),
                    commit,
                }),
                Ok(None) => {
                    log::debug!("commit_id {} does not exist for repo: {}", commit_id, name);
                    HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                }
                Err(err) => {
                    log::debug!("Err getting commit_id {}: {}", commit_id, err);
                    HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                }
            },
            Ok(None) => {
                log::debug!("404 could not get repo {}", name,);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::error!("Could not find repo [{}]: {}", name, err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Must supply `namespace`, `repo_name` and `commit_id` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn is_synced(req: HttpRequest, query: web::Query<SizeQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    let commit_or_branch: Option<&str> = req.match_info().get("commit_or_branch");
    let size = query.size;

    if let (Some(namespace), Some(name), Some(commit_or_branch)) =
        (namespace, name, commit_or_branch)
    {
        match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(repository)) => {
                match api::local::commits::get_by_id_or_branch(&repository, commit_or_branch) {
                    Ok(Some(commit)) => {
                        let mut is_valid = false;
                        let validator = CommitValidator::new(&repository);
                        if let Ok(result) = validator.has_all_data(&commit, size) {
                            is_valid = result;
                        }

                        HttpResponse::Ok().json(IsValidStatusMessage {
                            status: String::from(STATUS_SUCCESS),
                            status_message: String::from(MSG_RESOURCE_FOUND),
                            is_valid,
                        })
                    }
                    Ok(None) => {
                        log::debug!(
                            "commit or branch {} does not exist for repo: {}",
                            commit_or_branch,
                            name
                        );
                        HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                    }
                    Err(err) => {
                        log::debug!("Err getting commit or branch {}: {}", commit_or_branch, err);
                        HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                    }
                }
            }
            Ok(None) => {
                log::debug!("404 could not get repo {}", name,);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::error!("Could not find repo [{}]: {}", name, err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Must supply `namespace`, `repo_name` and `commit_or_branch` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn parents(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    let commit_or_branch: Option<&str> = req.match_info().get("commit_or_branch");

    if let (Some(namespace), Some(name), Some(commit_or_branch)) =
        (namespace, name, commit_or_branch)
    {
        match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(repository)) => match p_get_parents(&repository, commit_or_branch) {
                Ok(parents) => HttpResponse::Ok().json(CommitParentsResponse {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_FOUND),
                    parents,
                }),
                Err(err) => {
                    log::debug!(
                        "Error finding parent for commit {} in repo {}\nErr: {}",
                        commit_or_branch,
                        name,
                        err
                    );
                    HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                }
            },
            Ok(None) => {
                log::debug!("404 could not get repo {}", name,);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::debug!("Could not find repo [{}]: {}", name, err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Must supply `namespace`, `repo_name` and `commit_or_branch` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

fn p_get_parents(
    repository: &LocalRepository,
    commit_or_branch: &str,
) -> Result<Vec<Commit>, OxenError> {
    match api::local::commits::get_by_id_or_branch(repository, commit_or_branch)? {
        Some(commit) => api::local::commits::get_parents(repository, &commit),
        None => Ok(vec![]),
    }
}

fn p_index(repo_dir: &Path) -> Result<ListCommitResponse, OxenError> {
    let repo = LocalRepository::new(repo_dir)?;
    let commits = command::log(&repo)?;
    Ok(ListCommitResponse::success(commits))
}

fn p_index_commit_or_branch_history(
    repo_dir: &Path,
    commit_or_branch: &str,
) -> Result<ListCommitResponse, OxenError> {
    let repo = LocalRepository::new(repo_dir)?;
    let commits = command::log_commit_or_branch_history(&repo, commit_or_branch)?;
    log::debug!("controllers::commits: : {:#?}", commits);
    Ok(ListCommitResponse::success(commits))
}

pub async fn download_commit_db(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    let commit_or_branch: Option<&str> = req.match_info().get("commit_or_branch");

    if let (Some(namespace), Some(name), Some(commit_or_branch)) =
        (namespace, name, commit_or_branch)
    {
        match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(repository)) => {
                match api::local::commits::get_by_id_or_branch(&repository, commit_or_branch) {
                    Ok(Some(commit)) => match compress_commit(&repository, &commit) {
                        Ok(buffer) => HttpResponse::Ok().body(buffer),
                        Err(err) => {
                            log::error!("Error compressing commit: [{}] Err: {}", name, err);
                            HttpResponse::InternalServerError()
                                .json(StatusMessage::internal_server_error())
                        }
                    },
                    Ok(None) => {
                        log::debug!("Could not find commit [{}]", name);
                        HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                    }
                    Err(err) => {
                        log::error!("Error finding commit: [{}] Err: {}", name, err);
                        HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                    }
                }
            }
            Ok(None) => {
                log::debug!("404 could not get repo {}", name,);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::error!("Could not find repo [{}]: {}", name, err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Must supply `namespace`, `repo_name` and `commit_or_branch` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

fn compress_commit(repository: &LocalRepository, commit: &Commit) -> Result<Vec<u8>, OxenError> {
    // Tar and gzip the commit db directory
    // zip up the rocksdb in history dir, and post to server
    let commit_dir = util::fs::oxen_hidden_dir(&repository.path)
        .join(HISTORY_DIR)
        .join(commit.id.clone());
    // This will be the subdir within the tarball
    let tar_subdir = Path::new("history").join(commit.id.clone());

    log::debug!("Compressing commit {}", commit.id);
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    tar.append_dir_all(&tar_subdir, commit_dir)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    Ok(buffer)
}

pub async fn create(req: HttpRequest, body: String) -> HttpResponse {
    log::debug!("Got commit data: {}", body);

    let app_data = req.app_data::<OxenAppData>().unwrap();
    let data: Result<Commit, serde_json::Error> = serde_json::from_str(&body);
    log::debug!("Serialized commit data: {:?}", data);

    // name to the repo, should be in url path so okay to unwap
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();

    match (
        api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name),
        data,
    ) {
        (Ok(Some(repo)), Ok(commit)) => {
            // Create Commit from uri params
            match create_commit(&repo.path, &commit) {
                Ok(_) => HttpResponse::Ok().json(CommitResponse {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_CREATED),
                    commit: commit.to_owned(),
                }),
                Err(err) => {
                    log::error!("Err create_commit: {}", err);
                    HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
                }
            }
        }
        (repo_err, commit_err) => {
            log::error!(
                "Err api::local::repositories::get_by_name {:?} serialization err {:?}",
                repo_err,
                commit_err
            );
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

/// Controller to upload the commit database
pub async fn upload(
    req: HttpRequest,
    mut body: web::Payload, // the actual file body
) -> Result<HttpResponse, Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    // name to the repo, should be in url path so okay to unwap
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let commit_id: &str = req.match_info().get("commit_id").unwrap();

    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => {
            let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);

            match api::local::commits::get_by_id(&repo, commit_id) {
                Ok(Some(commit)) => {
                    let mut bytes = web::BytesMut::new();
                    while let Some(item) = body.next().await {
                        bytes.extend_from_slice(&item.unwrap());
                    }
                    log::debug!("Got compressed data {} bytes", bytes.len());

                    std::thread::spawn(move || {
                        // Get tar.gz bytes for history/COMMIT_ID data
                        log::debug!("Decompressing {} bytes to {:?}", bytes.len(), hidden_dir);
                        // Unpack tarball to our hidden dir
                        let mut archive = Archive::new(GzDecoder::new(&bytes[..]));

                        // Unpack and compute HASH and save next to the file to speed up computation later
                        match archive.entries() {
                            Ok(entries) => {
                                for file in entries {
                                    if let Ok(mut file) = file {
                                        // Why hash now? To make sure everything synced properly
                                        // When we want to check is_synced, it is expensive to rehash everything
                                        // But since upload is network bound already, hashing here makes sense, and we will just
                                        // load the HASH file later
                                        file.unpack_in(&hidden_dir).unwrap();
                                        let path = file.path().unwrap();
                                        let full_path = hidden_dir.join(&path);
                                        let hash_dir = full_path.parent().unwrap();
                                        let hash_file = hash_dir.join("HASH");
                                        if path.starts_with("versions/files/") {
                                            if util::fs::is_tabular(&path) {
                                                let df =
                                                    tabular::read_df(full_path, DFOpts::empty())
                                                        .unwrap();
                                                // let df = tabular::df_hash_rows(df).unwrap();
                                                let hash = util::hasher::compute_tabular_hash(&df);

                                                util::fs::write_to_path(&hash_file, &hash);
                                            } else {
                                                // log::debug!(
                                                //     "Compute hash for file {:?}",
                                                //     full_path
                                                // );
                                                let hash =
                                                    util::hasher::hash_file_contents(&full_path)
                                                        .unwrap();
                                                // log::debug!(
                                                //     "Computed hash [{hash}] for file {:?}",
                                                //     full_path
                                                // );

                                                util::fs::write_to_path(&hash_file, &hash);
                                            }
                                        }
                                    } else {
                                        log::error!("Could not unpack file in archive...");
                                    }
                                }
                            }
                            Err(err) => {
                                log::error!("Could not unpack entries from archive...");
                                log::error!("Err: {:?}", err);
                            }
                        }

                        log::debug!("Done decompressing.");
                    });
                    // handle.join().unwrap();

                    Ok(HttpResponse::Ok().json(CommitResponse {
                        status: String::from(STATUS_SUCCESS),
                        status_message: String::from(MSG_RESOURCE_CREATED),
                        commit: commit.to_owned(),
                    }))
                }
                Ok(None) => {
                    log::error!("Could not find commit [{}]", commit_id);
                    Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
                }
                Err(err) => {
                    log::error!("Error finding commit [{}]: {}", commit_id, err);
                    Ok(HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()))
                }
            }
        }
        Ok(None) => {
            log::debug!("404 could not get repo {}", repo_name,);
            Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
        }
        Err(repo_err) => {
            log::error!("Err get_by_name: {}", repo_err);
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

fn create_commit(repo_dir: &Path, commit: &Commit) -> Result<(), OxenError> {
    let repo = LocalRepository::from_dir(repo_dir)?;
    let result = CommitWriter::new(&repo);
    match result {
        Ok(commit_writer) => match commit_writer.add_commit_to_db(commit) {
            Ok(_) => {}
            Err(err) => {
                log::error!("Error adding commit to db: {:?}", err);
            }
        },
        Err(err) => {
            log::error!("Error creating commit writer: {:?}", err);
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {

    use actix_web::body::to_bytes;
    use actix_web::{web, App};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::path::Path;
    use std::thread;

    use liboxen::command;
    use liboxen::constants::OXEN_HIDDEN_DIR;
    use liboxen::error::OxenError;
    use liboxen::util;
    use liboxen::view::{CommitResponse, ListCommitResponse};

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test::{self, init_test_env};

    #[actix_web::test]
    async fn test_controllers_commits_index_empty() -> Result<(), OxenError> {
        init_test_env();
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, namespace, name)?;

        let uri = format!("/oxen/{}/{}/commits", namespace, name);
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::commits::index(req).await;

        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        println!("Got response: {}", text);
        let list: ListCommitResponse = serde_json::from_str(text)?;
        // Plus the initial commit
        assert_eq!(list.commits.len(), 1);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_commits_list_two_commits() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        command::add(&repo, path)?;
        command::commit(&repo, "first commit")?;
        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "world")?;
        command::add(&repo, path)?;
        command::commit(&repo, "second commit")?;

        let uri = format!("/oxen/{}/{}/commits", namespace, name);
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::commits::index(req).await;
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        // Plus the initial commit
        assert_eq!(list.commits.len(), 3);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_commits_list_commits_on_branch() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        command::add(&repo, path)?;
        command::commit(&repo, "first commit")?;

        let branch_name = "feature/list-commits";
        command::create_checkout_branch(&repo, branch_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "world")?;
        command::add(&repo, path)?;
        command::commit(&repo, "second commit")?;

        let uri = format!(
            "/oxen/{}/{}/commits/{}/history",
            namespace, repo_name, branch_name
        );
        let req = test::repo_request_with_param(
            &sync_dir,
            &uri,
            namespace,
            repo_name,
            "commit_or_branch",
            branch_name,
        );

        let resp = controllers::commits::commit_history(req).await;
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        // Plus the initial commit
        assert_eq!(list.commits.len(), 3);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    // Switch branches, add a commit, and only list commits from first branch
    #[actix_web::test]
    async fn test_controllers_commits_list_some_commits_on_branch() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let og_branch = command::current_branch(&repo)?.unwrap();

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        command::add(&repo, path)?;
        command::commit(&repo, "first commit")?;

        let branch_name = "feature/list-commits";
        command::create_checkout_branch(&repo, branch_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "world")?;
        command::add(&repo, path)?;
        command::commit(&repo, "second commit")?;

        // List commits from the first branch
        let uri = format!(
            "/oxen/{}/{}/commits/{}/history",
            namespace, repo_name, og_branch.name
        );
        let req = test::repo_request_with_param(
            &sync_dir,
            &uri,
            namespace,
            repo_name,
            "commit_or_branch",
            og_branch.name,
        );

        let resp = controllers::commits::commit_history(req).await;
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        // there should be 2 instead of the 3 total
        assert_eq!(list.commits.len(), 2);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_commits_upload() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello");
        command::add(&repo, &hello_file)?;
        let commit = command::commit(&repo, "First commit")?.unwrap();

        // create random tarball to post.. currently no validation that it is a valid commit dir
        let path_to_compress = format!("history/{}", commit.id);
        let commit_dir_name = format!("data/test/runs/{}", commit.id);
        let commit_dir = Path::new(&commit_dir_name);
        std::fs::create_dir_all(commit_dir)?;
        // Write a random file to it
        let zipped_filename = "blah.txt";
        let zipped_file_contents = "sup";
        let random_file = commit_dir.join(zipped_filename);
        util::fs::write_to_path(&random_file, zipped_file_contents);

        println!("Compressing commit {}...", commit.id);
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);

        tar.append_dir_all(&path_to_compress, &commit_dir)?;
        tar.finish()?;
        let payload: Vec<u8> = tar.into_inner()?.finish()?;

        let uri = format!("/oxen/{}/{}/commits/{}", namespace, repo_name, commit.id);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData {
                    path: sync_dir.clone(),
                })
                .route(
                    "/oxen/{namespace}/{repo_name}/commits/{commit_id}",
                    web::post().to(controllers::commits::upload),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .set_payload(payload)
            .to_request();

        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;

        // Make sure commit gets populated
        assert_eq!(resp.commit.id, commit.id);
        assert_eq!(resp.commit.message, commit.message);
        assert_eq!(resp.commit.author, commit.author);
        assert_eq!(resp.commit.parent_ids.len(), commit.parent_ids.len());

        // We unzip in a background thread, so give it a second
        thread::sleep(std::time::Duration::from_secs(1));

        // Make sure we unzipped the tar ball
        let uploaded_file = sync_dir
            .join(namespace)
            .join(repo_name)
            .join(OXEN_HIDDEN_DIR)
            .join(path_to_compress)
            .join(zipped_filename);
        println!("Looking for file: {:?}", uploaded_file);
        assert!(uploaded_file.exists());
        assert_eq!(
            util::fs::read_from_path(&uploaded_file)?,
            zipped_file_contents
        );

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;
        std::fs::remove_dir_all(commit_dir)?;

        Ok(())
    }
}
