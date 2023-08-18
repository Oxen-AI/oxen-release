use liboxen::api;
use liboxen::constants;
use liboxen::constants::COMMITS_DIR;
use liboxen::constants::DIRS_DIR;
use liboxen::constants::FILES_DIR;
use liboxen::constants::HASH_FILE;
use liboxen::constants::HISTORY_DIR;
use liboxen::constants::SCHEMAS_DIR;
use liboxen::core::cache::cacher_status::CacherStatusType;
use liboxen::core::cache::cachers::content_validator;
use liboxen::core::cache::commit_cacher;
use liboxen::core::index::CommitReader;
use liboxen::core::index::CommitWriter;

use liboxen::core::index::RefWriter;
use liboxen::error::OxenError;
use liboxen::model::commit::CommitWithBranchName;
use liboxen::model::RepositoryNew;
use liboxen::model::{Commit, LocalRepository};
use liboxen::util;
use liboxen::view::branch::BranchName;
use liboxen::view::commit::CommitSyncStatusResponse;
use liboxen::view::http::MSG_CONTENT_IS_INVALID;
use liboxen::view::http::MSG_FAILED_PROCESS;
use liboxen::view::http::MSG_INTERNAL_SERVER_ERROR;
use liboxen::view::http::MSG_RESOURCE_IS_PROCESSING;
use liboxen::view::http::STATUS_ERROR;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::PaginatedCommits;
use liboxen::view::{CommitResponse, IsValidStatusMessage, ListCommitResponse, StatusMessage};

use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::PageNumQuery;
use crate::params::{app_data, path_param};
use crate::tasks::post_push_complete::PostPushComplete;

use actix_web::{web, Error, HttpRequest, HttpResponse};
use bytesize::ByteSize;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::stream::StreamExt as _;
use serde::Deserialize;
use std::convert::TryFrom;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use tar::Archive;

#[derive(Deserialize, Debug)]
pub struct ChunkedDataUploadQuery {
    hash: String,             // UUID to tie all the chunks together (hash of the contents)
    chunk_num: usize,         // which chunk it is, so that we can combine it all in the end
    total_chunks: usize,      // how many chunks to expect
    total_size: usize,        // total size so we can know when we are finished
    is_compressed: bool,      // whether or not we need to decompress the archive
    filename: Option<String>, // maybe a file name if !compressed
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
pub async fn commit_history(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");
    let repo_name: Option<&str> = req.match_info().get("repo_name");
    let commit_or_branch: Option<&str> = req.match_info().get("commit_or_branch");

    let page: usize = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size: usize = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    if let (Some(namespace), Some(repo_name), Some(commit_or_branch)) =
        (namespace, repo_name, commit_or_branch)
    {
        let repo_dir = app_data.path.join(namespace).join(repo_name);
        match p_index_commit_or_branch_history(&repo_dir, commit_or_branch, page, page_size) {
            Ok(response) => HttpResponse::Ok().json(response),
            Err(err) => {
                let msg = format!("{err}");
                HttpResponse::NotFound().json(StatusMessage::error(msg))
            }
        }
    } else {
        let msg = "Must supply `namespace`, `repo_name` and `commit_or_branch` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let commit = api::local::commits::get_by_id(&repo, &commit_id)?
        .ok_or(OxenError::revision_not_found(commit_id.into()))?;

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_found(),
        commit,
    }))
}

pub async fn commits_db_status(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let commits_to_sync = api::local::commits::list_with_missing_dbs(&repo, &commit_id)?;

    log::debug!(
        "About to respond with {} commits to sync",
        commits_to_sync.len()
    );

    Ok(HttpResponse::Ok().json(ListCommitResponse {
        status: StatusMessage::resource_found(),
        commits: commits_to_sync,
    }))
}

pub async fn entries_status(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("Beginning of entries_status_controller");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    log::debug!("Made it through params parsing");

    let commits_to_sync = api::local::commits::list_with_missing_entries(&repo, &commit_id)?;

    log::debug!(
        "About to respond with following missing entries: {:?}",
        commits_to_sync
    );

    Ok(HttpResponse::Ok().json(ListCommitResponse {
        status: StatusMessage::resource_found(),
        commits: commits_to_sync,
    }))
}

pub async fn latest_synced(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, &repo_name)?;
    let commit_id = path_param(&req, "commit_id")?;

    let commits = api::local::commits::list_from(&repository, &commit_id)?;
    let mut latest_synced: Option<Commit> = None;
    let mut commits_to_sync: Vec<Commit> = Vec::new();

    // Iterate first to last over commits
    for commit in commits {
        log::debug!("latest_synced checking commit {:?}", commit.id);
        match commit_cacher::get_status(&repository, &commit) {
            Ok(Some(CacherStatusType::Success)) => {
                match content_validator::is_valid(&repository, &commit) {
                    Ok(true) => {
                        // Iterating backwards, so this is the latest synced commit
                        // For this to work, we need to maintain relative order of commits in redis queue push
                        log::debug!("latest_synced commit is valid: {:?}", commit.id);
                        latest_synced = Some(commit);
                        break;
                    }
                    Ok(false) => {
                        // Desired behavior here?
                        log::debug!("latest_synced commit is invalid: {:?}", commit.id);
                        return Ok(HttpResponse::Ok().json(IsValidStatusMessage {
                            status: String::from(STATUS_ERROR),
                            status_message: String::from(MSG_CONTENT_IS_INVALID),
                            status_description: "Content is not valid".to_string(),
                            is_processing: false,
                            is_valid: false,
                        }));
                    }
                    err => {
                        // Desired behavior here?
                        log::error!("latest_synced content_validator::is_valid error {err:?}");
                        return Ok(HttpResponse::InternalServerError().json(
                            IsValidStatusMessage {
                                status: String::from(STATUS_ERROR),
                                status_message: String::from(MSG_INTERNAL_SERVER_ERROR),
                                status_description: format!("Err: {err:?}"),
                                is_processing: false,
                                is_valid: false,
                            },
                        ));
                    }
                }
            }
            Ok(Some(CacherStatusType::Pending)) => {
                log::debug!("latest_synced commit is pending {}", commit.id);
                commits_to_sync.push(commit);
            }
            Ok(Some(CacherStatusType::Failed)) => {
                let errors = commit_cacher::get_failures(&repository, &commit).unwrap();
                let error_str = errors
                    .into_iter()
                    .map(|e| e.status_message)
                    .collect::<Vec<String>>()
                    .join(", ");
                log::error!("latest_synced CacherStatusType::Failed for commit {error_str}");
                return Ok(
                    HttpResponse::InternalServerError().json(IsValidStatusMessage {
                        status: String::from(STATUS_ERROR),
                        status_message: String::from(MSG_FAILED_PROCESS),
                        status_description: format!("Err: {error_str}"),
                        is_processing: false,
                        is_valid: false,
                    }),
                );
            }
            Ok(None) => {
                log::debug!("latest_synced commit not yet processing: {}", commit.id);
                // Panic to fail the test
                // panic!("NONE WORLD IS IN HERE");
                commits_to_sync.push(commit);
            }

            err => {
                log::error!("latest_synced {:?}", err);
                return Ok(
                    HttpResponse::InternalServerError().json(IsValidStatusMessage {
                        status: String::from(STATUS_ERROR),
                        status_message: String::from(MSG_INTERNAL_SERVER_ERROR),
                        status_description: format!("Err: {err:?}"),
                        is_processing: false,
                        is_valid: false,
                    }),
                );
            }
        };
    }

    Ok(HttpResponse::Ok().json(CommitSyncStatusResponse {
        status: StatusMessage::resource_found(),
        latest_synced,
        num_unsynced: commits_to_sync.len(),
    }))
}

pub async fn is_synced(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_or_branch = path_param(&req, "commit_or_branch")?;
    let repository = get_repo(&app_data.path, namespace, &repo_name)?;

    let commit = api::local::revisions::get(&repository, &commit_or_branch)?.ok_or(
        OxenError::revision_not_found(commit_or_branch.clone().into()),
    )?;

    let response = match commit_cacher::get_status(&repository, &commit) {
        Ok(Some(CacherStatusType::Success)) => {
            match content_validator::is_valid(&repository, &commit) {
                Ok(true) => HttpResponse::Ok().json(IsValidStatusMessage {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_FOUND),
                    status_description: String::from(""),
                    is_processing: false,
                    is_valid: true,
                }),
                Ok(false) => {
                    log::error!("content_validator::is_valid false");

                    HttpResponse::Ok().json(IsValidStatusMessage {
                        status: String::from(STATUS_ERROR),
                        status_message: String::from(MSG_CONTENT_IS_INVALID),
                        status_description: "Content is not valid".to_string(),
                        is_processing: false,
                        is_valid: false,
                    })
                }
                err => {
                    log::error!("content_validator::is_valid error {err:?}");

                    HttpResponse::InternalServerError().json(IsValidStatusMessage {
                        status: String::from(STATUS_ERROR),
                        status_message: String::from(MSG_INTERNAL_SERVER_ERROR),
                        status_description: format!("Err: {err:?}"),
                        is_processing: false,
                        is_valid: false,
                    })
                }
            }
        }
        Ok(Some(CacherStatusType::Pending)) => HttpResponse::Ok().json(IsValidStatusMessage {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_IS_PROCESSING),
            status_description: String::from("Commit is still processing"),
            is_processing: true,
            is_valid: false,
        }),
        Ok(Some(CacherStatusType::Failed)) => {
            let errors = commit_cacher::get_failures(&repository, &commit).unwrap();
            let error_str = errors
                .into_iter()
                .map(|e| e.status_message)
                .collect::<Vec<String>>()
                .join(", ");
            log::error!("CacherStatusType::Failed for commit {error_str}");
            HttpResponse::InternalServerError().json(IsValidStatusMessage {
                status: String::from(STATUS_ERROR),
                status_message: String::from(MSG_FAILED_PROCESS),
                status_description: format!("Err: {error_str}"),
                is_processing: false,
                is_valid: false,
            })
        }
        Ok(None) => {
            // This means background status was never kicked off...
            log::debug!(
                "get_status commit {} no status kicked off for repo: {}",
                commit_or_branch,
                repo_name
            );
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        err => {
            log::error!("Error getting status... {:?}", err);
            HttpResponse::InternalServerError().json(IsValidStatusMessage {
                status: String::from(STATUS_ERROR),
                status_message: String::from(MSG_INTERNAL_SERVER_ERROR),
                status_description: format!("Err: {err:?}"),
                is_processing: false,
                is_valid: false,
            })
        }
    };

    Ok(response)
}

pub async fn parents(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let commit_or_branch = path_param(&req, "commit_or_branch")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let parents = p_get_parents(&repository, &commit_or_branch)?;
    Ok(HttpResponse::Ok().json(ListCommitResponse {
        status: StatusMessage::resource_found(),
        commits: parents,
    }))
}

fn p_get_parents(
    repository: &LocalRepository,
    commit_or_branch: &str,
) -> Result<Vec<Commit>, OxenError> {
    match api::local::revisions::get(repository, commit_or_branch)? {
        Some(commit) => api::local::commits::get_parents(repository, &commit),
        None => Ok(vec![]),
    }
}

fn p_index(repo_dir: &Path) -> Result<ListCommitResponse, OxenError> {
    let repo = LocalRepository::new(repo_dir)?;
    let commits = api::local::commits::list(&repo)?;
    Ok(ListCommitResponse::success(commits))
}

fn p_index_commit_or_branch_history(
    repo_dir: &Path,
    commit_or_branch: &str,
    page_num: usize,
    page_size: usize,
) -> Result<PaginatedCommits, OxenError> {
    let repo = LocalRepository::new(repo_dir)?;
    let commits =
        api::local::commits::list_from_paginated(&repo, commit_or_branch, page_num, page_size)?;
    // log::debug!("controllers::commits: : {:#?}", commits);
    Ok(commits)
}

/// Download the database that holds all the commits and their parents
pub async fn download_commits_db(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let buffer = compress_commits_db(&repository)?;
    Ok(HttpResponse::Ok().body(buffer))
}

/// Take the commits db and compress it into a tarball buffer we can return
fn compress_commits_db(repository: &LocalRepository) -> Result<Vec<u8>, OxenError> {
    // Tar and gzip the commit db directory
    // zip up the rocksdb in history dir, and post to server
    let commit_dir = util::fs::oxen_hidden_dir(&repository.path).join(COMMITS_DIR);
    // This will be the subdir within the tarball
    let tar_subdir = Path::new(COMMITS_DIR);

    log::debug!("Compressing commit db from dir {:?}", commit_dir);
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    tar.append_dir_all(tar_subdir, commit_dir)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!("Compressed commit dir size is {}", ByteSize::b(total_size));

    Ok(buffer)
}

/// Download the database of all entries given a specific commit
pub async fn download_commit_entries_db(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let commit_or_branch = path_param(&req, "commit_or_branch")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let commit = api::local::revisions::get(&repository, &commit_or_branch)?
        .ok_or(OxenError::revision_not_found(commit_or_branch.into()))?;

    let buffer = compress_commit(&repository, &commit)?;
    Ok(HttpResponse::Ok().body(buffer))
}

// Allow downloading of sub-dirs for efficiency
fn compress_commit(repository: &LocalRepository, commit: &Commit) -> Result<Vec<u8>, OxenError> {
    // Tar and gzip the commit db directory
    // zip up the rocksdb in history dir, and post to server
    let commit_dir = util::fs::oxen_hidden_dir(&repository.path)
        .join(HISTORY_DIR)
        .join(commit.id.clone());
    // This will be the subdir within the tarball
    let tar_subdir = Path::new(HISTORY_DIR).join(commit.id.clone());

    log::debug!("Compressing commit {} from dir {:?}", commit.id, commit_dir);
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    // Ignore cache and other dirs, only take what we need
    let dirs_to_compress = vec![DIRS_DIR, FILES_DIR, SCHEMAS_DIR];

    for dir in &dirs_to_compress {
        let full_path = commit_dir.join(dir);
        let tar_path = tar_subdir.join(dir);
        if full_path.exists() {
            tar.append_dir_all(&tar_path, full_path)?;
        }
    }

    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Compressed commit {} size is {}",
        commit.id,
        ByteSize::b(total_size)
    );

    Ok(buffer)
}

pub async fn create(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("Got commit data: {}", body);

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let commit: Commit = match serde_json::from_str(&body) {
        Ok(commit) => commit,
        Err(_) => return Err(OxenHttpError::BadRequest("Invalid commit data".into())),
    };

    let bn: BranchName =
        match serde_json::from_str(&body) {
            Ok(name) => name,
            Err(_) => return Err(OxenHttpError::BadRequest(
                "Must supply `branch_name` in body. Upgrade CLI to greater than v0.6.1 if failing."
                    .into(),
            )),
        };

    // Create Commit from uri params
    match api::local::commits::create_commit_object(&repository.path, bn.branch_name, &commit) {
        Ok(_) => Ok(HttpResponse::Ok().json(CommitResponse {
            status: StatusMessage::resource_created(),
            commit: commit.to_owned(),
        })),
        Err(OxenError::RootCommitDoesNotMatch(commit_id)) => {
            log::error!("Err create_commit: RootCommitDoesNotMatch {}", commit_id);
            Err(OxenHttpError::BadRequest("Remote commit history does not match local commit history. Make sure you are pushing to the correct remote.".into()))
        }
        Err(err) => {
            log::error!("Err create_commit: {}", err);
            Err(OxenHttpError::InternalServerError)
        }
    }
}

pub async fn create_bulk(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("Got bulk commit data: {}", body);

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let commits: Vec<CommitWithBranchName> = match serde_json::from_str(&body) {
        Ok(commits) => commits,
        Err(_) => return Err(OxenHttpError::BadRequest("Invalid commit data".into())),
    };

    let mut result_commits: Vec<Commit> = Vec::new();

    let commit_reader = CommitReader::new(&repository)?;
    let commit_writer = CommitWriter::new(&repository)?;

    let ref_writer = RefWriter::new(&repository)?;

    for commit_with_branch in &commits {
        // get branch name from this commit and raise error if it's not there
        let bn = &commit_with_branch.branch_name;

        // Get commit from commit_with_branch
        let commit = Commit::from_with_branch_name(commit_with_branch);

        log::debug!("Creating commit: {:?}", commit);

        if let Err(err) = api::local::commits::create_commit_object_with_committers(
            &repository.path,
            bn,
            &commit,
            &commit_reader,
            &commit_writer,
            &ref_writer,
        ) {
            log::error!("Err create_commit: {}", err);
            match err {
                OxenError::RootCommitDoesNotMatch(commit_id) => {
                    log::error!("Err create_commit: RootCommitDoesNotMatch {}", commit_id);
                    return Err(OxenHttpError::BadRequest("Remote commit history does not match local commit history. Make sure you are pushing to the correct remote.".into()));
                }
                _ => {
                    return Err(OxenHttpError::InternalServerError);
                }
            }
        }

        result_commits.push(commit);
    }
    Ok(HttpResponse::Ok().json(ListCommitResponse {
        status: StatusMessage::resource_created(),
        commits: result_commits.to_owned(),
    }))
}

/// Controller to upload large chunks of data that will be combined at the end
pub async fn upload_chunk(
    req: HttpRequest,
    mut chunk: web::Payload,                   // the chunk of the file body,
    query: web::Query<ChunkedDataUploadQuery>, // gives the file
) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("made it to the upload controller");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, name)?;

    let commit_reader = CommitReader::new(&repo)?;

    log::debug!("made it past params parsing");

    let commit = commit_reader
        .get_commit_by_id(&commit_id)?
        .ok_or(OxenError::revision_not_found(commit_id.into()))?;

    log::debug!("made it past getting commit");

    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let id = query.hash.clone();
    let size = query.total_size;
    let chunk_num = query.chunk_num;
    let total_chunks = query.total_chunks;

    log::debug!(
        "upload_raw got chunk {chunk_num}/{total_chunks} of upload {id} of total size {size}"
    );

    // Create a tmp dir for this upload
    let tmp_dir = hidden_dir.join("tmp").join("chunked").join(id);
    let chunk_file = tmp_dir.join(format!("chunk_{chunk_num:016}"));

    // mkdir if !exists
    if !tmp_dir.exists() {
        if let Err(err) = std::fs::create_dir_all(&tmp_dir) {
            log::error!("Could not complete chunk upload, mkdir failed: {:?}", err);
            return Ok(
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            );
        }
    }

    // Read bytes from body
    let mut bytes = web::BytesMut::new();
    while let Some(item) = chunk.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }

    // Write to tmp file
    log::debug!("upload_raw writing file {:?}", chunk_file);
    match std::fs::File::create(&chunk_file) {
        Ok(mut f) => {
            match f.write_all(&bytes) {
                Ok(_) => {
                    // Successfully wrote chunk
                    log::debug!("upload_raw successfully wrote chunk {:?}", chunk_file);

                    check_if_upload_complete_and_unpack(
                        hidden_dir,
                        tmp_dir,
                        total_chunks,
                        size,
                        query.is_compressed,
                        query.filename.to_owned(),
                    );

                    Ok(HttpResponse::Ok().json(CommitResponse {
                        status: StatusMessage::resource_created(),
                        commit: commit.to_owned(),
                    }))
                }
                Err(err) => {
                    log::error!(
                        "Could not complete chunk upload, file create failed: {:?}",
                        err
                    );
                    Ok(HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()))
                }
            }
        }
        Err(err) => {
            log::error!(
                "Could not complete chunk upload, file create failed: {:?}",
                err
            );
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

fn check_if_upload_complete_and_unpack(
    hidden_dir: PathBuf,
    tmp_dir: PathBuf,
    total_chunks: usize,
    total_size: usize,
    is_compressed: bool,
    filename: Option<String>,
) {
    let mut files = util::fs::list_files_in_dir(&tmp_dir);

    if total_chunks < files.len() {
        return;
    }
    files.sort();

    let mut uploaded_size: u64 = 0;
    for file in files.iter() {
        match util::fs::metadata(file) {
            Ok(metadata) => {
                uploaded_size += metadata.len();
            }
            Err(err) => {
                log::warn!("Err getting metadata on {:?}\n{:?}", file, err);
            }
        }
    }

    log::debug!(
        "upload_raw checking if complete... {} == {}",
        total_size,
        uploaded_size
    );

    if total_size == (uploaded_size as usize) {
        // std::thread::spawn(move || {
        // Get tar.gz bytes for history/COMMIT_ID data
        log::debug!("Decompressing {} bytes to {:?}", total_size, hidden_dir);

        let mut buffer: Vec<u8> = Vec::new();
        for file in files.iter() {
            log::debug!("Reading file bytes {:?}", file);
            let mut f = std::fs::File::open(file).unwrap();

            f.read_to_end(&mut buffer).unwrap();
        }

        // TODO: better error handling...
        // Combine into actual file data
        if is_compressed {
            // Unpack tarball to our hidden dir
            let mut archive = Archive::new(GzDecoder::new(&buffer[..]));
            unpack_entry_tarball(&hidden_dir, &mut archive);
        } else {
            // just write buffer to disk
            match filename {
                Some(filename) => {
                    // TODO: better error handling...

                    log::debug!("Got filename {}", filename);
                    let full_path = hidden_dir.join(filename);
                    log::debug!("Unpack to {:?}", full_path);
                    if let Some(parent) = full_path.parent() {
                        if !parent.exists() {
                            std::fs::create_dir_all(parent).expect("Could not create parent dir");
                        }
                    }

                    let mut f = std::fs::File::create(&full_path).expect("Could write file");
                    match f.write_all(&buffer) {
                        Ok(_) => {
                            log::debug!("Unpack successful! {:?}", full_path);
                        }
                        Err(err) => {
                            log::error!("Could not write all data to disk {:?}", err);
                        }
                    }
                }
                None => {
                    log::error!("Must supply filename if !compressed");
                }
            }
        }

        // Cleanup tmp files
        util::fs::remove_dir_all(tmp_dir).unwrap();
        // });
    }
}

/// Controller to upload the commit database
pub async fn upload(
    req: HttpRequest,
    mut body: web::Payload, // the actual file body
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, name)?;

    let commit = api::local::commits::get_by_id(&repo, &commit_id)?
        .ok_or(OxenError::revision_not_found(commit_id.to_owned().into()))?;
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);

    // Read bytes from body
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }

    // Compute total size as u64
    let total_size: u64 = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Got compressed data for commit {} -> {}",
        commit_id,
        ByteSize::b(total_size)
    );

    // Unpack in background thread because could take awhile
    // std::thread::spawn(move || {
    // Get tar.gz bytes for history/COMMIT_ID data
    log::debug!("Decompressing {} bytes to {:?}", bytes.len(), hidden_dir);
    // Unpack tarball to our hidden dir
    let mut archive = Archive::new(GzDecoder::new(&bytes[..]));
    unpack_entry_tarball(&hidden_dir, &mut archive);
    // });

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit: commit.to_owned(),
    }))
}

/// Notify that the push should be complete, and we should start doing our background processing
pub async fn complete(req: HttpRequest) -> Result<HttpResponse, Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    // name to the repo, should be in url path so okay to unwrap
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let commit_id: &str = req.match_info().get("commit_id").unwrap();

    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => {
            match api::local::commits::get_by_id(&repo, commit_id) {
                Ok(Some(commit)) => {
                    // Kick off processing in background thread because could take awhile
                    std::thread::spawn(move || {
                        log::debug!("Processing commit {:?} on repo {:?}", commit, repo.path);
                        let force = false;
                        match commit_cacher::run_all(&repo, &commit, force) {
                            Ok(_) => {
                                log::debug!(
                                    "Success processing commit {:?} on repo {:?}",
                                    commit,
                                    repo.path
                                );
                            }
                            Err(err) => {
                                log::error!(
                                    "Could not process commit {:?} on repo {:?}: {}",
                                    commit,
                                    repo.path,
                                    err
                                );
                            }
                        }
                    });

                    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
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

// TODO use this more

// Bulk complete
pub async fn complete_bulk(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("In the commits controller");
    let app_data = req.app_data::<OxenAppData>().unwrap();
    // name to the repo, should be in url path so okay to unwrap
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let _repo = get_repo(&app_data.path, namespace, repo_name)?;
    // Deserialize the "commits" param into Vec<Commit> with serde
    let commits: Vec<Commit> = match serde_json::from_str(&body) {
        Ok(commits) => commits,
        Err(_) => return Err(OxenHttpError::BadRequest("Invalid commit data".into())),
    };

    // Redis connection - TODO, make a globally accessible connection pool

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost/".to_string());
    let redis_client = redis::Client::open(redis_url).expect("Failed to connect to redis");
    let mut con = redis_client.get_connection()?;

    // Get repo by name
    let repo =
        api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)?
            .ok_or(OxenError::repo_not_found(RepositoryNew::new(
                namespace, repo_name,
            )))?;

    let repo_path = repo.path.clone();

    let commit_reader = CommitReader::new(&repo)?;

    for req_commit in commits {
        let commit_id = req_commit.id;
        let commit = commit_reader
            .get_commit_by_id(&commit_id)?
            .ok_or(OxenError::revision_not_found(commit_id.clone().into()))?;

        let repo_path_clone = repo_path.clone();

        // Append a task to the queue
        let task = PostPushComplete {
            commit: commit.clone(),
            repo: repo.clone(),
        };
        let task_bytes = bincode::serialize(&task).unwrap();

        log::debug!(
            "Adding to queue for commit {:?} on repo {:?}",
            commit,
            &repo_path_clone
        );

        let _: isize = redis::cmd("LPUSH")
            .arg("commit_queue")
            .arg(task_bytes.clone())
            .query(&mut con)?;
    }
    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
}

fn unpack_entry_tarball(hidden_dir: &Path, archive: &mut Archive<GzDecoder<&[u8]>>) {
    // Unpack and compute HASH and save next to the file to speed up computation later
    match archive.entries() {
        Ok(entries) => {
            for file in entries {
                if let Ok(mut file) = file {
                    // Why hash now? To make sure everything synced properly
                    // When we want to check is_synced, it is expensive to rehash everything
                    // But since upload is network bound already, hashing here makes sense, and we will just
                    // load the HASH file later
                    file.unpack_in(hidden_dir).unwrap();
                    let path = file.path().unwrap();
                    let full_path = hidden_dir.join(&path);
                    let hash_dir = full_path.parent().unwrap();
                    let hash_file = hash_dir.join(HASH_FILE);

                    // log::debug!("unpack_entry_tarball unpacking entry {:?} to {:?}", path, full_path);

                    if path.starts_with("versions/files/") {
                        let hash = util::hasher::hash_file_contents(&full_path).unwrap();
                        util::fs::write_to_path(&hash_file, &hash)
                            .expect("Could not write hash file");
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
}

#[cfg(test)]
mod tests {

    use actix_web::body::to_bytes;
    use actix_web::{web, App};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::path::Path;
    use std::thread;

    use liboxen::api;
    use liboxen::command;
    use liboxen::constants::OXEN_HIDDEN_DIR;
    use liboxen::error::OxenError;
    use liboxen::util;
    use liboxen::view::{CommitResponse, ListCommitResponse};

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::params::PageNumQuery;
    use crate::test::{self, init_test_env};

    #[actix_web::test]
    async fn test_controllers_commits_index_empty() -> Result<(), OxenError> {
        init_test_env();
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, namespace, name)?;

        let uri = format!("/oxen/{namespace}/{name}/commits");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::commits::index(req).await;

        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        println!("Got response: {text}");
        let list: ListCommitResponse = serde_json::from_str(text)?;
        // Plus the initial commit
        assert_eq!(list.commits.len(), 1);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

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

        let uri = format!("/oxen/{namespace}/{name}/commits");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::commits::index(req).await;
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        // Plus the initial commit
        assert_eq!(list.commits.len(), 3);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

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
        api::local::branches::create_checkout(&repo, branch_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "world")?;
        command::add(&repo, path)?;
        command::commit(&repo, "second commit")?;

        let uri = format!("/oxen/{namespace}/{repo_name}/commits/{branch_name}/history");
        let req = test::repo_request_with_param(
            &sync_dir,
            &uri,
            namespace,
            repo_name,
            "commit_or_branch",
            branch_name,
        );

        let query: web::Query<PageNumQuery> =
            web::Query::from_query("page=1&page_size=10").unwrap();
        let resp = controllers::commits::commit_history(req, query).await;
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        // Plus the initial commit
        assert_eq!(list.commits.len(), 3);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    // Switch branches, add a commit, and only list commits from first branch
    #[actix_web::test]
    async fn test_controllers_commits_list_some_commits_on_branch() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        command::add(&repo, path)?;
        command::commit(&repo, "first commit")?;

        let branch_name = "feature/list-commits";
        api::local::branches::create_checkout(&repo, branch_name)?;

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

        let query: web::Query<PageNumQuery> =
            web::Query::from_query("page=1&page_size=10").unwrap();
        let resp = controllers::commits::commit_history(req, query).await;
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        // there should be 2 instead of the 3 total
        assert_eq!(list.commits.len(), 2);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_commits_upload() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        command::add(&repo, &hello_file)?;
        let commit = command::commit(&repo, "First commit")?;

        // create random tarball to post.. currently no validation that it is a valid commit dir
        let path_to_compress = format!("history/{}", commit.id);
        let commit_dir_name = format!("data/test/runs/{}", commit.id);
        let commit_dir = Path::new(&commit_dir_name);
        std::fs::create_dir_all(commit_dir)?;
        // Write a random file to it
        let zipped_filename = "blah.txt";
        let zipped_file_contents = "sup";
        let random_file = commit_dir.join(zipped_filename);
        util::fs::write_to_path(&random_file, zipped_file_contents)?;

        println!("Compressing commit {}...", commit.id);
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);

        tar.append_dir_all(&path_to_compress, commit_dir)?;
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
        println!("Looking for file: {uploaded_file:?}");
        assert!(uploaded_file.exists());
        assert_eq!(
            util::fs::read_from_path(&uploaded_file)?,
            zipped_file_contents
        );

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;
        util::fs::remove_dir_all(commit_dir)?;

        Ok(())
    }
}
