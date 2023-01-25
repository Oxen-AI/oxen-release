use crate::app_data::OxenAppData;
use crate::view::PaginatedLinesResponse;

use liboxen::api;
use liboxen::constants::AVG_CHUNK_SIZE;
use liboxen::error::OxenError;
use liboxen::index::CommitDirReader;
use liboxen::model::{Commit, CommitEntry, LocalRepository, RemoteEntry};
use liboxen::util;
use liboxen::view::http::{MSG_RESOURCE_CREATED, MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{PaginatedEntries, RemoteEntryResponse, StatusMessage};

use actix_web::{web, HttpRequest, HttpResponse};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::stream::StreamExt as _;
use serde::Deserialize;

use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

#[derive(Deserialize, Debug)]
pub struct PageNumQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

#[derive(Deserialize, Debug)]
pub struct ChunkQuery {
    pub chunk_start: Option<u64>,
    pub chunk_size: Option<u64>,
}

pub async fn create(
    req: HttpRequest,
    body: web::Payload,
    data: web::Query<CommitEntry>,
) -> Result<HttpResponse, actix_web::Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => p_create_entry(&repo, body, data).await,
        Ok(None) => {
            log::debug!("404 could not get repo {}", name,);
            Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
        }
        Err(err) => {
            let msg = format!("Could not find repo at path\nErr: {}", err);
            Ok(HttpResponse::BadRequest().json(StatusMessage::error(&msg)))
        }
    }
}

pub async fn download_data_from_version_paths(
    req: HttpRequest,
    mut body: web::Payload,
) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            let mut bytes = web::BytesMut::new();
            while let Some(item) = body.next().await {
                bytes.extend_from_slice(&item.unwrap());
            }
            log::debug!(
                "download_data_from_version_paths got repo [{}] and content_ids size {}",
                name,
                bytes.len()
            );

            let mut gz = GzDecoder::new(&bytes[..]);
            let mut line_delimited_files = String::new();
            gz.read_to_string(&mut line_delimited_files).unwrap();

            let content_files: Vec<&str> = line_delimited_files.split('\n').collect();

            let enc = GzEncoder::new(Vec::new(), Compression::default());
            let mut tar = tar::Builder::new(enc);

            log::debug!("Got {} content ids", content_files.len());
            for content_file in content_files.iter() {
                if content_file.is_empty() {
                    // last line might be empty on split \n
                    continue;
                }

                let version_path = repo.path.join(content_file);
                if version_path.exists() {
                    tar.append_path_with_name(version_path, content_file)
                        .unwrap();
                } else {
                    log::error!(
                        "Could not find content: {:?} -> {:?}",
                        content_file,
                        version_path
                    );
                }
            }

            tar.finish().unwrap();
            let buffer: Vec<u8> = tar.into_inner().unwrap().finish().unwrap();
            HttpResponse::Ok().body(buffer)
        }
        Ok(None) => {
            log::debug!("Could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("Unable to get repository {}. Err: {}", name, err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

/// Download a chunk of a larger file
pub async fn download_chunk(req: HttpRequest, query: web::Query<ChunkQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            if let Ok(Some((commit_id, _, filepath))) =
                util::resource::parse_resource(&repo, &resource)
            {
                log::debug!(
                    "entries::download_chunk commit_id [{}] and filepath {:?}",
                    commit_id,
                    filepath
                );

                match util::fs::version_path_for_commit_id(&repo, &commit_id, &filepath) {
                    Ok(version_path) => {
                        let chunk_start: u64 = query.chunk_start.unwrap_or(0);
                        let chunk_size: u64 = query.chunk_size.unwrap_or(AVG_CHUNK_SIZE);

                        let mut f = File::open(version_path).unwrap();
                        f.seek(std::io::SeekFrom::Start(chunk_start)).unwrap();
                        let mut buffer = vec![0u8; chunk_size as usize];
                        f.read_exact(&mut buffer).unwrap();

                        HttpResponse::Ok().body(buffer)
                    }
                    Err(err) => {
                        log::error!("Error listing lines in file {:?}", err);
                        HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error())
                    }
                }
            } else {
                log::debug!(
                    "entries::download_chunk could not find resource from uri {:?}",
                    resource
                );
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
        }
        Ok(None) => {
            log::debug!("Could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("Unable to get repository {}. Err: {}", name, err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

pub async fn download_page(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let commit_id: &str = req.match_info().get("commit_id").unwrap();

    // default to first page with first ten values
    let page: usize = query.page.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);

    log::debug!(
        "download_entries repo name [{}] commit_id [{}] page {} page_size {}",
        name,
        commit_id,
        page,
        page_size,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            log::debug!("download_entries got repo [{}]", name);
            match get_entries_for_page(&repo, commit_id, page, page_size) {
                Ok((entries, commit)) => match compress_entries(&repo, &commit, &entries.entries) {
                    Ok(buffer) => HttpResponse::Ok().body(buffer),
                    Err(err) => {
                        log::error!(
                            "Unable to get compress {} entries Err: {}",
                            entries.entries.len(),
                            err
                        );
                        HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error())
                    }
                },
                Err(status_message) => HttpResponse::InternalServerError().json(status_message),
            }
        }
        Ok(None) => {
            log::debug!("Could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("Unable to get commit id {}. Err: {}", commit_id, err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

fn compress_entries(
    repo: &LocalRepository,
    commit: &Commit,
    entries: &[RemoteEntry],
) -> Result<Vec<u8>, OxenError> {
    let entry_reader = CommitDirReader::new(repo, commit)?;

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    for entry in entries.iter() {
        let filename = &entry.filename;
        if let Some(entry) = entry_reader.get_entry(Path::new(filename))? {
            let version_path = util::fs::version_path(repo, &entry);
            tar.append_path_with_name(version_path, filename)?;
        } else {
            log::error!(
                "Could not read entry {} from commit {}",
                filename,
                commit.id
            );
        }
    }

    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    Ok(buffer)
}

pub async fn list_entries(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    // default to first page with first ten values
    let page: usize = query.page.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);

    log::debug!(
        "list_entries repo name [{}] resource [{:?}] page {} page_size {}",
        name,
        resource,
        page,
        page_size,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            if let Ok(Some((commit_id, _, filepath))) =
                util::resource::parse_resource(&repo, &resource)
            {
                log::debug!(
                    "entries::list_entries commit_id [{}] and filepath {:?}",
                    commit_id,
                    filepath
                );
                match get_entries_for_page(&repo, &commit_id, page, page_size) {
                    Ok((entries, _commit)) => HttpResponse::Ok().json(entries),
                    Err(status_message) => HttpResponse::InternalServerError().json(status_message),
                }
            } else {
                log::debug!(
                    "entries::list_entries could not find resource from uri {:?}",
                    resource
                );
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
        }
        Ok(None) => {
            log::debug!("Could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("Unable to get resource {:?}. Err: {}", resource, err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

pub async fn list_lines_in_file(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();

    // default to first page with first ten values
    let page: usize = query.page.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);

    log::debug!(
        "list_entries repo name [{}] resource [{:?}] page {} page_size {}",
        name,
        resource,
        page,
        page_size,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            log::debug!("list_lines_in_file got repo [{}]", name);
            if let Ok(Some((commit_id, _, filepath))) =
                util::resource::parse_resource(&repo, &resource)
            {
                log::debug!(
                    "list_lines_in_file got commit_id [{}] and filepath {:?}",
                    commit_id,
                    filepath
                );
                match util::fs::version_path_for_commit_id(&repo, &commit_id, &filepath) {
                    Ok(version_path) => {
                        let start = page * page_size;
                        let (lines, total_entries) =
                            liboxen::util::fs::read_lines_paginated_ret_size(
                                &version_path,
                                start,
                                page_size,
                            );
                        let total_pages = (total_entries as f64 / page_size as f64) + 1f64;
                        HttpResponse::Ok().json(PaginatedLinesResponse {
                            status: String::from(STATUS_SUCCESS),
                            status_message: String::from(MSG_RESOURCE_FOUND),
                            lines,
                            page_size,
                            page_number: page,
                            total_pages: total_pages as usize,
                            total_entries,
                        })
                    }
                    Err(err) => {
                        log::error!("Error listing lines in file {:?}", err);
                        HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error())
                    }
                }
            } else {
                log::debug!(
                    "list_lines_in_file Could not find resource from uri {:?}",
                    resource
                );
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
        }
        Ok(None) => {
            log::debug!("list_lines_in_file Could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("Unable to list lines {:?}. Err: {}", resource, err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

pub fn get_entries_for_page(
    repo: &LocalRepository,
    commit_id: &str,
    page_num: usize,
    page_size: usize,
) -> Result<(PaginatedEntries, Commit), StatusMessage> {
    match api::local::commits::get_by_id(repo, commit_id) {
        Ok(Some(commit)) => {
            log::debug!(
                "get_entries_for_page got commit [{}] '{}'",
                commit.id,
                commit.message
            );
            match api::local::entries::list_page(repo, &commit, &page_num, &page_size) {
                Ok(entries) => {
                    log::debug!(
                        "get_entries_for_page commit {} got {} entries",
                        commit_id,
                        entries.len()
                    );
                    let entries: Vec<RemoteEntry> =
                        entries.into_iter().map(|entry| entry.to_remote()).collect();

                    let total_entries: usize = api::local::entries::count_for_commit(repo, &commit)
                        .unwrap_or(entries.len());
                    let total_pages = (total_entries as f64 / page_size as f64) + 1f64;
                    let view = PaginatedEntries {
                        status: String::from(STATUS_SUCCESS),
                        status_message: String::from(MSG_RESOURCE_FOUND),
                        page_size,
                        page_number: page_num,
                        total_pages: total_pages as usize,
                        total_entries,
                        entries,
                    };
                    Ok((view, commit))
                }
                Err(err) => {
                    log::error!("Unable to list repositories. Err: {}", err);
                    Err(StatusMessage::internal_server_error())
                }
            }
        }
        Ok(None) => {
            log::debug!("Could not find commit with id {}", commit_id);

            Err(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("Unable to get commit id {}. Err: {}", commit_id, err);
            Err(StatusMessage::internal_server_error())
        }
    }
}

async fn p_create_entry(
    repository: &LocalRepository,
    mut body: web::Payload,
    data: web::Query<CommitEntry>,
) -> Result<HttpResponse, actix_web::Error> {
    // Write entry to versions dir
    let version_path = util::fs::version_path(repository, &data);

    if let Some(parent) = version_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let mut file = File::create(&version_path)?;
    let mut total_bytes = 0;
    while let Some(item) = body.next().await {
        total_bytes += file.write(&item?)?;
    }
    log::debug!(
        "Wrote {} bytes to for {:?} to {:?}",
        total_bytes,
        data.path,
        version_path,
    );

    Ok(HttpResponse::Ok().json(RemoteEntryResponse {
        status: String::from(STATUS_SUCCESS),
        status_message: String::from(MSG_RESOURCE_CREATED),
        entry: RemoteEntry::from_commit_entry(&data.into_inner()),
    }))
}

#[cfg(test)]
mod tests {
    use actix_web::{web, App};
    use flate2::read::GzDecoder;
    use std::path::{Path, PathBuf};
    use tar::Archive;

    use liboxen::command;
    use liboxen::error::OxenError;
    use liboxen::model::CommitEntry;
    use liboxen::util;
    use liboxen::view::RemoteEntryResponse;

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_entries_create_text_file() -> Result<(), OxenError> {
        liboxen::test::init_test_env();

        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        let entry = CommitEntry {
            commit_id: String::from("4312"),
            path: PathBuf::from("file.txt"),
            hash: String::from("1234"),
            num_bytes: 0,
            last_modified_seconds: 1,
            last_modified_nanoseconds: 2,
        };

        let payload = "🐂 💨";
        let uri = format!(
            "/oxen/{}/{}/entries?{}",
            namespace,
            name,
            entry.to_uri_encoded()
        );
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData {
                    path: sync_dir.clone(),
                })
                .route(
                    "/oxen/{namespace}/{repo_name}/entries",
                    web::post().to(controllers::entries::create),
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
        let entry_resp: RemoteEntryResponse = serde_json::from_str(body)?;

        // Make sure entry gets populated
        assert_eq!(entry_resp.entry.filename, entry.path.to_str().unwrap());
        assert_eq!(entry_resp.entry.hash, entry.hash);

        // Make sure file actually exists on disk in versions dir
        let uploaded_file = util::fs::version_path(&repo, &entry);

        assert!(uploaded_file.exists());
        // Make sure file contents are the same as the payload
        assert_eq!(util::fs::read_from_path(&uploaded_file)?, payload);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_entries_download_entries() -> Result<(), OxenError> {
        liboxen::test::init_test_env();

        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let name_2 = "Testing-Name-2";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;
        let repo_2 = test::create_local_repo(&sync_dir, namespace, name_2)?;

        // write files to dir
        liboxen::test::populate_dir_with_training_data(&repo.path)?;

        // add the full dir
        let train_dir = repo.path.join(Path::new("train"));
        let num_entries = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;

        // commit the changes
        let commit = command::commit(&repo, "adding training dir")?.expect("Could not commit data");

        // Use the api list the files from the commit
        let uri = format!(
            "/oxen/{}/{}/commits/{}/download_page",
            namespace, name, commit.id
        );
        println!("Hit uri {}", uri);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData {
                    path: sync_dir.clone(),
                })
                .route(
                    "/oxen/{namespace}/{repo_name}/commits/{commit_id}/download_page",
                    web::get().to(controllers::entries::download_page),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        println!("GOT RESP STATUS: {}", resp.response().status());
        assert_eq!(200, resp.response().status());

        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();

        let mut archive = Archive::new(GzDecoder::new(bytes.as_ref()));
        archive.unpack(&repo_2.path)?;

        let repo_2_train_dir = repo_2.path.join(Path::new("train"));
        let repo_2_num_entries = util::fs::rcount_files_in_dir(&repo_2_train_dir);
        assert_eq!(repo_2_num_entries, num_entries);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
