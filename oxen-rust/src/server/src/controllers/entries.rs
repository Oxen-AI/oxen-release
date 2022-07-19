use crate::app_data::OxenAppData;

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::index::CommitEntryReader;
use liboxen::model::{Commit, CommitEntry, LocalRepository, RemoteEntry};
use liboxen::util;
use liboxen::view::http::{MSG_RESOURCE_CREATED, MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{PaginatedEntries, RemoteEntryResponse, StatusMessage};

use actix_web::{web, HttpRequest, HttpResponse};
use futures_util::stream::StreamExt as _;
use serde::Deserialize;
use flate2::write::GzEncoder;
use flate2::Compression;

use std::path::Path;
use std::fs::File;
use std::io::prelude::*;

#[derive(Deserialize, Debug)]
pub struct PageNumQuery {
    page_num: Option<usize>,
    page_size: Option<usize>,
}

pub async fn create(
    req: HttpRequest,
    body: web::Payload,
    data: web::Query<CommitEntry>,
) -> Result<HttpResponse, actix_web::Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    // name of the repo
    let name: &str = req.match_info().get("repo_name").unwrap();
    match api::local::repositories::get_by_name(&app_data.path, name) {
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

pub async fn download_entries(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let name: &str = req.match_info().get("repo_name").unwrap();
    let commit_id: &str = req.match_info().get("commit_id").unwrap();

    // default to first page with first ten values
    let page_num: usize = query.page_num.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);

    log::debug!(
        "list_entries repo name [{}] commit_id [{}] page_num {} page_size {}",
        name,
        commit_id,
        page_num,
        page_size,
    );
    match api::local::repositories::get_by_name(&app_data.path, name) {
        Ok(Some(repo)) => {
            log::debug!("list_entries got repo [{}]", name);
            match get_entries_for_page(&repo, commit_id, page_num, page_size) {
                Ok((entries, commit)) => match compress_entries(&repo, &commit, &entries.entries) {
                    Ok(buffer) => HttpResponse::Ok().body(buffer),
                    Err(err) => {
                        log::error!("Unable to get compress {} entries Err: {}", entries.entries.len(), err);
                        HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
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

fn compress_entries(repo: &LocalRepository, commit: &Commit, entries: &Vec<RemoteEntry>) -> Result<Vec<u8>, OxenError> {
    let entry_reader = CommitEntryReader::new(repo, commit)?;

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    for entry in entries.iter() {
        let filename = &entry.filename;
        if let Some(entry) = entry_reader.get_entry(Path::new(filename))? {
            let version_path = util::fs::version_path(repo, &entry);
            tar.append_path_with_name(version_path, filename)?;
        } else {
            log::error!("Could not read entry {} from commit {}", filename, commit.id);
        }
    }

    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    Ok(buffer)
}

pub async fn list_entries(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let name: &str = req.match_info().get("repo_name").unwrap();
    let commit_id: &str = req.match_info().get("commit_id").unwrap();

    // default to first page with first ten values
    let page_num: usize = query.page_num.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);

    log::debug!(
        "list_entries repo name [{}] commit_id [{}] page_num {} page_size {}",
        name,
        commit_id,
        page_num,
        page_size,
    );
    match api::local::repositories::get_by_name(&app_data.path, name) {
        Ok(Some(repo)) => {
            log::debug!("list_entries got repo [{}]", name);
            match get_entries_for_page(&repo, commit_id, page_num, page_size) {
                Ok((entries, _commit)) => HttpResponse::Ok().json(entries),
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

fn get_entries_for_page(
    repo: &LocalRepository,
    commit_id: &str,
    page_num: usize,
    page_size: usize,
) -> Result<(PaginatedEntries, Commit), StatusMessage> {
    match api::local::commits::get_by_id(repo, commit_id) {
        Ok(Some(commit)) => {
            log::debug!(
                "list_entries got commit [{}] '{}'",
                commit.id,
                commit.message
            );
            match api::local::entries::list_page(repo, &commit, page_num, page_size) {
                Ok(entries) => {
                    log::debug!(
                        "list_entries commit {} got {} entries",
                        commit_id,
                        entries.len()
                    );
                    let entries: Vec<RemoteEntry> =
                        entries.into_iter().map(|entry| entry.to_remote()).collect();

                    let total_entries: usize =
                        api::local::entries::count_for_commit(repo, &commit)
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
    use std::path::{Path, PathBuf};
    use tar::Archive;
    use flate2::read::GzDecoder;

    use liboxen::command;
    use liboxen::error::OxenError;
    use liboxen::model::CommitEntry;
    use liboxen::util;
    use liboxen::view::{PaginatedEntries, RemoteEntryResponse};

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_entries_create_text_file() -> Result<(), OxenError> {
        liboxen::test::init_test_env();

        let sync_dir = test::get_sync_dir()?;

        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, name)?;

        let entry = CommitEntry {
            commit_id: String::from("4312"),
            path: PathBuf::from("file.txt"),
            is_synced: false,
            hash: String::from("1234"),
            last_modified_seconds: 1,
            last_modified_nanoseconds: 2,
        };

        let payload = "🐂 💨";
        let uri = format!("/repositories/{}/entries?{}", name, entry.to_uri_encoded());
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData {
                    path: sync_dir.clone(),
                })
                .route(
                    "/repositories/{repo_name}/entries",
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
    async fn test_entries_controller_list_entries() -> Result<(), OxenError> {
        liboxen::test::init_test_env();

        let sync_dir = test::get_sync_dir()?;

        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, name)?;

        // write files to dir
        liboxen::test::populate_dir_with_training_data(&repo.path)?;

        // add the full dir
        let train_dir = repo.path.join(Path::new("train"));
        let num_entries = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;

        // commit the changes
        let commit = command::commit(&repo, "adding training dir")?.expect("Could not commit data");

        // Use the api list the files from the commit
        let uri = format!("/repositories/{}/commits/{}/entries", name, commit.id);
        println!("Hit uri {}", uri);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData {
                    path: sync_dir.clone(),
                })
                .route(
                    "/repositories/{repo_name}/commits/{commit_id}/entries",
                    web::get().to(controllers::entries::list_entries),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        println!("GOT RESP STATUS: {}", resp.response().status());
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        println!("GOT BODY: {}", body);
        let entries_resp: PaginatedEntries = serde_json::from_str(body)?;

        // Make sure we can fetch all the entries
        assert_eq!(entries_resp.total_entries, num_entries);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_entries_controller_download_entries() -> Result<(), OxenError> {
        liboxen::test::init_test_env();

        let sync_dir = test::get_sync_dir()?;

        let name = "Testing-Name";
        let name_2 = "Testing-Name-2";
        let repo = test::create_local_repo(&sync_dir, name)?;
        let repo_2 = test::create_local_repo(&sync_dir, name_2)?;

        // write files to dir
        liboxen::test::populate_dir_with_training_data(&repo.path)?;

        // add the full dir
        let train_dir = repo.path.join(Path::new("train"));
        let num_entries = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;

        // commit the changes
        let commit = command::commit(&repo, "adding training dir")?.expect("Could not commit data");

        // Use the api list the files from the commit
        let uri = format!("/repositories/{}/commits/{}/download_entries", name, commit.id);
        println!("Hit uri {}", uri);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData {
                    path: sync_dir.clone(),
                })
                .route(
                    "/repositories/{repo_name}/commits/{commit_id}/download_entries",
                    web::get().to(controllers::entries::download_entries),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        println!("GOT RESP STATUS: {}", resp.response().status());
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
