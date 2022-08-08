use crate::app_data::OxenAppData;
use crate::controllers;
use crate::view::PaginatedLinesResponse;

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::index::{CommitEntryReader, CommitReader, RefReader};
use liboxen::model::{Commit, CommitEntry, LocalRepository, RemoteEntry};
use liboxen::util;
use liboxen::view::http::{MSG_RESOURCE_CREATED, MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{PaginatedDirEntries, PaginatedEntries, RemoteEntryResponse, StatusMessage};

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
pub struct DirectoryPageNumQuery {
    directory: Option<String>,
    page_num: Option<usize>,
    page_size: Option<usize>,
}

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

pub async fn download_content_ids(req: HttpRequest, mut body: web::Payload) -> HttpResponse {
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
                "download_content_ids got repo [{}] and content_ids size {}",
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

pub async fn download_page(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let commit_id: &str = req.match_info().get("commit_id").unwrap();

    // default to first page with first ten values
    let page_num: usize = query.page_num.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);

    log::debug!(
        "download_entries repo name [{}] commit_id [{}] page_num {} page_size {}",
        name,
        commit_id,
        page_num,
        page_size,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            log::debug!("download_entries got repo [{}]", name);
            match get_entries_for_page(&repo, commit_id, page_num, page_size) {
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
    let entry_reader = CommitEntryReader::new(repo, commit)?;

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
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
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

/// Returns commit_id,filepath
/// Parses a path looking for either a commit id or a branch name, returns None of neither exist
fn parse_resource(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<(String, PathBuf)>, OxenError> {
    let mut components = path.components().collect::<Vec<_>>();
    let commit_reader = CommitReader::new(repo)?;

    // See if the first component is the commit id
    if let Some(first_component) = components.first() {
        let base_path: &Path = first_component.as_ref();
        let maybe_commit_id = base_path.to_str().unwrap();
        log::debug!("parse_resource looking for commit id {}", maybe_commit_id);
        if let Ok(Some(commit)) = commit_reader.get_commit_by_id(maybe_commit_id) {
            let mut file_path = PathBuf::new();
            for (i, component) in components.iter().enumerate() {
                if i != 0 {
                    let component_path: &Path = component.as_ref();
                    file_path = file_path.join(component_path);
                }
            }
            return Ok(Some((commit.id, file_path)));
        }
    }

    // See if the component has a valid branch name in it
    let ref_reader = RefReader::new(repo)?;
    let mut file_path = PathBuf::new();
    while let Some(component) = components.pop() {
        let component_path: &Path = component.as_ref();
        if file_path == PathBuf::new() {
            file_path = component_path.to_path_buf();
        } else {
            file_path = component_path.join(file_path);
        }

        let mut branch_path = PathBuf::new();
        for component in components.iter() {
            let component_path: &Path = component.as_ref();
            branch_path = branch_path.join(component_path);
        }

        let branch_name = branch_path.to_str().unwrap();
        log::debug!("parse_resource looking for branch {}", branch_name);
        if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
            return Ok(Some((branch.commit_id, file_path)));
        }
    }

    Ok(None)
}

pub async fn list_lines_in_file(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();

    // default to first page with first ten values
    let page_num: usize = query.page_num.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);

    log::debug!(
        "list_entries repo name [{}] resource [{:?}] page_num {} page_size {}",
        name,
        resource,
        page_num,
        page_size,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            log::debug!("list_lines_in_file got repo [{}]", name);
            if let Ok(Some((commit_id, filepath))) = parse_resource(&repo, &resource) {
                log::debug!(
                    "list_lines_in_file got commit_id [{}] and filepath {:?}",
                    commit_id,
                    filepath
                );
                match controllers::repositories::get_version_path_for_commit_id(
                    &repo, &commit_id, &filepath,
                ) {
                    Ok(version_path) => {
                        let start = page_num * page_size;
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
                            page_number: page_num,
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

pub async fn list_files_for_head(
    req: HttpRequest,
    query: web::Query<DirectoryPageNumQuery>,
) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();

    // default to first page with first ten values
    let page_num: usize = query.page_num.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);
    let directory = query
        .directory
        .clone()
        .unwrap_or_else(|| String::from("./"));
    let directory = Path::new(&directory);

    log::debug!(
        "list_files_for_head repo name [{}] directory: {:?} page_num {} page_size {}",
        name,
        directory,
        page_num,
        page_size,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            log::debug!("list_files_for_head got repo [{}]", name);
            if let Ok(commit) = api::local::commits::get_head_commit(&repo) {
                match list_directory_for_commit(&repo, &commit.id, directory, page_num, page_size) {
                    Ok((entries, _commit)) => HttpResponse::Ok().json(entries),
                    Err(status_message) => HttpResponse::InternalServerError().json(status_message),
                }
            } else {
                log::debug!(
                    "list_files_for_head Could not find head commit for repo {}",
                    name
                );
                HttpResponse::Ok().json(PaginatedDirEntries {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_FOUND),
                    page_size: 0,
                    page_number: 0,
                    total_pages: 0,
                    total_entries: 0,
                    entries: vec![],
                })
            }
        }
        Ok(None) => {
            log::debug!("list_files_for_head Could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!(
                "list_files_for_head Unable to list directory {:?} in repo {}/{} for. Err: {}",
                directory,
                namespace,
                name,
                err
            );
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

pub async fn list_files_for_commit(
    req: HttpRequest,
    query: web::Query<DirectoryPageNumQuery>,
) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let commit_id: &str = req.match_info().get("commit_id").unwrap();

    // default to first page with first ten values
    let page_num: usize = query.page_num.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);
    let directory = query
        .directory
        .clone()
        .unwrap_or_else(|| String::from("./"));
    let directory = Path::new(&directory);

    log::debug!(
        "list_files repo name [{}] commit_id [{}] directory: {:?} page_num {} page_size {}",
        name,
        commit_id,
        directory,
        page_num,
        page_size,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            log::debug!("list_files got repo [{}]", name);
            match list_directory_for_commit(&repo, commit_id, directory, page_num, page_size) {
                Ok((entries, _commit)) => HttpResponse::Ok().json(entries),
                Err(status_message) => HttpResponse::InternalServerError().json(status_message),
            }
        }
        Ok(None) => {
            log::debug!("list_files Could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!(
                "list_files Unable to list directory {:?} in repo {}/{} for commit {}. Err: {}",
                directory,
                namespace,
                name,
                commit_id,
                err
            );
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

fn list_directory_for_commit(
    repo: &LocalRepository,
    commit_id: &str,
    directory: &Path,
    page_num: usize,
    page_size: usize,
) -> Result<(PaginatedDirEntries, Commit), StatusMessage> {
    match api::local::commits::get_by_id(repo, commit_id) {
        Ok(Some(commit)) => {
            log::debug!(
                "list_directory_for_commit got commit [{}] '{}'",
                commit.id,
                commit.message
            );
            match api::local::entries::list_directory(
                repo, &commit, directory, &page_num, &page_size,
            ) {
                Ok((entries, total_entries)) => {
                    log::debug!(
                        "list_directory_for_commit commit {} got {} entries",
                        commit_id,
                        entries.len()
                    );

                    let total_pages = total_entries as f64 / page_size as f64;
                    let view = PaginatedDirEntries {
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
            log::debug!(
                "list_directory_for_commit Could not find commit with id {}",
                commit_id
            );

            Err(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!(
                "list_directory_for_commit Unable to get commit id {}. Err: {}",
                commit_id,
                err
            );
            Err(StatusMessage::internal_server_error())
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
    use liboxen::view::{PaginatedEntries, RemoteEntryResponse};

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    #[test]
    fn test_parse_resource_for_commit() -> Result<(), OxenError> {
        liboxen::test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let commit = history.first().unwrap();
            let path_str = format!("{}/annotations/train/one_shot.txt", commit.id);
            let path = Path::new(&path_str);

            match controllers::entries::parse_resource(&repo, path) {
                Ok(Some((commit_id, path))) => {
                    assert_eq!(commit.id, commit_id);
                    assert_eq!(path, Path::new("annotations/train/one_shot.txt"));
                }
                _ => {
                    panic!("Should return a commit");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_for_branch() -> Result<(), OxenError> {
        liboxen::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "my-branch";
            let branch = command::create_checkout_branch(&repo, branch_name)?;

            let path_str = format!("{}/annotations/train/one_shot.txt", branch_name);
            let path = Path::new(&path_str);

            match controllers::entries::parse_resource(&repo, path) {
                Ok(Some((commit_id, path))) => {
                    println!("Got branch: {:?} -> {:?}", branch, path);
                    assert_eq!(branch.commit_id, commit_id);
                    assert_eq!(path, Path::new("annotations/train/one_shot.txt"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_for_long_branch_name() -> Result<(), OxenError> {
        liboxen::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "my/crazy/branch/name";
            let branch = command::create_checkout_branch(&repo, branch_name)?;

            let path_str = format!("{}/annotations/train/one_shot.txt", branch_name);
            let path = Path::new(&path_str);

            match controllers::entries::parse_resource(&repo, path) {
                Ok(Some((commit_id, path))) => {
                    println!("Got branch: {:?} -> {:?}", branch, path);
                    assert_eq!(branch.commit_id, commit_id);
                    assert_eq!(path, Path::new("annotations/train/one_shot.txt"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
    }

    #[actix_web::test]
    async fn test_entries_create_text_file() -> Result<(), OxenError> {
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
    async fn test_entries_controller_list_entries() -> Result<(), OxenError> {
        liboxen::test::init_test_env();

        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        // write files to dir
        liboxen::test::populate_dir_with_training_data(&repo.path)?;

        // add the full dir
        let train_dir = repo.path.join(Path::new("train"));
        let num_entries = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;

        // commit the changes
        let commit = command::commit(&repo, "adding training dir")?.expect("Could not commit data");

        // Use the api list the files from the commit
        let uri = format!("/oxen/{}/{}/commits/{}/entries", namespace, name, commit.id);
        println!("Hit uri {}", uri);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData {
                    path: sync_dir.clone(),
                })
                .route(
                    "/oxen/{namespace}/{repo_name}/commits/{commit_id}/entries",
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
