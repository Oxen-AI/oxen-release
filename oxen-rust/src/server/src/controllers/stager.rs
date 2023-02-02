use crate::app_data::OxenAppData;

use futures_util::stream::StreamExt as _;
use liboxen::model::{Branch, DirEntry, LocalRepository, User};
use liboxen::view::entry::ResourceVersion;
use liboxen::view::http::{MSG_RESOURCE_CREATED, STATUS_SUCCESS};
use liboxen::view::remote_staged_status::RemoteStagedStatus;
use liboxen::view::{
    FilePathsResponse, PaginatedDirEntries, RemoteStagedStatusResponse, StatusMessage,
};
use liboxen::{api, index, util};

use actix_web::{web, HttpRequest, HttpResponse};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub async fn status(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!("stager::status repo name {repo_name}/{:?}", resource);
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match util::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, _))) => get_status_for_branch(&repo, &branch_name),
            Ok(None) => {
                log::error!("unable to find resource {:?}", resource);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::error!("Could not parse resource  {repo_name} -> {err}");
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        },
        Ok(None) => {
            log::error!("unable to find repo {}", repo_name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("Error getting repo by name {repo_name} -> {err}");
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

use std::io::Write;

use actix_multipart::Multipart;
use actix_web::Error;
use futures_util::TryStreamExt as _;
use uuid::Uuid;

async fn save_parts(
    repo: &LocalRepository,
    branch: &Branch,
    directory: &Path,
    mut payload: Multipart,
) -> Result<Vec<PathBuf>, Error> {
    let mut files: Vec<PathBuf> = vec![];
    // iterate over multipart stream
    while let Some(mut field) = payload.try_next().await? {
        // A multipart/form-data stream has to contain `content_disposition`
        let content_disposition = field.content_disposition();

        let upload_filename = content_disposition
            .get_filename()
            .map_or_else(|| Uuid::new_v4().to_string(), sanitize_filename::sanitize);
        let upload_extension = upload_filename.split(".").last().unwrap_or("");

        let staging_dir = index::remote_stager::branch_staging_dir(repo, branch);
        let uuid = Uuid::new_v4();
        let filename = format!("{}.{}", uuid, upload_extension);
        let full_dir = staging_dir.join(directory);

        if !full_dir.exists() {
            std::fs::create_dir_all(&full_dir)?;
        }

        let filepath = full_dir.join(&filename);
        let relative_path = util::fs::path_relative_to_dir(&filepath, &staging_dir).unwrap();
        log::debug!("stager::save_file writing file to {:?}", filepath);

        // File::create is blocking operation, use threadpool
        let mut f = web::block(|| std::fs::File::create(filepath)).await??;

        // Field in turn is stream of *Bytes* object
        while let Some(chunk) = field.try_next().await? {
            // filesystem operations are blocking, we have to use threadpool
            f = web::block(move || f.write_all(&chunk).map(|_| f)).await??;
        }
        files.push(relative_path);
    }

    Ok(files)
}

pub async fn stage(req: HttpRequest, payload: Multipart) -> Result<HttpResponse, Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!("stager::stage repo name {repo_name} -> {:?}", resource);
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match util::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, directory))) => {
                match api::local::branches::get_by_name(&repo, &branch_name) {
                    Ok(Some(branch)) => {
                        log::debug!(
                            "stager::stage file branch_name [{}] in directory {:?}",
                            branch_name,
                            directory
                        );

                        let files = save_parts(&repo, &branch, &directory, payload).await?;

                        for file in files.iter() {
                            log::debug!("stager::stage file {:?}", file);
                            match index::remote_stager::stage_file(&repo, &branch, &file) {
                                Ok(file_path) => {
                                    log::debug!(
                                        "stager::stage ✅ success! staged file {:?}",
                                        file_path
                                    );
                                }
                                Err(err) => {
                                    log::error!("unable to stage file {:?}. Err: {}", file, err);
                                    return Ok(HttpResponse::InternalServerError()
                                        .json(StatusMessage::internal_server_error()));
                                }
                            }
                        }

                        Ok(HttpResponse::Ok().json(FilePathsResponse {
                            status: String::from(STATUS_SUCCESS),
                            status_message: String::from(MSG_RESOURCE_CREATED),
                            paths: files,
                        }))
                    }
                    Ok(None) => {
                        log::debug!("stager::stage could not find branch {:?}", branch_name);
                        Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
                    }
                    Err(err) => {
                        log::error!("unable to get branch {:?}. Err: {}", branch_name, err);
                        Ok(HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error()))
                    }
                }
            }
            Ok(None) => {
                log::debug!("stager::stage could not find parse resource {:?}", resource);
                Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
            }
            Err(err) => {
                log::error!("unable to parse resource {:?}. Err: {}", resource, err);
                Ok(
                    HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()),
                )
            }
        },
        Ok(None) => {
            log::debug!("stager::stage could not find repo with name {}", repo_name);
            Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
        }
        Err(err) => {
            log::error!("unable to get repo {:?}. Err: {}", repo_name, err);
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

pub async fn commit(req: HttpRequest) -> Result<HttpResponse, Error> {
    log::error!("TODO: implement commit");
    Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
}

fn get_status_for_branch(repo: &LocalRepository, branch_name: &str) -> HttpResponse {
    match api::local::branches::get_by_name(&repo, &branch_name) {
        Ok(Some(branch)) => match index::remote_stager::list_staged_data(&repo, &branch) {
            Ok(staged) => {
                staged.print_stdout();
                log::debug!("GOT {} ADDED FILES", staged.added_files.len());
                let entries: Vec<DirEntry> = staged
                    .added_files
                    .iter()
                    .map(|(path, _)| {
                        let full_path =
                            index::remote_stager::branch_staging_dir(&repo, &branch).join(path);
                        let meta = std::fs::metadata(&full_path).unwrap();
                        let path_str = path.to_string_lossy().to_string();

                        DirEntry {
                            filename: path_str.to_owned(),
                            is_dir: false,
                            size: meta.len(),
                            latest_commit: None,
                            datatype: util::fs::file_datatype(&full_path),
                            resource: ResourceVersion {
                                path: path_str.to_owned(),
                                version: branch_name.to_owned(),
                            },
                        }
                    })
                    .collect();
                let response = RemoteStagedStatusResponse {
                    status: STATUS_SUCCESS.to_string(),
                    status_message: MSG_RESOURCE_CREATED.to_string(),
                    staged: RemoteStagedStatus {
                        added_files: PaginatedDirEntries {
                            entries: entries,
                            page_number: 1,
                            page_size: 10,
                            total_pages: 0,
                            total_entries: 0,
                            resource: ResourceVersion {
                                path: "".to_string(),
                                version: "".to_string(),
                            },
                        },
                    },
                };
                HttpResponse::Ok().json(response)
            }
            Err(err) => {
                log::error!(
                    "Error getting staged data for branch {} {}",
                    branch_name,
                    err
                );
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        },
        Ok(None) => {
            log::error!("unable to find branch {}", branch_name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("Error getting branch by name {branch_name} -> {err}");
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}
