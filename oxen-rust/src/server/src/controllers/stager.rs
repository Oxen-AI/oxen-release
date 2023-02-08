use crate::app_data::OxenAppData;

use liboxen::compute::commit_cacher;
use liboxen::error::OxenError;
use liboxen::model::entry::mod_entry::ModType;
use liboxen::model::{Branch, DirEntry, LocalRepository, User};
use liboxen::view::entry::ResourceVersion;
use liboxen::view::http::{MSG_RESOURCE_CREATED, MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::remote_staged_status::{
    ListStagedFileModResponse, RemoteStagedStatus, StagedFileModResponse,
};
use liboxen::view::{
    CommitResponse, FilePathsResponse, PaginatedDirEntries, RemoteStagedStatusResponse,
    StatusMessage,
};
use liboxen::{api, index, util};

use actix_web::{web, web::Bytes, HttpRequest, HttpResponse};
use std::io::Write;

use actix_multipart::Multipart;
use actix_web::Error;
use futures_util::TryStreamExt as _;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct CommitBody {
    message: String,
    user: User,
}

pub async fn status_dir(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!("stager::status repo name {repo_name}/{:?}", resource);
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match util::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, _))) => get_dir_status_for_branch(&repo, &branch_name),
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

pub async fn status_file(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!("stager::status repo name {repo_name}/{:?}", resource);
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match util::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, file_name))) => {
                get_file_status_for_branch(&repo, &branch_name, &file_name)
            }
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
        let upload_extension = upload_filename.split('.').last().unwrap_or("");

        let staging_dir = index::remote_dir_stager::branch_staging_dir(repo, branch);
        let uuid = Uuid::new_v4();
        let filename = format!("{uuid}.{upload_extension}");
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

pub async fn stage_append_to_file(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    let data = String::from_utf8(bytes.to_vec()).expect("Could not parse bytes as utf8");
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => {
            match util::resource::parse_resource(&repo, &resource) {
                Ok(Some((_, branch_name, file_name))) => {
                    match api::local::branches::get_by_name(&repo, &branch_name) {
                        Ok(Some(branch)) => {
                            log::debug!(
                            "stager::stage_append_to_file file branch_name [{}] file_name [{:?}]",
                            branch_name,
                            file_name
                        );
                            match liboxen::index::mod_stager::create_mod(
                                &repo,
                                &branch,
                                &file_name,
                                ModType::Append, // TODO: support modify, delete
                                data,
                            ) {
                                Ok(entry) => Ok(HttpResponse::Ok().json(StagedFileModResponse {
                                    status: String::from(STATUS_SUCCESS),
                                    status_message: String::from(MSG_RESOURCE_CREATED),
                                    modification: entry,
                                })),
                                Err(OxenError::Basic(err)) => {
                                    log::error!(
                                        "unable to append data to file {:?}/{:?}. Err: {}",
                                        branch_name,
                                        file_name,
                                        err
                                    );
                                    Ok(HttpResponse::BadRequest()
                                        .json(StatusMessage::error(&err)))
                                }
                                Err(err) => {
                                    log::error!(
                                        "unable to append data to file {:?}/{:?}. Err: {}",
                                        branch_name,
                                        file_name,
                                        err
                                    );
                                    Ok(HttpResponse::BadRequest()
                                        .json(StatusMessage::error(&format!("{err:?}"))))
                                }
                            }
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
                    Ok(HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()))
                }
            }
        }
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

pub async fn stage_into_dir(req: HttpRequest, payload: Multipart) -> Result<HttpResponse, Error> {
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

                        let branch_repo =
                            index::remote_dir_stager::init_or_get(&repo, &branch).unwrap();
                        let files = save_parts(&repo, &branch, &directory, payload).await?;
                        for file in files.iter() {
                            log::debug!("stager::stage file {:?}", file);
                            match index::remote_dir_stager::stage_file(
                                &repo,
                                &branch_repo,
                                &branch,
                                file,
                            ) {
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

pub async fn commit(req: HttpRequest, data: web::Json<CommitBody>) -> Result<HttpResponse, Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let branch_name: &str = req.match_info().query("branch");

    log::debug!("stager::commit repo name {repo_name} -> {branch_name}");
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match api::local::branches::get_by_name(&repo, branch_name) {
            Ok(Some(branch)) => {
                let branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch).unwrap();
                match index::remote_dir_stager::commit_staged(
                    &repo,
                    &branch_repo,
                    &branch,
                    &data.user,
                    &data.message,
                ) {
                    Ok(commit) => {
                        log::debug!("stager::commit ✅ success! commit {:?}", commit);

                        // Clone the commit so we can move it into the thread
                        let ret_commit = commit.clone();

                        // Start computing data about the commit in the background thread
                        std::thread::spawn(move || {
                            log::debug!("Processing commit {:?} on repo {:?}", commit, repo.path);
                            match commit_cacher::run_all(&repo, &commit) {
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

                        Ok(HttpResponse::Ok().json(CommitResponse {
                            status: String::from(STATUS_SUCCESS),
                            status_message: String::from(MSG_RESOURCE_CREATED),
                            commit: ret_commit,
                        }))
                    }
                    Err(err) => {
                        log::error!("unable to commit branch {:?}. Err: {}", branch_name, err);
                        Ok(HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error()))
                    }
                }
            }
            Ok(None) => {
                log::debug!("unable to find branch {}", branch_name);
                Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
            }
            Err(err) => {
                log::error!("Could not commit staged: {:?}", err);
                Ok(
                    HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()),
                )
            }
        },
        Ok(None) => {
            log::debug!("unable to find repo {}", repo_name);
            Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
        }
        Err(err) => {
            log::error!("Could not commit staged: {:?}", err);
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

fn get_file_status_for_branch(
    repo: &LocalRepository,
    branch_name: &str,
    path: &Path,
) -> HttpResponse {
    match api::local::branches::get_by_name(repo, branch_name) {
        Ok(Some(branch)) => match api::local::commits::get_by_id(repo, &branch.commit_id) {
            Ok(Some(commit)) => {
                match api::local::entries::get_entry_for_commit(repo, &commit, path) {
                    Ok(Some(entry)) => match index::mod_stager::list_mods(repo, &branch, &entry) {
                        Ok(staged) => {
                            let response = ListStagedFileModResponse {
                                status: String::from(STATUS_SUCCESS),
                                status_message: String::from(MSG_RESOURCE_FOUND),
                                modifications: staged,
                            };
                            HttpResponse::Ok().json(response)
                        }
                        Err(err) => {
                            log::error!(
                                "unable to get list staged data {:?}. Err: {}",
                                branch_name,
                                err
                            );
                            HttpResponse::InternalServerError()
                                .json(StatusMessage::internal_server_error())
                        }
                    },
                    Ok(None) => {
                        log::error!("unable to find entry {:?}", path);
                        HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                    }
                    Err(err) => {
                        log::error!("Error getting entry by path {:?} -> {err}", path);
                        HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error())
                    }
                }
            }
            Ok(None) => {
                log::error!("unable to find commit {}", branch.commit_id);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::error!("Error getting commit by id {} -> {err}", branch.commit_id);
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

fn get_dir_status_for_branch(repo: &LocalRepository, branch_name: &str) -> HttpResponse {
    match api::local::branches::get_by_name(repo, branch_name) {
        Ok(Some(branch)) => match index::remote_dir_stager::list_staged_data(repo, &branch) {
            Ok(staged) => {
                staged.print_stdout();
                log::debug!("GOT {} ADDED FILES", staged.added_files.len());
                let entries: Vec<DirEntry> = staged
                    .added_files
                    .keys()
                    .map(|path| {
                        let full_path =
                            index::remote_dir_stager::branch_staging_dir(repo, &branch).join(path);
                        let meta = std::fs::metadata(&full_path).unwrap();
                        let path_str = path.to_string_lossy().to_string();

                        DirEntry {
                            filename: path_str.to_owned(),
                            is_dir: false,
                            size: meta.len(),
                            latest_commit: None,
                            datatype: util::fs::file_datatype(&full_path),
                            resource: ResourceVersion {
                                path: path_str,
                                version: branch_name.to_owned(),
                            },
                        }
                    })
                    .collect();
                let response = RemoteStagedStatusResponse {
                    status: STATUS_SUCCESS.to_string(),
                    status_message: MSG_RESOURCE_FOUND.to_string(),
                    staged: RemoteStagedStatus {
                        added_files: PaginatedDirEntries {
                            entries,
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
