use crate::app_data::OxenAppData;
use crate::params::df_opts_query::{self, DFOptsQuery};

use liboxen::compute::commit_cacher;
use liboxen::df::{tabular, DFOpts};
use liboxen::error::OxenError;
use liboxen::model::entry::mod_entry::ModType;
use liboxen::model::{Branch, CommitBody, CommitEntry, LocalRepository, Schema};
use liboxen::view::http::{MSG_RESOURCE_CREATED, MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::json_data_frame::JsonDataSize;
use liboxen::view::remote_staged_status::{
    ListStagedFileModResponseDF, ListStagedFileModResponseRaw, RemoteStagedStatus,
    StagedDFModifications, StagedFileModResponse,
};
use liboxen::view::{
    CommitResponse, FilePathsResponse, JsonDataFrame, RemoteStagedStatusResponse, StatusMessage,
};
use liboxen::{api, constants, index, util};

use actix_web::{web, web::Bytes, HttpRequest, HttpResponse};
use std::io::Write;

use actix_multipart::Multipart;
use actix_web::Error;
use futures_util::TryStreamExt as _;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use super::entries::PageNumQuery;

enum ModResponseFormat {
    Raw,
    DataFrame,
}

pub async fn status_dir(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();
    let page_num = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    log::debug!(
        "stager::status repo name {repo_name}/{}",
        resource.to_string_lossy()
    );

    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match util::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, directory))) => {
                get_dir_status_for_branch(&repo, &branch_name, &directory, page_num, page_size)
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

pub async fn status_file(req: HttpRequest, query: web::Query<DFOptsQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!(
        "stager::status repo name {repo_name}/{}",
        resource.to_string_lossy()
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match util::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, file_name))) => get_file_status_for_branch(
                &repo,
                &branch_name,
                &file_name,
                ModResponseFormat::Raw,
                query,
            ),
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

pub async fn diff_file(req: HttpRequest, query: web::Query<DFOptsQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!(
        "stager::status repo name {repo_name}/{}",
        resource.to_string_lossy()
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match util::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, file_name))) => get_file_status_for_branch(
                &repo,
                &branch_name,
                &file_name,
                ModResponseFormat::DataFrame,
                query,
            ),
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

        log::debug!("Got uploaded file name: {upload_filename:?}");

        let staging_dir = index::remote_dir_stager::branch_staging_dir(repo, branch);
        let full_dir = staging_dir.join(directory);

        if !full_dir.exists() {
            std::fs::create_dir_all(&full_dir)?;
        }

        let filepath = full_dir.join(&upload_filename);
        let filepath_cpy = full_dir.join(&upload_filename);
        log::debug!("stager::save_file writing file to {:?}", filepath);

        // File::create is blocking operation, use threadpool
        let mut f = web::block(|| std::fs::File::create(filepath)).await??;

        // Field in turn is stream of *Bytes* object
        while let Some(chunk) = field.try_next().await? {
            // filesystem operations are blocking, we have to use threadpool
            f = web::block(move || f.write_all(&chunk).map(|_| f)).await??;
        }
        files.push(filepath_cpy);
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
                            index::remote_dir_stager::init_or_get(&repo, &branch).unwrap();
                            create_mod(&repo, &branch, &file_name, data)
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

fn create_mod(
    repo: &LocalRepository,
    branch: &Branch,
    file: &Path,
    data: String,
) -> Result<HttpResponse, Error> {
    match liboxen::index::mod_stager::create_mod(
        repo,
        branch,
        file,
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
                branch.name,
                file,
                err
            );
            Ok(HttpResponse::BadRequest().json(StatusMessage::error(&err)))
        }
        Err(err) => {
            log::error!(
                "unable to append data to file {:?}/{:?}. Err: {}",
                branch.name,
                file,
                err
            );
            Ok(HttpResponse::BadRequest().json(StatusMessage::error(&format!("{err:?}"))))
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
                        let mut ret_files = vec![];
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
                                    ret_files.push(file_path);
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
                            paths: ret_files,
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

pub async fn commit(req: HttpRequest, body: String) -> Result<HttpResponse, Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let branch_name: &str = req.match_info().query("branch");

    log::debug!("stager::commit got body: {body}");

    let data: Result<CommitBody, serde_json::Error> = serde_json::from_str(&body);

    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!("unable to parse commit data. Err: {}\n{}", err, body);
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(&err.to_string())));
        }
    };

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

pub async fn delete_file(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!(
        "stager::delete_file repo name {repo_name}/{}",
        resource.to_string_lossy()
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match util::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, file_name))) => {
                delete_staged_file_on_branch(&repo, &branch_name, &file_name)
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

fn delete_staged_file_on_branch(
    repo: &LocalRepository,
    branch_name: &str,
    path: &Path,
) -> HttpResponse {
    match api::local::branches::get_by_name(repo, branch_name) {
        Ok(Some(branch)) => {
            let branch_repo = index::remote_dir_stager::init_or_get(repo, &branch).unwrap();
            match index::remote_dir_stager::has_file(&branch_repo, path) {
                Ok(true) => match index::remote_dir_stager::delete_file(&branch_repo, path) {
                    Ok(_) => {
                        log::debug!("stager::delete_file success!");
                        HttpResponse::Ok().json(StatusMessage::resource_deleted())
                    }
                    Err(err) => {
                        log::error!("unable to delete file {:?}. Err: {}", path, err);
                        HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error())
                    }
                },
                Ok(false) => {
                    log::error!("unable to find file {:?}", path);
                    HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                }
                Err(err) => {
                    log::error!("Error getting file by path {path:?} -> {err}");
                    HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
                }
            }
        }
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

fn get_file_status_for_branch(
    repo: &LocalRepository,
    branch_name: &str,
    path: &Path,
    format: ModResponseFormat,
    query: web::Query<DFOptsQuery>,
) -> HttpResponse {
    let page_num = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    match api::local::branches::get_by_name(repo, branch_name) {
        Ok(Some(branch)) => match api::local::commits::get_by_id(repo, &branch.commit_id) {
            Ok(Some(commit)) => {
                match api::local::entries::get_entry_for_commit(repo, &commit, path) {
                    Ok(Some(entry)) => match format {
                        ModResponseFormat::Raw => {
                            raw_mods_response(repo, &branch, &entry, page_num, page_size)
                        }
                        ModResponseFormat::DataFrame => {
                            df_mods_response(repo, &branch, &entry, query)
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

fn df_mods_response(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
    query: web::Query<DFOptsQuery>,
) -> HttpResponse {
    match index::mod_stager::list_mods_df(repo, branch, entry) {
        Ok(diff) => {
            let df = if let Some(added) = diff.added_rows {
                let og_size = JsonDataSize {
                    width: added.width(),
                    height: added.height(),
                };
                log::debug!("added rows: {:?}", added);

                let polars_schema = added.schema();
                let schema = Schema::from_polars(&polars_schema);

                let mut filter = DFOpts::from_schema_columns_exclude_hidden(&schema);
                log::debug!("Initial filter {:?}", filter);
                filter = df_opts_query::parse_opts(&query, &mut filter);
                let mut df = tabular::transform(added, filter).unwrap();

                let df = JsonDataFrame::from_slice(&mut df, og_size);
                Some(df)
            } else {
                log::debug!("No added rows for entry {entry:?}");
                None
            };

            let response = ListStagedFileModResponseDF {
                status: String::from(STATUS_SUCCESS),
                status_message: String::from(MSG_RESOURCE_FOUND),
                data_type: String::from("tabular"),
                modifications: StagedDFModifications { added: df },
            };

            HttpResponse::Ok().json(response)
        }
        Err(err) => {
            log::error!(
                "unable to get list staged data {:?}. Err: {}",
                branch.name,
                err
            );
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

fn raw_mods_response(
    repo: &LocalRepository,
    branch: &Branch,
    entry: &CommitEntry,
    page_num: usize,
    page_size: usize,
) -> HttpResponse {
    match index::mod_stager::list_mods_raw(repo, branch, entry) {
        Ok(staged) => {
            let total_entries = staged.len();
            let total_pages = (total_entries / page_size) + 1;
            let paginated = util::paginate(staged, page_num, page_size);
            let response = ListStagedFileModResponseRaw {
                status: String::from(STATUS_SUCCESS),
                status_message: String::from(MSG_RESOURCE_FOUND),
                data_type: String::from("text"),
                modifications: paginated,
                page_size,
                page_number: page_num,
                total_pages,
                total_entries,
            };
            HttpResponse::Ok().json(response)
        }
        Err(err) => {
            log::error!(
                "unable to get list staged data {:?}. Err: {}",
                branch.name,
                err
            );
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

fn get_dir_status_for_branch(
    repo: &LocalRepository,
    branch_name: &str,
    directory: &Path,
    page_num: usize,
    page_size: usize,
) -> HttpResponse {
    match api::local::branches::get_by_name(repo, branch_name) {
        Ok(Some(branch)) => {
            let branch_repo = match index::remote_dir_stager::init_or_get(repo, &branch) {
                Ok(repo) => repo,
                Err(err) => {
                    log::error!("Error getting branch repo for branch {:?} -> {err}", branch);
                    return HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error());
                }
            };
            log::debug!(
                "GOT BRANCH REPO {:?} and DIR {:?}",
                branch_repo.path,
                directory
            );
            match index::remote_dir_stager::list_staged_data(repo, &branch_repo, &branch, directory)
            {
                Ok(staged) => {
                    staged.print_stdout();
                    let full_path = index::remote_dir_stager::branch_staging_dir(repo, &branch);
                    let branch_repo = LocalRepository::new(&full_path).unwrap();

                    let response = RemoteStagedStatusResponse {
                        status: STATUS_SUCCESS.to_string(),
                        status_message: MSG_RESOURCE_FOUND.to_string(),
                        staged: RemoteStagedStatus::from_staged(
                            &branch_repo,
                            &staged,
                            page_num,
                            page_size,
                        ),
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
            }
        }
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
