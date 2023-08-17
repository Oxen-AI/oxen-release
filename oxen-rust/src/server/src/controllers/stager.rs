use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{
    app_data, df_opts_query, parse_resource, path_param, DFOptsQuery, PageNumQuery,
};

use liboxen::core::cache::commit_cacher;
use liboxen::core::df::tabular;
use liboxen::core::index::mod_stager;
use liboxen::error::OxenError;
use liboxen::model::entry::mod_entry::NewMod;
use liboxen::model::DataFrameSize;
use liboxen::model::{
    entry::mod_entry::ModType, Branch, CommitEntry, ContentType, LocalRepository, NewCommitBody,
    ObjectID, Schema,
};
use liboxen::opts::DFOpts;
use liboxen::view::remote_staged_status::{
    ListStagedFileModResponseDF, RemoteStagedStatus, StagedDFModifications, StagedFileModResponse,
};
use liboxen::view::{
    CommitResponse, FilePathsResponse, JsonDataFrame, RemoteStagedStatusResponse, StatusMessage,
};
use liboxen::{api, constants, core::index};

use actix_web::{web, web::Bytes, HttpRequest, HttpResponse};
use std::io::Write;

use actix_multipart::Multipart;
use actix_web::Error;
use futures_util::TryStreamExt as _;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub async fn status_dir(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let identifier = path_param(&req, "identifier")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let page_num = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );

    get_dir_status_for_branch(
        &repo,
        &resource.branch.ok_or(OxenHttpError::NotFound)?.name,
        &identifier,
        &resource.file_path,
        page_num,
        page_size,
    )
}

pub async fn diff_file(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let identifier = path_param(&req, "identifier")?;

    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );

    let entry =
        api::local::entries::get_commit_entry(&repo, &resource.commit, &resource.file_path)?
            .ok_or(OxenHttpError::NotFound)?;

    Ok(df_mods_response(
        &repo,
        &resource
            .branch
            .to_owned()
            .ok_or(OxenError::parsed_resource_not_found(resource))?,
        &identifier,
        &entry,
        query,
    ))
}

async fn save_parts(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
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

        let staging_dir = index::remote_dir_stager::branch_staging_dir(repo, branch, user_id);
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

fn get_content_type(req: &HttpRequest) -> Option<&str> {
    req.headers().get("content-type")?.to_str().ok()
}

pub async fn df_add_row(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let identifier = path_param(&req, "identifier")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    // TODO: better error handling for content-types
    let content_type_str = get_content_type(&req).unwrap_or("text/plain");
    let content_type = ContentType::from_http_content_type(content_type_str)?;
    let data = String::from_utf8(bytes.to_vec()).expect("Could not parse bytes as utf8");

    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    // Have to initialize this branch repo before we can do any operations on it
    index::remote_dir_stager::init_or_get(&repo, &branch, &identifier)?;
    log::debug!(
        "stager::df_add_row repo {resource} -> staged repo path {:?}",
        repo.path
    );

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.ok_or(
        OxenError::revision_not_found(branch.commit_id.to_owned().into()),
    )?;

    let entry = api::local::entries::get_commit_entry(&repo, &commit, &resource.file_path)?
        .ok_or(OxenError::entry_does_not_exist(resource.file_path))?;

    let new_mod = NewMod {
        content_type,
        mod_type: ModType::Append,
        entry,
        data,
    };

    let row = liboxen::core::index::mod_stager::create_mod(&repo, &branch, &identifier, &new_mod)?;

    Ok(HttpResponse::Ok().json(StagedFileModResponse {
        status: StatusMessage::resource_created(),
        modification: row,
    }))
}

pub async fn df_delete_row(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let user_id: &str = req.match_info().get("identifier").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    let body_err_msg = "Invalid Body, must be valid json in the format {\"id\": \"<id>\"}";
    let body = String::from_utf8(bytes.to_vec());
    if body.is_err() {
        log::error!("stager::df_delete_row could not parse body as utf8");
        return Ok(HttpResponse::BadRequest().json(StatusMessage::error(body_err_msg)));
    }

    let body = body.unwrap();
    let response: Result<ObjectID, serde_json::Error> = serde_json::from_str(&body);
    if response.is_err() {
        log::error!("stager::df_delete_row could not parse body as ObjectID\n{body:?}");
        return Ok(HttpResponse::BadRequest().json(StatusMessage::error(body_err_msg)));
    }

    // Safe to unwrap after checks above
    let uuid = response.unwrap().id;

    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => {
            match api::local::resource::parse_resource(&repo, &resource) {
                Ok(Some((_, branch_name, file_name))) => {
                    match api::local::branches::get_by_name(&repo, &branch_name) {
                        Ok(Some(branch)) => {
                            log::debug!(
                                "stager::df_delete_row file branch_name [{}] file_name [{:?}] uuid [{}]",
                                branch_name,
                                file_name,
                                uuid
                            );
                            delete_mod(&repo, &branch, user_id, &file_name, uuid)
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

fn delete_mod(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    file: &Path,
    uuid: String,
) -> Result<HttpResponse, Error> {
    match liboxen::core::index::mod_stager::delete_mod_from_path(repo, branch, user_id, file, &uuid)
    {
        Ok(entry) => Ok(HttpResponse::Ok().json(StagedFileModResponse {
            status: StatusMessage::resource_deleted(),
            modification: entry,
        })),
        Err(OxenError::Basic(err)) => {
            log::error!(
                "unable to delete data to file {:?}/{:?} uuid {}. Err: {}",
                branch.name,
                file,
                uuid,
                err
            );
            Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())))
        }
        Err(err) => {
            log::error!(
                "unable to delete data to file {:?}/{:?} uuid {}. Err: {}",
                branch.name,
                file,
                uuid,
                err
            );
            Ok(HttpResponse::BadRequest().json(StatusMessage::error(format!("{err:?}"))))
        }
    }
}

pub async fn add_file(req: HttpRequest, payload: Multipart) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let user_id = path_param(&req, "identifier")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    log::debug!("stager::stage repo name {repo_name} -> {:?}", resource);

    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch, &user_id)?;
    log::debug!(
        "stager::stage file repo {resource} -> staged repo path {:?}",
        repo.path
    );

    let files = save_parts(&repo, &branch, &user_id, &resource.file_path, payload).await?;
    let mut ret_files = vec![];

    for file in files.iter() {
        log::debug!("stager::stage file {:?}", file);
        let file_path =
            index::remote_dir_stager::stage_file(&repo, &branch_repo, &branch, &user_id, file)?;
        log::debug!("stager::stage ✅ success! staged file {:?}", file_path);
        ret_files.push(file_path);
    }
    Ok(HttpResponse::Ok().json(FilePathsResponse {
        status: StatusMessage::resource_created(),
        paths: ret_files,
    }))
}

pub async fn commit(req: HttpRequest, body: String) -> Result<HttpResponse, Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let user_id: &str = req.match_info().get("identifier").unwrap();
    let branch_name: &str = req.match_info().query("branch");

    log::debug!("stager::commit got body: {body}");

    let data: Result<NewCommitBody, serde_json::Error> = serde_json::from_str(&body);

    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!("unable to parse commit data. Err: {}\n{}", err, body);
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    log::debug!("stager::commit repo name {repo_name} -> {branch_name}");
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match api::local::branches::get_by_name(&repo, branch_name) {
            Ok(Some(branch)) => {
                let branch_repo =
                    index::remote_dir_stager::init_or_get(&repo, &branch, user_id).unwrap();
                match index::remote_dir_stager::commit_staged(
                    &repo,
                    &branch_repo,
                    &branch,
                    &data,
                    user_id,
                ) {
                    Ok(commit) => {
                        log::debug!("stager::commit ✅ success! commit {:?}", commit);

                        // Clone the commit so we can move it into the thread
                        let ret_commit = commit.clone();

                        // Start computing data about the commit in the background thread
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

                        Ok(HttpResponse::Ok().json(CommitResponse {
                            status: StatusMessage::resource_created(),
                            commit: ret_commit,
                        }))
                    }
                    Err(err) => {
                        log::error!("unable to commit branch {:?}. Err: {}", branch_name, err);
                        Ok(HttpResponse::UnprocessableEntity()
                            .json(StatusMessage::error(format!("{err:?}"))))
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

pub async fn clear_modifications(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let user_id: &str = req.match_info().get("identifier").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!(
        "stager::clear_modifications repo name {repo_name}/{}",
        resource.to_string_lossy()
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match api::local::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, file_name))) => {
                clear_staged_modifications_on_branch(&repo, &branch_name, user_id, &file_name)
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

pub async fn delete_file(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let user_id: &str = req.match_info().get("identifier").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!(
        "stager::delete_file repo name {repo_name}/{}",
        resource.to_string_lossy()
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match api::local::resource::parse_resource(&repo, &resource) {
            Ok(Some((_, branch_name, file_name))) => {
                delete_staged_file_on_branch(&repo, &branch_name, user_id, &file_name)
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

fn clear_staged_modifications_on_branch(
    repo: &LocalRepository,
    branch_name: &str,
    user_id: &str,
    path: &Path,
) -> HttpResponse {
    match api::local::branches::get_by_name(repo, branch_name) {
        Ok(Some(branch)) => {
            index::remote_dir_stager::init_or_get(repo, &branch, user_id).unwrap();
            match mod_stager::clear_mods(repo, &branch, user_id, path) {
                Ok(_) => {
                    log::debug!("clear_staged_modifications_on_branch success!");
                    HttpResponse::Ok().json(StatusMessage::resource_deleted())
                }
                Err(err) => {
                    log::error!("unable to delete file {:?}. Err: {}", path, err);
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

fn delete_staged_file_on_branch(
    repo: &LocalRepository,
    branch_name: &str,
    user_id: &str,
    path: &Path,
) -> HttpResponse {
    match api::local::branches::get_by_name(repo, branch_name) {
        Ok(Some(branch)) => {
            let branch_repo =
                index::remote_dir_stager::init_or_get(repo, &branch, user_id).unwrap();
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

fn df_mods_response(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    entry: &CommitEntry,
    query: web::Query<DFOptsQuery>,
) -> HttpResponse {
    match index::mod_stager::list_mods_df(repo, branch, user_id, entry) {
        Ok(diff) => {
            let df = if let Some(added) = diff.added_rows {
                let og_size = DataFrameSize {
                    width: added.width(),
                    height: added.height(),
                };
                log::debug!("added rows: {:?}", added);

                let polars_schema = added.schema();
                let og_schema = Schema::from_polars(&polars_schema);

                if og_size.is_empty() {
                    Some(JsonDataFrame::empty(&og_schema))
                } else {
                    let mut filter = DFOpts::from_schema_columns_exclude_hidden(&og_schema);
                    log::debug!("Initial filter {:?}", filter);
                    filter = df_opts_query::parse_opts(&query, &mut filter);
                    let mut df = tabular::transform(added, filter).unwrap();

                    let df = JsonDataFrame::from_slice(&mut df, og_schema, og_size);
                    Some(df)
                }
            } else {
                log::debug!("No added rows for entry {entry:?}");
                None
            };

            let response = ListStagedFileModResponseDF {
                status: StatusMessage::resource_found(),
                data_type: String::from("tabular"),
                modifications: StagedDFModifications { added_rows: df },
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
    user_id: &str,
    directory: &Path,
    page_num: usize,
    page_size: usize,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let branch = api::local::branches::get_by_name(repo, branch_name)?
        .ok_or(OxenError::remote_branch_not_found(branch_name))?;

    let branch_repo = index::remote_dir_stager::init_or_get(repo, &branch, user_id)?;

    log::debug!(
        "GOT BRANCH REPO {:?} and DIR {:?}",
        branch_repo.path,
        directory
    );
    let staged = index::remote_dir_stager::list_staged_data(
        repo,
        &branch_repo,
        &branch,
        user_id,
        directory,
    )?;

    staged.print_stdout();
    let full_path = index::remote_dir_stager::branch_staging_dir(repo, &branch, user_id);
    let branch_repo = LocalRepository::new(&full_path).unwrap();

    let response = RemoteStagedStatusResponse {
        status: StatusMessage::resource_found(),
        staged: RemoteStagedStatus::from_staged(&branch_repo, &staged, page_num, page_size),
    };
    Ok(HttpResponse::Ok().json(response))
}
