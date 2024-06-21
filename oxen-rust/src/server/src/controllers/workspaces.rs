use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{
    app_data, df_opts_query, parse_resource, path_param, DFOptsQuery, PageNumQuery,
};

use actix_files::NamedFile;

use liboxen::constants::TABLE_NAME;
use liboxen::core::cache::commit_cacher;
use liboxen::core::db::{df_db, staged_df_db};
use liboxen::error::OxenError;
use liboxen::model::diff::DiffResult;

use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::{Commit, LocalRepository, NewCommitBody, Schema};
use liboxen::opts::DFOpts;
use liboxen::util;
use liboxen::view::compare::{CompareTabular, CompareTabularResponseWithDF};
use liboxen::view::entry::ResourceVersion;
use liboxen::view::remote_staged_status::RemoteStagedStatus;
use liboxen::view::{
    CommitResponse, FilePathsResponse, JsonDataFrameViewResponse, JsonDataFrameViews,
    RemoteStagedStatusResponse, StatusMessage, WorkspaceResponseView, WorkspaceView,
};
use liboxen::{api, constants, core::index};

use actix_web::{web, HttpRequest, HttpResponse};

use actix_multipart::Multipart;
use actix_web::Error;
use futures_util::TryStreamExt as _;
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub mod data_frames;

pub async fn get_or_create(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let data: Result<WorkspaceView, serde_json::Error> = serde_json::from_str(&body);
    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!("Unable to parse body. Err: {}\n{}", err, body);
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    let Some(branch) = api::local::branches::get_by_name(&repo, &data.branch_name)? else {
        return Ok(HttpResponse::BadRequest().json(StatusMessage::error("Branch not found")));
    };

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

    let identifier = data.identifier.clone();

    // Get or create the workspace
    let _workspace = index::workspaces::init_or_get(&repo, &commit, &identifier)?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: data,
    }))
}

pub async fn status_dir(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let page_num = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );

    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

    get_workspace_dir_status(
        &repo,
        &commit,
        &workspace_id,
        &resource.path,
        page_num,
        page_size,
    )
}

pub async fn diff_file(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    // Need resource to have a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    // Get the workspace
    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
    let _workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;

    let diff_result =
        index::workspaces::data_frames::diff(&repo, &commit, &workspace_id, resource.path.clone())?;
    let diff = match diff_result {
        DiffResult::Tabular(diff) => diff,
        _ => {
            return Err(OxenHttpError::BadRequest(
                "Expected tabular diff result".into(),
            ))
        }
    };
    // TODO expensive clone
    let diff_df = diff.contents.clone();
    let diff_view = CompareTabular::from(diff);

    // TODO: Oxen schema vs polars inferred schema

    let diff_schema = Schema::from_polars(&diff_df.schema().clone());

    let opts = DFOpts::empty();
    let diff_json_df = JsonDataFrameViews::from_df_and_opts(diff_df, diff_schema, &opts);

    let response = CompareTabularResponseWithDF {
        data: diff_json_df,
        dfs: diff_view,
        status: StatusMessage::resource_found(),
        messages: vec![],
    };

    // The path to the actual file is just the working directory here...

    Ok(HttpResponse::Ok().json(response))
}

pub async fn diff_df(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    // Remote staged calls must be on a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
    let _workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;

    let staged_db_path =
        index::workspaces::data_frames::mods_db_path(&repo, &commit, &workspace_id, &resource.path);

    let conn = df_db::get_connection(staged_db_path)?;

    let diff_df = staged_df_db::df_diff(&conn)?;

    let df_schema = df_db::get_schema(&conn, TABLE_NAME)?;

    let df_views = JsonDataFrameViews::from_df_and_opts(diff_df, df_schema, &opts);

    let resource = ResourceVersion {
        path: resource.path.to_string_lossy().to_string(),
        version: resource.version.to_string_lossy().to_string(),
    };

    let resource = JsonDataFrameViewResponse {
        data_frame: df_views,
        status: StatusMessage::resource_found(),
        resource: Some(resource),
        commit: None,
        derived_resource: None,
    };

    Ok(HttpResponse::Ok().json(resource))
}

pub async fn get_file(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> Result<NamedFile, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let workspace_id = path_param(&req, "workspace_id")?;

    // Remote staged calls must be on a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
    let branch_repo = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;

    // The path in a workspace context is just the working path of the workspace repo
    let path = branch_repo.path.join(resource.path);

    log::debug!("got staged file path {:?}", path);

    let img_resize = query.into_inner();

    if img_resize.width.is_some() || img_resize.height.is_some() {
        let resized_path = util::fs::resized_path_for_staged_entry(
            repo,
            &path,
            img_resize.width,
            img_resize.height,
        )?;

        util::fs::resize_cache_image(&path, &resized_path, img_resize)?;
        return Ok(NamedFile::open(resized_path)?);
    }
    Ok(NamedFile::open(path)?)
}

async fn save_parts(
    repo: &LocalRepository,
    commit: &Commit,
    user_id: &str,
    directory: &Path,
    mut payload: Multipart,
) -> Result<Vec<PathBuf>, Error> {
    let mut files: Vec<PathBuf> = vec![];

    // iterate over multipart stream
    while let Some(mut field) = payload.try_next().await? {
        // A multipart/form-data stream has to contain `content_disposition`
        let content_disposition = field.content_disposition();

        log::debug!(
            "stager::save_file content_disposition.get_name() {:?}",
            content_disposition.get_name()
        );

        // Filter to process only fields with the name "file[]" or "file"
        // (the old client is sending "file" instead of "file[]", but "file[]" makes sense for more than 1 file)
        if let Some(name) = content_disposition.get_name() {
            if "file[]" == name || "file" == name {
                let upload_filename = content_disposition
                    .get_filename()
                    .map_or_else(|| Uuid::new_v4().to_string(), sanitize_filename::sanitize);

                log::debug!("Got uploaded file name: {upload_filename:?}");

                let staging_dir = index::workspaces::workspace_dir(repo, commit, user_id);
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
        }
    }

    Ok(files)
}

fn get_content_type(req: &HttpRequest) -> Option<&str> {
    req.headers().get("content-type")?.to_str().ok()
}

pub async fn add_file(req: HttpRequest, payload: Multipart) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let user_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    log::debug!("stager::stage repo name {repo_name} -> {:?}", resource);

    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
    let workspace = index::workspaces::init_or_get(&repo, &commit, &user_id)?;
    log::debug!(
        "stager::stage file repo {resource} -> staged repo path {:?}",
        repo.path
    );

    let files = save_parts(&repo, &commit, &user_id, &resource.path, payload).await?;
    let mut ret_files = vec![];

    for file in files.iter() {
        log::debug!("stager::stage file {:?}", file);
        let path = index::workspaces::files::add(&repo, &workspace, &commit, &user_id, file)?;
        log::debug!("stager::stage ✅ success! staged file {:?}", path);
        ret_files.push(path);
    }
    Ok(HttpResponse::Ok().json(FilePathsResponse {
        status: StatusMessage::resource_created(),
        paths: ret_files,
    }))
}

pub async fn commit(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    log::debug!("stager::commit {namespace}/{repo_name} on branch {} with id {} for resource {} got body: {}", branch.name, workspace_id, resource.path.to_string_lossy(), body);

    let data: Result<NewCommitBody, serde_json::Error> = serde_json::from_str(&body);

    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!("unable to parse commit data. Err: {}\n{}", err, body);
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
    let workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;
    match index::workspaces::commit(
        &repo,
        &workspace,
        &commit,
        &workspace_id,
        &data,
        &branch.name,
    ) {
        Ok(commit) => {
            log::debug!("stager::commit ✅ success! commit {:?}", commit);

            // Clone the commit so we can move it into the thread
            let ret_commit = commit.clone();

            // Start computing data about the commit in the background thread
            // std::thread::spawn(move || {
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
            // });

            Ok(HttpResponse::Ok().json(CommitResponse {
                status: StatusMessage::resource_created(),
                commit: ret_commit,
            }))
        }
        Err(err) => {
            log::error!("unable to commit branch {:?}. Err: {}", branch.name, err);
            Ok(HttpResponse::UnprocessableEntity().json(StatusMessage::error(format!("{err:?}"))))
        }
    }
}

pub async fn clear_modifications(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;
    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

    clear_staged_modifications_on_workspace(&repo, &commit, &workspace_id, &resource.path)
}

pub async fn delete_file(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let user_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    // Staging calls must be on a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    // Get commit for branch head
    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?
        .ok_or(OxenError::resource_not_found(branch.commit_id.clone()))?;

    log::debug!(
        "stager::delete_file repo name {repo_name}/{}",
        resource.path.to_string_lossy()
    );

    // This may not be in the commit if it's added, so have to parse tabular-ness from the path.
    // TODO: can we find the file / check if it's in the staging area?

    if util::fs::is_tabular(&resource.path) {
        index::workspaces::data_frames::restore(&repo, &commit, &user_id, &resource.path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        log::debug!("not tabular");
        delete_staged_file_on_branch(&repo, &commit, &user_id, &resource.path)
    }
}

fn clear_staged_modifications_on_workspace(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: &Path,
) -> Result<HttpResponse, OxenHttpError> {
    index::workspaces::init_or_get(repo, commit, workspace_id).unwrap();
    match index::workspaces::data_frames::restore(repo, commit, workspace_id, path) {
        Ok(_) => {
            log::debug!("clear_staged_modifications_on_workspace success!");
            Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
        }
        Err(err) => {
            log::error!("unable to delete file {:?}. Err: {}", path, err);
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

fn delete_staged_file_on_branch(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: &Path,
) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("delete_staged_file_on_branch()");

    let workspace = index::workspaces::init_or_get(repo, commit, workspace_id).unwrap();
    log::debug!("got workspace");
    match index::workspaces::files::has_file(&workspace, path) {
        Ok(true) => {
            match index::workspaces::files::delete_file(&workspace, path) {
                Ok(_) => {
                    log::debug!("stager::delete_file success!");
                    Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
                }
                Err(err) => {
                    log::error!("unable to delete file {:?}. Err: {}", path, err);
                    Ok(HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()))
                }
            }
        }
        Ok(false) => {
            log::error!("unable to find file {:?}", path);
            Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
        }
        Err(err) => {
            log::error!("Error getting file by path {path:?} -> {err}");
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

fn get_workspace_dir_status(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: &Path,
    page_num: usize,
    page_size: usize,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let workspace = index::workspaces::init_or_get(repo, commit, workspace_id)?;

    log::debug!("GOT WORKSPACE {:?} and DIR {:?}", repo.path, path);
    let staged = index::workspaces::stager::status(repo, &workspace, commit, workspace_id, path)?;

    staged.print_stdout();
    let full_path = index::workspaces::workspace_dir(repo, commit, workspace_id);
    let workspace = LocalRepository::new(&full_path).unwrap();

    let response = RemoteStagedStatusResponse {
        status: StatusMessage::resource_found(),
        staged: RemoteStagedStatus::from_staged(&workspace, &staged, page_num, page_size),
    };
    Ok(HttpResponse::Ok().json(response))
}
