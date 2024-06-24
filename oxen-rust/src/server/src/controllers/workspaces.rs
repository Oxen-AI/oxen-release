use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param, PageNumQuery};

use actix_files::NamedFile;

use liboxen::core::cache::commit_cacher;

use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::{NewCommitBody, Workspace};
use liboxen::util;
use liboxen::view::remote_staged_status::RemoteStagedStatus;
use liboxen::view::workspaces::{NewWorkspace, WorkspaceResponse};
use liboxen::view::{
    CommitResponse, FilePathsResponse, RemoteStagedStatusResponse, StatusMessage,
    WorkspaceResponseView,
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

    let data: Result<NewWorkspace, serde_json::Error> = serde_json::from_str(&body);
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

    let workspace_id = data.workspace_id.clone();

    // Get or create the workspace
    index::workspaces::create(&repo, &commit, &workspace_id)?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: WorkspaceResponse {
            workspace_id,
            branch_name: branch.name,
            commit,
        },
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
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let path = PathBuf::from(path_param(&req, "path")?);
    let page_num = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    let workspace = index::workspaces::get(&repo, workspace_id)?;
    let staged = index::workspaces::stager::status(&workspace, &path)?;

    staged.print_stdout();

    let response = RemoteStagedStatusResponse {
        status: StatusMessage::resource_found(),
        staged: RemoteStagedStatus::from_staged(
            &workspace.workspace_repo,
            &staged,
            page_num,
            page_size,
        ),
    };
    Ok(HttpResponse::Ok().json(response))
}

pub async fn get_file(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> Result<NamedFile, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let workspace = index::workspaces::get(&repo, workspace_id)?;
    let path = path_param(&req, "path")?;

    // The path in a workspace context is just the working path of the workspace repo
    let path = workspace.workspace_repo.path.join(path);

    log::debug!("got workspace file path {:?}", path);

    // TODO: This probably isn't the best place for the resize logic
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
    workspace: &Workspace,
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

                let workspace_dir = workspace.dir();
                let full_dir = workspace_dir.join(directory);

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

pub async fn add_file(req: HttpRequest, payload: Multipart) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let path = PathBuf::from(path_param(&req, "path")?);

    let workspace = index::workspaces::get(&repo, &workspace_id)?;

    let files = save_parts(&workspace, &path, payload).await?;
    let mut ret_files = vec![];

    for file in files.iter() {
        log::debug!("add_file file {:?}", file);
        let path = index::workspaces::files::add(&workspace, file)?;
        log::debug!("add_file ✅ success! staged file {:?}", path);
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
    let branch_name = path_param(&req, "branch")?;

    log::debug!(
        "stager::commit {namespace}/{repo_name} workspace id {} to branch {} got body: {}",
        workspace_id,
        branch_name,
        body
    );

    let data: Result<NewCommitBody, serde_json::Error> = serde_json::from_str(&body);

    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!("unable to parse commit data. Err: {}\n{}", err, body);
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    let workspace = index::workspaces::get(&repo, &workspace_id)?;

    match index::workspaces::commit(&workspace, &data, &branch_name) {
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
            log::error!("unable to commit branch {:?}. Err: {}", branch_name, err);
            Ok(HttpResponse::UnprocessableEntity().json(StatusMessage::error(format!("{err:?}"))))
        }
    }
}

pub async fn delete_file(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let user_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let path = PathBuf::from(path_param(&req, "path")?);

    let workspace = index::workspaces::get(&repo, user_id)?;

    // This may not be in the commit if it's added, so have to parse tabular-ness from the path.
    if util::fs::is_tabular(&path) {
        index::workspaces::data_frames::restore(&workspace, &path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else if index::workspaces::files::has_file(&workspace, &path)? {
        index::workspaces::files::delete_file(&workspace, &path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
    }
}
