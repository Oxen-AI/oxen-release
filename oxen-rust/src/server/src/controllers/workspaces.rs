use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use liboxen::core::cache::commit_cacher;

use liboxen::model::NewCommitBody;
use liboxen::view::workspaces::{ListWorkspaceResponseView, NewWorkspace, WorkspaceResponse};
use liboxen::view::{CommitResponse, StatusMessage, WorkspaceResponseView};
use liboxen::{api, core::index};

use actix_web::{HttpRequest, HttpResponse};

pub mod changes;
pub mod data_frames;
pub mod files;

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

    // Return workspace if it already exists
    let workspace_id = data.workspace_id.clone();
    log::debug!("get_or_create workspace_id {:?}", workspace_id);
    if let Ok(workspace) = index::workspaces::get(&repo, &workspace_id) {
        return Ok(HttpResponse::Ok().json(WorkspaceResponseView {
            status: StatusMessage::resource_created(),
            workspace: WorkspaceResponse {
                workspace_id,
                commit: workspace.commit,
            },
        }));
    }

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

    // Get or create the workspace
    index::workspaces::create(&repo, &commit, &workspace_id)?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: WorkspaceResponse {
            workspace_id,
            commit,
        },
    }))
}

pub async fn list(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    log::debug!("workspaces::list got repo: {:?}", repo.path);
    let workspaces = index::workspaces::list(&repo)?;
    let workspace_views = workspaces
        .iter()
        .map(|workspace| WorkspaceResponse {
            workspace_id: workspace.id.clone(),
            commit: workspace.commit.clone(),
        })
        .collect();

    Ok(HttpResponse::Ok().json(ListWorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspaces: workspace_views,
    }))
}

pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let workspace = index::workspaces::get(&repo, &workspace_id)?;

    index::workspaces::delete(&workspace)?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: WorkspaceResponse {
            workspace_id,
            commit: workspace.commit,
        },
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
        "workspace::commit {namespace}/{repo_name} workspace id {} to branch {} got body: {}",
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
            log::debug!("workspace::commit ✅ success! commit {:?}", commit);

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
