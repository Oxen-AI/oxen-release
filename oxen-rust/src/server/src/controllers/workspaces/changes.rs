use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param, PageNumQuery};

use liboxen::view::remote_staged_status::RemoteStagedStatus;
use liboxen::view::{RemoteStagedStatusResponse, StatusMessage};
use liboxen::{constants, core::index};

use actix_web::{web, HttpRequest, HttpResponse};

use std::path::PathBuf;

pub async fn list(
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
