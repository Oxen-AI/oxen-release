use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{HttpRequest, HttpResponse};
use liboxen::repositories;
use liboxen::view::data_frames::embeddings::EmbeddingColumnsResponse;
use liboxen::view::StatusMessage;

/// Get the embedding status for a data frame
pub async fn get(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let file_path = path_param(&req, "path")?;

    let workspace = repositories::workspaces::get(&repo, workspace_id)?;

    let response = EmbeddingColumnsResponse {
        columns: repositories::workspaces::data_frames::embeddings::list_indexed_columns(
            &workspace, file_path,
        )?,
        status: StatusMessage::resource_found(),
    };

    Ok(HttpResponse::Ok().json(response))
}

// TODO: Write client / tests for this endpoint
