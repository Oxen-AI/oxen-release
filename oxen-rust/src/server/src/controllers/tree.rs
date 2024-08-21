use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};
use actix_web::{HttpRequest, HttpResponse};
use liboxen::view::StatusMessage;

pub async fn get_node_by_id(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    // Take in the node id, and return which children are missing
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let node_id = path_param(&req, "node_id")?;

    return Ok(HttpResponse::BadRequest().json(StatusMessage::error("Implement me!".to_string())));
}
