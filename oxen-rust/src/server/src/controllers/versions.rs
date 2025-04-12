
pub mod chunks;

use actix_web::{web, HttpRequest, HttpResponse};
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use liboxen::view::StatusMessage;

pub async fn exists(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?; 
    let version_id = path_param(&req, "version_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let exists = false; // TODO: implement
    Ok(HttpResponse::Ok().json(StatusMessage::resource_found()))
}