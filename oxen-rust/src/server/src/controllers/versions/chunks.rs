

use std::collections::HashMap;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::view::StatusMessage;

pub async fn upload(
    req: HttpRequest,
    mut body: web::Payload
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;
    let chunk_number = path_param(&req, "chunk_number")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!("/upload version {} chunk {} to repo: {:?}", version_id, chunk_number, repo.path);

    Ok(HttpResponse::Ok().json(StatusMessage::resource_found()))
}

pub async fn complete(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!("/complete version chunk upload to repo: {:?}", repo.path);

    // Try to deserialize the body
    let data: Result<Vec<HashMap<String, String>>, serde_json::Error> = serde_json::from_str(&body);
    if let Ok(data) = data {
        log::debug!("Received {} chunks", data.len());
        return Ok(HttpResponse::Ok().json(StatusMessage::resource_found()));
    }

    Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid request body")))
}