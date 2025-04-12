

use std::collections::HashMap;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::view::StatusMessage;

pub async fn upload(
    req: HttpRequest,
    body: web::Payload
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;
    let chunk_number = path_param(&req, "chunk_number")?;
    let chunk_number = chunk_number.parse::<u32>()
        .map_err(|_| OxenHttpError::BadRequest(
            format!("Invalid chunk number, must be a number: {}", chunk_number).into()
        )
    )?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!("/upload version {} chunk {} to repo: {:?}", version_id, chunk_number, repo.path);

    let version_store = repo.version_store()?;
    let body = body.to_bytes().await?;
    version_store.store_version_chunk(&version_id, chunk_number, &body)?;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_found()))
}

pub async fn complete(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!("/complete version chunk upload to repo: {:?}", repo.path);

    // Try to deserialize the body
    let data: Result<Vec<HashMap<String, String>>, serde_json::Error> = serde_json::from_str(&body);
    if let Ok(data) = data {
        log::debug!("Received {} chunks", data.len());
        let version_store = repo.version_store()?;

        let chunks = version_store.list_version_chunks(&version_id)?;
        log::debug!("Found {} chunks", chunks.len());

        if chunks.len() != data.len() {
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(format!("Number of chunks does not match expected number of chunks: {} != {}", chunks.len(), data.len()))));
        }

        // Combine all the chunks for a version file into a single file
        version_store.combine_version_chunks(&version_id, true)?;
        return Ok(HttpResponse::Ok().json(StatusMessage::resource_found()));
    }

    Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid request body")))
}