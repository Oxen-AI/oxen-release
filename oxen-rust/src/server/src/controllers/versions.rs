pub mod chunks;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_multipart::Multipart;
use actix_web::Error;
use actix_web::{HttpRequest, HttpResponse};
use flate2::read::GzDecoder;
use futures_util::TryStreamExt as _;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::view::versions::{VersionFile, VersionFileResponse};
use liboxen::view::{ErrorFileInfo, FilesHashResponse, StatusMessage};
use mime;
use std::io::Read as StdRead;

pub async fn metadata(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let exists = repo.version_store()?.version_exists(&version_id)?;
    if !exists {
        return Err(OxenHttpError::NotFound);
    }

    let data = repo.version_store()?.get_version(&version_id)?;
    Ok(HttpResponse::Ok().json(VersionFileResponse {
        status: StatusMessage::resource_found(),
        version: VersionFile {
            hash: version_id,
            size: data.len() as u64,
        },
    }))
}

pub async fn download(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!(
        "download file for repo: {:?}, file_hash: {}",
        repo.path,
        version_id
    );

    let version_store = repo.version_store()?;

    let file_data = version_store.get_version(&version_id)?;
    Ok(HttpResponse::Ok().body(file_data))
}

pub async fn batch_upload(
    req: HttpRequest,
    payload: Multipart,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;

    log::debug!("batch upload file for repo: {:?}", repo.path);

    let err_files = save_multiparts(payload, &repo).await?;
    Ok(HttpResponse::Ok().json(FilesHashResponse {
        status: StatusMessage::resource_created(),
        err_files,
    }))
}

pub async fn save_multiparts(
    mut payload: Multipart,
    repo: &LocalRepository,
) -> Result<Vec<ErrorFileInfo>, Error> {
    let version_store = repo.version_store().map_err(|oxen_err: OxenError| {
        log::error!("Failed to get version store: {:?}", oxen_err);
        actix_web::error::ErrorInternalServerError(oxen_err.to_string())
    })?;
    let gzip_mime: mime::Mime = "application/gzip".parse().unwrap();

    let mut err_files: Vec<ErrorFileInfo> = vec![];

    while let Some(mut field) = payload.try_next().await? {
        let Some(content_disposition) = field.content_disposition() else {
            continue;
        };

        if let Some(name) = content_disposition.get_name() {
            if "file[]" == name {
                let upload_filehash = content_disposition.get_filename().map_or_else(
                    || {
                        log::warn!("Multipart file part for '{}' missing filename (expected hash). Generating UUID as fallback.", name);
                        uuid::Uuid::new_v4().to_string()
                    },
                    |fhash_os_str| fhash_os_str.to_string()
                );

                let mut field_bytes = Vec::new();
                while let Some(chunk) = field.try_next().await? {
                    field_bytes.extend_from_slice(&chunk);
                }

                let data_to_store = if field.content_type() == Some(&gzip_mime) {
                    log::debug!("Decompressing gzipped data for hash: {}", &upload_filehash);
                    let mut decoder = GzDecoder::new(&field_bytes[..]);
                    let mut decompressed_bytes = Vec::new();
                    if let Err(e) = decoder.read_to_end(&mut decompressed_bytes) {
                        log::error!(
                            "Failed to decompress gzipped data for hash {}: {}",
                            &upload_filehash,
                            e
                        );
                        err_files.push(ErrorFileInfo {
                            hash: upload_filehash.clone(),
                            error: format!("Failed to decompress gzipped data: {}", e),
                        });
                        continue;
                    }
                    decompressed_bytes
                } else {
                    log::debug!("Data for hash {} is not gzipped.", &upload_filehash);
                    field_bytes
                };
                match version_store.store_version(&upload_filehash, &data_to_store) {
                    Ok(_) => {
                        log::info!("Successfully stored version for hash: {}", &upload_filehash);
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to store version for hash {}: {}",
                            &upload_filehash,
                            e
                        );
                        err_files.push(ErrorFileInfo {
                            hash: upload_filehash.clone(),
                            error: format!("Failed to store version: {}", e),
                        });
                        continue;
                    }
                }
            }
        }
    }
    Ok(err_files)
}
