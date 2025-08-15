pub mod chunks;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use actix_multipart::Multipart;
use actix_web::{web, Error, HttpRequest, HttpResponse};
use flate2::read::GzDecoder;
use futures_util::TryStreamExt as _;
use liboxen::error::OxenError;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::versions::{VersionFile, VersionFileResponse};
use liboxen::view::{ErrorFileInfo, ErrorFilesResponse, StatusMessage};
use mime;
use std::io::Read as StdRead;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;

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

    let data = repo.version_store()?.get_version(&version_id).await?;
    Ok(HttpResponse::Ok().json(VersionFileResponse {
        status: StatusMessage::resource_found(),
        version: VersionFile {
            hash: version_id,
            size: data.len() as u64,
        },
    }))
}

// TODO: Refactor places that call /file/{resource:*} to use this new version store download endpoint
pub async fn download(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let version_store = repo.version_store()?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource
        .clone()
        .commit
        .ok_or_else(|| OxenError::basic_str("Resource commit not found"))?;
    let path = resource.path.clone();

    log::debug!("Download resource {namespace}/{repo_name}/{resource} version file");

    let entry = repositories::entries::get_file(&repo, &commit, &path)?
        .ok_or(OxenError::path_does_not_exist(path.clone()))?;
    let file_hash = entry.hash();
    let mime_type = entry.mime_type();
    let last_commit_id = entry.last_commit_id().to_string();
    let version_path = version_store.get_version_path(&file_hash.to_string())?;

    // TODO: refactor out of here and check for type,
    // but seeing if it works to resize the image and cache it to disk if we have a resize query
    let img_resize = query.into_inner();
    if img_resize.width.is_some() || img_resize.height.is_some() {
        log::debug!("img_resize {:?}", img_resize);

        let resized_path = util::fs::handle_image_resize(
            Arc::clone(&version_store),
            file_hash.to_string(),
            &path,
            &version_path,
            img_resize,
        )?;

        // Generate stream for the resized image
        let file = File::open(&resized_path).await?;
        let reader = BufReader::new(file);
        let stream = ReaderStream::new(reader);

        return Ok(HttpResponse::Ok()
            .content_type(mime_type)
            .insert_header(("oxen-revision-id", last_commit_id.as_str()))
            .streaming(stream));
    } else {
        log::debug!("did not hit the resize cache");
    }

    let stream = version_store
        .get_version_stream(&file_hash.to_string())
        .await?;
    Ok(HttpResponse::Ok()
        .content_type(mime_type)
        .insert_header(("oxen-revision-id", last_commit_id.as_str()))
        .streaming(stream))
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
    let files = save_multiparts(payload, &repo).await?;

    Ok(HttpResponse::Ok().json(ErrorFilesResponse {
        status: StatusMessage::resource_created(),
        err_files: files,
    }))
}

pub async fn save_multiparts(
    mut payload: Multipart,
    repo: &LocalRepository,
) -> Result<Vec<ErrorFileInfo>, Error> {
    // Receive a multipart request and save the files to the version store
    let version_store = repo.version_store().map_err(|oxen_err: OxenError| {
        log::error!("Failed to get version store: {:?}", oxen_err);
        actix_web::error::ErrorInternalServerError(oxen_err.to_string())
    })?;
    let gzip_mime: mime::Mime = "application/gzip".parse().unwrap();

    let mut err_files: Vec<ErrorFileInfo> = vec![];

    while let Some(mut field) = payload.try_next().await? {
        let Some(content_disposition) = field.content_disposition().cloned() else {
            continue;
        };

        if let Some(name) = content_disposition.get_name() {
            if name == "file[]" {
                // The file hash is passed in as the filename. In version store, the file hash is the identifier.
                let upload_filehash = content_disposition.get_filename().map_or_else(
                    || {
                        Err(actix_web::error::ErrorBadRequest(
                            "Missing hash in multipart request",
                        ))
                    },
                    |fhash_os_str| Ok(fhash_os_str.to_string()),
                )?;

                let mut field_bytes = Vec::new();
                while let Some(chunk) = field.try_next().await? {
                    field_bytes.extend_from_slice(&chunk);
                }

                let is_gzipped = field
                    .content_type()
                    .map(|mime| {
                        mime.type_() == gzip_mime.type_() && mime.subtype() == gzip_mime.subtype()
                    })
                    .unwrap_or(false);

                let upload_filehash_copy = upload_filehash.clone();

                // decompress the data if it is gzipped
                let data_to_store =
                    match actix_web::web::block(move || -> Result<Vec<u8>, OxenError> {
                        if is_gzipped {
                            log::debug!(
                                "Decompressing gzipped data for hash: {}",
                                &upload_filehash_copy
                            );
                            let mut decoder = GzDecoder::new(&field_bytes[..]);
                            let mut decompressed_bytes = Vec::new();
                            decoder.read_to_end(&mut decompressed_bytes).map_err(|e| {
                                OxenError::basic_str(format!(
                                    "Failed to decompress gzipped data: {}",
                                    e
                                ))
                            })?;
                            Ok(decompressed_bytes)
                        } else {
                            log::debug!("Data for hash {} is not gzipped.", &upload_filehash_copy);
                            Ok(field_bytes)
                        }
                    })
                    .await
                    {
                        Ok(Ok(data)) => data,
                        Ok(Err(e)) => {
                            log::error!(
                                "Failed to decompress data for hash {}: {}",
                                &upload_filehash,
                                e
                            );
                            record_error_file(
                                &mut err_files,
                                upload_filehash.clone(),
                                None,
                                format!("Failed to decompress data: {}", e),
                            );
                            continue;
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to execute blocking decompression task for hash {}: {}",
                                &upload_filehash,
                                e
                            );
                            record_error_file(
                                &mut err_files,
                                upload_filehash.clone(),
                                None,
                                format!("Failed to execute blocking decompression: {}", e),
                            );
                            continue;
                        }
                    };

                match version_store
                    .store_version(&upload_filehash, &data_to_store)
                    .await
                {
                    Ok(_) => {
                        log::info!("Successfully stored version for hash: {}", &upload_filehash);
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to store version for hash {}: {}",
                            &upload_filehash,
                            e
                        );
                        record_error_file(
                            &mut err_files,
                            upload_filehash.clone(),
                            None,
                            format!("Failed to store version: {}", e),
                        );
                        continue;
                    }
                }
            }
        }
    }
    Ok(err_files)
}

// Record the error file info for retry
fn record_error_file(
    err_files: &mut Vec<ErrorFileInfo>,
    filehash: String,
    filepath: Option<PathBuf>,
    error: String,
) {
    let info = ErrorFileInfo {
        hash: filehash,
        path: filepath,
        error,
    };
    err_files.push(info);
}

#[cfg(test)]
mod tests {
    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;
    use actix_multipart::test::create_form_data_payload_and_headers;
    use actix_web::{web, web::Bytes, App};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::ErrorFilesResponse;
    use mime;
    use std::io::Write;

    #[actix_web::test]
    async fn test_controllers_versions_download() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        // create test file and commit
        util::fs::create_dir_all(repo.path.join("data"))?;
        let relative_path = "data/hello.txt";
        let hello_file = repo.path.join(relative_path);
        let file_content = "Hello";
        util::fs::write_to_path(&hello_file, file_content)?;
        repositories::add(&repo, &hello_file).await?;
        repositories::commit(&repo, "First commit")?;

        // test download
        let uri = format!("/oxen/{namespace}/{repo_name}/versions/main/{relative_path}");
        let req = actix_web::test::TestRequest::get()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/versions/{resource:.*}",
                    web::get().to(controllers::versions::download),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(bytes, "Hello");

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_versions_batch_upload() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        repositories::add(&repo, path).await?;
        repositories::commit(&repo, "first commit")?;

        let file_content = "Test Content";
        let file_hash = util::hasher::hash_str(file_content);

        // compress file content
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(file_content.as_bytes())?;
        let compressed_bytes = encoder.finish()?;

        // create multipart request
        let (body, headers) = create_form_data_payload_and_headers(
            "file[]",
            Some(file_hash.clone()),
            Some("application/gzip".parse::<mime::Mime>().unwrap()),
            Bytes::from(compressed_bytes),
        );
        let uri = format!("/oxen/{namespace}/{repo_name}/versions");

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()));

        let req = headers
            .into_iter()
            .fold(req, |req, hdr| req.insert_header(hdr))
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/versions",
                    web::post().to(controllers::versions::batch_upload),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let response: ErrorFilesResponse = serde_json::from_slice(&bytes)?;
        assert_eq!(response.status.status, "success");
        assert!(response.err_files.is_empty());

        // verify file is stored correctly
        let version_store = repo.version_store()?;
        let stored_data = version_store.get_version(&file_hash).await?;
        assert_eq!(stored_data, file_content.as_bytes());

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }
}
