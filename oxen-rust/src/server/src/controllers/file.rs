use crate::controllers;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::error::OxenError;
use liboxen::model::commit::NewCommitBody;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::{CommitResponse, StatusMessage};

use actix_files::NamedFile;
use actix_multipart::Multipart;
use actix_web::{http::header, web, HttpRequest, HttpResponse};
use futures::StreamExt;
use reqwest::header::HeaderValue;
use reqwest::Client;
use serde_json::Value;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use uuid::Uuid;
use zip::ZipArchive;

const BUFFER_SIZE_THRESHOLD: usize = 262144; // 256kb

/// Download file content
pub async fn get(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("get file path {:?}", req.path());

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );
    let path = resource.path.clone();
    let entry = repositories::entries::get_file(&repo, &commit, &path)?;
    // log::debug!("entry {:?}", entry);
    let entry = entry.ok_or(OxenError::path_does_not_exist(path.clone()))?;

    let version_path = util::fs::version_path_from_hash(&repo, entry.hash().to_string());
    log::debug!("version path {version_path:?}",);

    // TODO: refactor out of here and check for type,
    // but seeing if it works to resize the image and cache it to disk if we have a resize query
    let img_resize = query.into_inner();
    if img_resize.width.is_some() || img_resize.height.is_some() {
        log::debug!("img_resize {:?}", img_resize);

        let resized_path = util::fs::resized_path_for_file_node(
            &repo,
            &entry,
            img_resize.width,
            img_resize.height,
        )?;

        util::fs::resize_cache_image(&version_path, &resized_path, img_resize)?;

        log::debug!("In the resize cache! {:?}", resized_path);
        return Ok(NamedFile::open(resized_path)?.into_response(&req));
    } else {
        log::debug!("did not hit the resize cache");
    }

    log::debug!(
        "get_file_for_commit_id looking for {:?} -> {:?}",
        path,
        version_path
    );

    let file = NamedFile::open(version_path)?;
    let mut response = file.into_response(&req);

    let last_commit_id = entry.last_commit_id().to_string();
    let meta_entry = repositories::entries::get_meta_entry(&repo, &commit, &path)?;

    response.headers_mut().insert(
        header::HeaderName::from_static("oxen-revision-id"),
        header::HeaderValue::from_str(&last_commit_id).unwrap(),
    );

    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_str(&meta_entry.mime_type).unwrap(),
    );

    Ok(response)
}

/// Update file content in place (add to temp workspace and commit)
pub async fn put(
    req: HttpRequest,
    payload: Multipart,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::put path {:?}", req.path());
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    // Resource must specify branch because we need to commit the workspace back to a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;
    // Generate a random workspace id
    let workspace_id = Uuid::new_v4().to_string();
    let workspace_name = format!("file-put-{}-{}", commit.id, resource.path.display());
    // Make sure the resource path is not already a file
    let node = repositories::tree::get_node_by_path(&repo, &commit, &resource.path)?;
    if node.is_some() && node.unwrap().is_file() {
        return Err(OxenHttpError::BasicError(
            format!(
                "Target path must be a directory: {}",
                resource.path.display()
            )
            .into(),
        ));
    }

    // Optional commit info
    let author = req.headers().get("oxen-commit-author");
    let email = req.headers().get("oxen-commit-email");
    let message = req.headers().get("oxen-commit-message");

    // Create temporary workspace
    let workspace = repositories::workspaces::create_with_name(
        &repo,
        &commit,
        &workspace_id,
        Some(workspace_name),
        true,
    )?;

    // Add files to workspace
    let files =
        controllers::workspaces::files::save_parts(&workspace, &resource.path, payload).await?;

    for file in files.iter() {
        log::debug!("file::put add file {:?}", file);
        let path = repositories::workspaces::files::add(&workspace, file)?;
        log::debug!("file::put add file ✅ success! staged file {:?}", path);
    }

    // Commit workspace
    let commit_body = NewCommitBody {
        author: author.map_or("".to_string(), |a| a.to_str().unwrap().to_string()),
        email: email.map_or("".to_string(), |e| e.to_str().unwrap().to_string()),
        message: message.map_or(
            format!("Auto-commit files to {}", &resource.path.to_string_lossy()),
            |m| m.to_str().unwrap().to_string(),
        ),
    };
    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name)?;
    log::debug!("file::put workspace commit ✅ success! commit {:?}", commit);

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

/// import files from hf/kaggle (create a workspace and commit)
pub async fn import(
    req: HttpRequest,
    body: web::Json<Value>,
) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("In workspace::files::import_file");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    // Resource must specify branch for committing the workspace
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;
    let directory = resource.path.clone();
    log::debug!("workspace::files::import_file Got directory: {directory:?}");

    // commit info
    let author = req.headers().get("oxen-commit-author");
    let email = req.headers().get("oxen-commit-email");
    let message = req.headers().get("oxen-commit-message");

    log::debug!(
        "file::import commit info author:{:?}, email:{:?}, message:{:?}",
        author,
        email,
        message
    );

    let workspace_id = Uuid::new_v4().to_string();
    let workspace_name = format!("import-file-{}", directory.display());

    // Make sure the resource path is not already a file
    let node = repositories::tree::get_node_by_path(&repo, &commit, &resource.path)?;
    if node.is_some() && node.unwrap().is_file() {
        return Err(OxenHttpError::BasicError(
            format!(
                "Target path must be a directory: {}",
                resource.path.display()
            )
            .into(),
        ));
    }

    // Create temporary workspace
    let workspace = repositories::workspaces::create_with_name(
        &repo,
        &commit,
        &workspace_id,
        Some(workspace_name),
        true,
    )?;

    log::debug!("workspace::files::import_file workspace created!");

    // extract auth key from req body
    let auth = body
        .get("headers")
        .and_then(|headers| headers.as_object())
        .and_then(|map| map.get("Authorization"))
        .and_then(|auth| auth.as_str())
        .unwrap_or_default();

    let url = body.get("url").and_then(|v| v.as_str()).unwrap_or_default();

    let mut filename = url.split('/').last().unwrap_or_default().to_string();
    log::debug!(
        "workspace::files::import_file Got uploaded file name: {}",
        filename
    );

    let auth_header_value = HeaderValue::from_str(auth)
        .map_err(|_e| OxenHttpError::BadRequest("Invalid header value".into()))?;

    let response = Client::new()
        .get(url)
        .header("Authentication", auth_header_value)
        .send()
        .await
        .map_err(|_e| OxenHttpError::BadRequest("Request failed".into()))?;

    let resp_headers = response.headers();

    let content_type = resp_headers
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| OxenHttpError::BadRequest("Missing content type".into()))?;

    let content_length = response
        .content_length()
        .ok_or_else(|| OxenHttpError::BadRequest("Missing content length".into()))?;

    // change the suffix to .zip if is_zip
    let mut is_zip: bool = false;
    if content_type.contains("zip") {
        is_zip = true;
        if let Some(dot_index) = filename.rfind('.') {
            filename.truncate(dot_index);
        }
        filename.push_str(".zip");
    };
    log::debug!("files::import_file Got filename : {filename:?}");

    let filepath = directory.join(filename);
    log::debug!("files::import_file got download filepath: {:?}", filepath);

    // handle download stream
    let mut stream = response.bytes_stream();
    let mut buffer = web::BytesMut::new();
    let mut save_path = PathBuf::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|_| OxenHttpError::BadRequest("Error reading import file payload".into()))?;
        let processed_chunk = chunk.to_vec();
        buffer.extend_from_slice(&processed_chunk);

        if buffer.len() > BUFFER_SIZE_THRESHOLD {
            save_path = controllers::workspaces::files::save_stream(
                &workspace,
                &filepath,
                buffer.split().freeze().to_vec(),
            )
            .await?;
        }
    }

    if !buffer.is_empty() {
        save_path = controllers::workspaces::files::save_stream(
            &workspace,
            &filepath,
            buffer.freeze().to_vec(),
        )
        .await?;
    }
    log::debug!("workspace::files::import_file save_path is {:?}", save_path);

    // check if the file size matches
    let bytes_written = if save_path.exists() {
        std::fs::metadata(&save_path)?.len()
    } else {
        0
    };

    log::debug!(
        "workspace::files::import_file has written {:?} bytes. It's expecting {:?} bytes",
        bytes_written,
        content_length
    );
    
    if bytes_written != content_length {
        return Err(OxenHttpError::BadRequest(
            "Content length does not match. File incomplete.".into(),
        ));
    }

    // decompress and stage file
    if is_zip {
        let files = decompress_zip(&save_path).await?;
        log::debug!("workspace::files::import_file unzipped file");

        for file in files.iter() {
            log::debug!("file::import add file {:?}", file);
            let path = repositories::workspaces::files::add(&workspace, file)?;
            log::debug!("file::import add file ✅ success! staged file {:?}", path);
        }
    } else {
        log::debug!("file::import add file {:?}", &filepath);
        let path = repositories::workspaces::files::add(&workspace, &save_path)?;
        log::debug!("file::import add file ✅ success! staged file {:?}", path);
    }

    // Commit workspace
    let commit_body = NewCommitBody {
        author: author.map_or("".to_string(), |a| a.to_str().unwrap().to_string()),
        email: email.map_or("".to_string(), |e| e.to_str().unwrap().to_string()),
        message: message.map_or(
            format!("Import files to {}", &resource.path.to_string_lossy()),
            |m| m.to_str().unwrap().to_string(),
        ),
    };

    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name)?;
    log::debug!("workspace::commit ✅ success! commit {:?}", commit);

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

async fn decompress_zip(zip_filepath: &PathBuf) -> Result<Vec<PathBuf>, OxenError> {
    let mut files: Vec<PathBuf> = vec![];
    let file = File::open(zip_filepath)?;
    let mut archive = ZipArchive::new(file)
        .map_err(|e| OxenError::Basic(format!("Failed to access zip file: {}", e).into()))?;

    log::debug!("files::decompress_zip zip filepath is {:?}", zip_filepath);

    let parent = match zip_filepath.parent() {
        Some(p) => p.to_path_buf(),
        None => PathBuf::from("."),
    };
    log::debug!("files::decompress_zip zipfilepath parent is {:?}", parent);

    // iterate thru zip archive and save the decompressed file
    for i in 0..archive.len() {
        let mut zip_file = archive.by_index(i).map_err(|e| {
            OxenError::Basic(format!("Failed to access zip file at index {}: {}", i, e).into())
        })?;

        let mut zipfile_name = zip_file.mangled_name();
        if let Some(zipfile_name_str) = zipfile_name.to_str() {
            if zipfile_name_str.contains(' ') {
                let new_name = zipfile_name_str.replace(' ', "_");
                zipfile_name = PathBuf::from(new_name);
            }
        }
        let outpath = parent.join(zipfile_name);
        log::debug!("files::decompress_zip unzipping file to: {:?}", outpath);
        if let Some(outdir) = outpath.parent() {
            std::fs::create_dir_all(outdir)?;
        }

        if zip_file.is_dir() {
            std::fs::create_dir_all(&outpath)?;
        } else {
            let mut outfile = File::create(&outpath)?;
            let mut buffer = vec![0; BUFFER_SIZE_THRESHOLD];

            loop {
                let n = zip_file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                outfile.write_all(&buffer[..n])?;
            }
        }

        files.push(outpath);
    }
    log::debug!(
        "files::decompress_zip removing zip file: {:?}",
        zip_filepath
    );

    // remove the zip file after decompress
    std::fs::remove_file(zip_filepath)?;

    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use actix_multipart::test::create_form_data_payload_and_headers;
    use actix_web::{web, web::Bytes, App};
    use liboxen::view::CommitResponse;
    use mime;

    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_file_put() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        std::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        let _commit = repositories::commit(&repo, "First commit")?;

        // Create multipart request data
        let (body, headers) = create_form_data_payload_and_headers(
            "file",
            Some("hello.txt".to_owned()),
            Some(mime::TEXT_PLAIN_UTF_8),
            Bytes::from_static(b"Updated Content!"),
        );

        let uri = format!("/oxen/{namespace}/{repo_name}/file/main/data");
        let req = actix_web::test::TestRequest::put()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("repo_name", repo_name)
            .param("resource", "hello.txt");

        let req = headers
            .into_iter()
            .fold(req, |req, hdr| req.insert_header(hdr))
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        println!("Upload response: {}", body);
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        // Check that the file was updated
        let entry =
            repositories::entries::get_file(&repo, &resp.commit, PathBuf::from("data/hello.txt"))?
                .unwrap();
        let version_path = util::fs::version_path_from_hash(&repo, entry.hash().to_string());
        let updated_content = util::fs::read_from_path(&version_path)?;
        assert_eq!(updated_content, "Updated Content!");

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
