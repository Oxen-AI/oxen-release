use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};
use crate::auth::access_keys::AccessKeyManager;

use liboxen::error::OxenError;
use liboxen::model::commit::NewCommitBody;
use liboxen::model::file::{FileContents, FileNew, TempFileNew};
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::{Commit, User};
use liboxen::repositories::{self, branches};
use liboxen::util;
use liboxen::view::{CommitResponse, StatusMessage};

use actix_files::NamedFile;
use actix_multipart::Multipart;
use actix_web::{http::header, web, HttpRequest, HttpResponse};
use futures_util::{StreamExt, TryStreamExt as _};
use liboxen::repositories::commits;
use serde_json::Value;
use std::path::PathBuf;

const ALLOWED_IMPORT_DOMAINS: [&str; 3] = ["huggingface.co", "kaggle.com", "oxen.ai"];

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
    let workspace_ref = resource.workspace.as_ref();
    let commit = if let Some(workspace) = workspace_ref {
        workspace.commit.clone()
    } else {
        resource.clone().commit.unwrap()
    };

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );
    let path = resource.path.clone();

    // If the resource is a workspace, return the file from the workspace
    let response = if let Some(workspace) = workspace_ref {
        let file_path = workspace.workspace_repo.path.join(path.clone());
        let file = NamedFile::open(file_path)?;
        file.into_response(&req)
    } else {
        let entry = repositories::entries::get_file(&repo, &commit, &path)?;
        let entry = entry.ok_or(OxenError::path_does_not_exist(path.clone()))?;

        let version_path = util::fs::version_path_from_hash(&repo, entry.hash().to_string());

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
        let content_length = meta_entry.size.to_string();
        response.headers_mut().insert(
            header::HeaderName::from_static("oxen-revision-id"),
            header::HeaderValue::from_str(&last_commit_id).unwrap(),
        );

        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_str(&meta_entry.mime_type).unwrap(),
        );

        response.headers_mut().insert(
            header::CONTENT_LENGTH,
            header::HeaderValue::from_str(&content_length).unwrap(),
        );

        response
    };

    Ok(response)
}

/// Update file content in place (add to temp workspace and commit)
pub async fn put(
    req: HttpRequest,
    payload: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::put path {:?}", req.path());
    
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    // Try to parse the resource (branch/commit/path). If the repo has no commits yet this will
    // fail, so fall back to an initial-upload helper.
    let resource = match parse_resource(&req, &repo) {
        Ok(res) => res,
        Err(parse_err) => {
            if repositories::commits::head_commit_maybe(&repo)?.is_none() {
                return handle_initial_put_empty_repo(req, payload, &repo).await;
            } else {
                return Err(parse_err);
            }
        }
    };

    // Resource must specify branch because we need to commit the workspace back to a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;
    
    // Extract claimed commit hash from HTTP header
    let claimed_commit_hash = req.headers()
        .get("oxen-based-on")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string());
    
    // Check if the resource path is a file and handle conflicts
    let node = repositories::tree::get_node_by_path(&repo, &commit, &resource.path)?;
    if let Some(node) = node {
        if node.is_file() {
            // Get current commit hash for the file
            let current_commit_hash = node.latest_commit_id()?.to_string();
            
            // Only fail if claimed hash is provided but doesn't match current hash
            if let Some(claimed_hash) = claimed_commit_hash {
                if current_commit_hash != claimed_hash {
                    return Err(OxenHttpError::BasicError(
                        format!(
                            "File has been modified since claimed revision. Current: {}, Claimed: {}. Your changes would overwrite another change without that being from a merge",
                            current_commit_hash, claimed_hash
                        )
                        .into(),
                    ));
                }
            }
        }
    }

    // Try to get commit message from header first (for backwards compatibility)
    let header_message = req.headers().get("oxen-commit-message")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Optional commit info from headers
    //TODO: cease using header_author and  header_email below, instead take from authenticated_user var below

    // Parse payload based on content type
    let content_type = req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .unwrap_or("");
    
    let (message, temp_files) = if content_type.starts_with("multipart/form-data") {
        // Handle multipart data
        let multipart = Multipart::new(req.headers(), payload);
        parse_multipart_fields(multipart).await?
    } else {
        // Handle raw payload
        parse_raw_payload(&req, payload).await?
    };
    
    // Get authenticated user from bearer token
    let authenticated_user = get_authenticated_user(&req)?;
    
    // If header message is provided, it must be valid and non-empty (backwards compatibility)
    if req.headers().contains_key("oxen-commit-message") {
        if header_message.is_none() {
            log::warn!("💬 Invalid oxen-commit-message header provided");
            return Err(OxenHttpError::BadRequest("Invalid oxen-commit-message header value".into()));
        }
        if let Some(ref msg) = header_message {
            if msg.trim().is_empty() {
                log::warn!("💬 Empty oxen-commit-message header provided");
                return Err(OxenHttpError::BadRequest("Invalid oxen-commit-message header value".into()));
            }
        }
    }

    // Use authenticated user if available, otherwise require authentication
    let user = match authenticated_user {
        Some(user) => user,
        None => return Err(OxenHttpError::BadRequest("Bearer token required for PUT operations".into())),
    };

    let mut files: Vec<FileNew> = vec![];
    for temp_file in temp_files {
        files.push(FileNew {
            path: temp_file.path,
            contents: temp_file.contents,
            user: user.clone(), // Clone the user for each file
        });
    }
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

    process_and_add_files(
        &repo,
        Some(&workspace),
        resource.path.clone(),
        files.clone(),
    )
    .await?;

    // Commit workspace
    let commit_body = NewCommitBody {
        author: user.name.clone(),
        email: user.email.clone(),
        message: header_message.or(message).unwrap_or(format!(
            "Auto-commit files to {}",
            &resource.path.to_string_lossy()
        )),
    };
    
    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name)?;

    log::debug!("file::put workspace commit ✅ success! commit {:?}", commit);

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

/// Delete file content (remove from repository and commit)
pub async fn delete(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::delete path {:?}", req.path());
    
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    // Parse the resource (branch/commit/path) - DELETE operations require existing commits
    let resource = parse_resource(&req, &repo)?;

    // Resource must specify branch because we need to commit the workspace back to a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;
    
    // Extract claimed commit hash from HTTP header
    let claimed_commit_hash = req.headers()
        .get("oxen-based-on")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string());
    
    // Check if the resource path exists and is a file
    let node = repositories::tree::get_node_by_path(&repo, &commit, &resource.path)?;
    let node = node.ok_or_else(|| {
        OxenHttpError::NotFound
    })?;
    
    if !node.is_file() {
        return Err(OxenHttpError::BadRequest(
            format!("Cannot delete directory: {}", resource.path.display()).into()
        ));
    }

    // Get current commit hash for the file and validate oxen-based-on header if provided
    let current_commit_hash = node.latest_commit_id()?.to_string();
    if let Some(claimed_hash) = claimed_commit_hash {
        if current_commit_hash != claimed_hash {
            return Err(OxenHttpError::BasicError(
                format!(
                    "File has been modified since claimed revision. Current: {}, Claimed: {}. Your changes would overwrite another change without that being from a merge",
                    current_commit_hash, claimed_hash
                )
                .into(),
            ));
        }
    }

    // Get authenticated user from bearer token
    let authenticated_user = get_authenticated_user(&req)?;
    let user = match authenticated_user {
        Some(user) => user,
        None => return Err(OxenHttpError::BadRequest("Bearer token required for DELETE operations".into())),
    };

    // Create temporary workspace
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

    // Stage the deletion using the relative path (not absolute workspace path)
    repositories::workspaces::files::rm(&workspace, &resource.path).await?;

    // Commit workspace with deletion
    let commit_body = NewCommitBody {
        author: user.name.clone(),
        email: user.email.clone(),
        message: format!("Delete file {}", resource.path.display()),
    };
    
    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name)?;

    log::debug!("file::delete workspace commit ✅ success! commit {:?}", commit);

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

// Helper: when the repository has no commits yet, accept the upload as the first commit on the
// default branch ("main").
async fn handle_initial_put_empty_repo(
    req: HttpRequest,
    payload: web::Payload,
    repo: &liboxen::model::LocalRepository,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let resource: PathBuf = PathBuf::from(req.match_info().query("resource"));
    let path_string = resource
        .components()
        .skip(1)
        .collect::<PathBuf>()
        .to_string_lossy()
        .to_string();

    // Parse payload based on content type
    let content_type = req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .unwrap_or("");
    
    let (_message, temp_files) = if content_type.starts_with("multipart/form-data") {
        // Handle multipart data
        let multipart = Multipart::new(req.headers(), payload);
        parse_multipart_fields(multipart).await?
    } else {
        // Handle raw payload
        parse_raw_payload(&req, payload).await?
    };

    // Get authenticated user from bearer token
    let authenticated_user = get_authenticated_user(&req)?;
    let user = match authenticated_user {
        Some(user) => user,
        None => return Err(OxenHttpError::BadRequest("Bearer token required for PUT operations".into())),
    };

    // Convert temporary files to FileNew with the complete user information
    let mut files: Vec<FileNew> = vec![];
    for temp_file in temp_files {
        files.push(FileNew {
            path: temp_file.path,
            contents: temp_file.contents,
            user: user.clone(), // Clone the user for each file
        });
    }

    // If the user supplied files, add and commit them
    let mut commit: Option<Commit> = None;

    process_and_add_files(repo, None, PathBuf::from(&path_string), files.clone()).await?;

    if !files.is_empty() {
        let user_ref = &files[0].user; // Use the user from the first file, since it's the same for all
        commit = Some(commits::commit_with_user(repo, "Initial commit", user_ref)?);
        
        // Only create main branch if it doesn't exist
        if branches::current_branch(repo).is_err() {
            branches::create(repo, "main", &commit.as_ref().unwrap().id)?;
        }
    }

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit: commit.unwrap(),
    }))
}

/// import files from hf/kaggle (create a workspace and commit)
pub async fn import(
    req: HttpRequest,
    body: web::Json<Value>,
) -> Result<HttpResponse, OxenHttpError> {
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
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

    log::debug!("workspace::files::import_file workspace created!");

    // extract auth key from req body
    let auth = body
        .get("headers")
        .and_then(|headers| headers.as_object())
        .and_then(|map| map.get("Authorization"))
        .and_then(|auth| auth.as_str())
        .unwrap_or_default();

    let download_url = body
        .get("download_url")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    // Validate URL domain
    let url_parsed = url::Url::parse(download_url)
        .map_err(|_| OxenHttpError::BadRequest("Invalid URL".into()))?;
    let domain = url_parsed
        .domain()
        .ok_or_else(|| OxenHttpError::BadRequest("Invalid URL domain".into()))?;
    if !ALLOWED_IMPORT_DOMAINS.iter().any(|&d| domain.ends_with(d)) {
        return Err(OxenHttpError::BadRequest("URL domain not allowed".into()));
    }

    // parse filename from the given url
    let filename = if url_parsed.domain() == Some("huggingface.co") {
        url_parsed.path_segments().and_then(|segments| {
            let segments: Vec<_> = segments.collect();
            if segments.len() >= 2 {
                let last_two = &segments[segments.len() - 2..];
                Some(format!("{}_{}", last_two[0], last_two[1]))
            } else {
                None
            }
        })
    } else {
        url_parsed
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .map(|s| s.to_string())
    }
    .ok_or_else(|| OxenHttpError::BadRequest("Invalid filename in URL".into()))?;

    // download and save the file into the workspace
    repositories::workspaces::files::import(download_url, auth, directory, filename, &workspace)
        .await?;

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

// Helper function to extract authenticated user from bearer token
fn get_authenticated_user(req: &HttpRequest) -> Result<Option<User>, OxenHttpError> {
    // Extract bearer token from Authorization header
    let auth_header = req.headers().get("authorization");
    
    if let Some(auth_value) = auth_header {
        if let Ok(auth_str) = auth_value.to_str() {
            if auth_str.starts_with("Bearer ") {
                let token = &auth_str[7..]; // Remove "Bearer " prefix
                let app_data = app_data(req)?;
                
                log::debug!("🔑 Attempting to validate bearer token: {}...", &token[..std::cmp::min(20, token.len())]);
                log::debug!("🔑 AccessKeyManager path: {:?}", &app_data.path);
                
                match AccessKeyManager::new_read_only(&app_data.path) {
                    Ok(keygen) => {
                        log::debug!("🔑 AccessKeyManager created successfully");
                        match keygen.get_claim(token) {
                            Ok(Some(claim)) => {
                                log::debug!("🔑 ✅ Token validated successfully for user: {}", claim.name());
                                return Ok(Some(User {
                                    name: claim.name().to_string(),
                                    email: claim.email().to_string(),
                                }));
                            }
                            Ok(None) => {
                                log::debug!("🔑 ❌ Token validation returned None");
                            }
                            Err(e) => {
                                log::debug!("🔑 ❌ Token validation error: {:?}", e);
                            }
                        }
                    }
                    Err(err) => {
                        log::debug!("🔑 ❌ AccessKeyManager creation failed: {:?}", err);
                        // Log the keys database issue but don't crash with internal server error
                        log::debug!("AccessKeyManager failed to initialize: {:?}", err);
                        // Treat missing keys DB as "no authentication configured" instead of crashing
                    }
                }
            } else {
                log::debug!("🔑 ❌ Authorization header does not start with 'Bearer '");
            }
        } else {
            log::debug!("🔑 ❌ Could not parse authorization header as string");
        }
    } else {
        log::debug!("🔑 ❌ No authorization header found");
    }
    
    Ok(None)
}

async fn parse_multipart_fields(
    mut payload: Multipart,
) -> actix_web::Result<
    (
        Option<String>,
        Vec<TempFileNew>,
    ),
    OxenHttpError,
> {
    let mut message: Option<String> = None;
    let mut temp_files: Vec<TempFileNew> = vec![];

    while let Some(mut field) = payload
        .try_next()
        .await
        .map_err(OxenHttpError::MultipartError)?
    {
        let disposition = field.content_disposition().ok_or(OxenHttpError::NotFound)?;
        let field_name = disposition
            .get_name()
            .ok_or(OxenHttpError::NotFound)?
            .to_string();

        match field_name.as_str() {
            "name" | "email" => {
                // Skip name and email fields - they come from authenticated user
                while let Some(_chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    // Just consume the field data
                }
            }
            "message" => {
                let mut bytes = Vec::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    bytes.extend_from_slice(&chunk);
                }
                let value = String::from_utf8(bytes)
                    .map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;
                message = Some(value);
            }
            "files[]" | "file" => {
                let filename = disposition.get_filename().map_or_else(
                    || uuid::Uuid::new_v4().to_string(),
                    sanitize_filename::sanitize,
                );

                let mut contents = Vec::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    contents.extend_from_slice(&chunk);
                }

                temp_files.push(TempFileNew {
                    path: PathBuf::from(&filename),
                    contents: FileContents::Binary(contents),
                });
            }
            _ => {}
        }
    }

    Ok((message, temp_files))
}

async fn parse_raw_payload(
    req: &HttpRequest,
    mut payload: web::Payload,
) -> actix_web::Result<(Option<String>, Vec<TempFileNew>), OxenHttpError> {
    // Extract file path from the URL
    let path_info = req.path();
    // Extract the filename from the last part of the path
    let filename = path_info.split('/').last().unwrap_or("file").to_string();
    
    // Check if the path ends with '/' which indicates a directory
    if path_info.ends_with('/') {
        return Err(OxenHttpError::BadRequest("Cannot PUT to a directory path. Path cannot end with '/'".into()));
    }
    
    // Collect the raw payload bytes
    let mut bytes = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|e| OxenHttpError::BadRequest(format!("Payload error: {}", e).into()))?;
        bytes.extend_from_slice(&chunk);
    }
    
    // Create a temporary file from the raw bytes
    let temp_file = TempFileNew {
        path: std::path::PathBuf::from(&filename),
        contents: FileContents::Text(String::from_utf8_lossy(&bytes).to_string()),
    };
    
    // Extract commit message from header (optional)
    let message = req.headers()
        .get("oxen-commit-message")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    
    Ok((message, vec![temp_file]))
}

// Helper function for processing files and adding to repo/workspace
async fn process_and_add_files(
    repo: &liboxen::model::LocalRepository,
    workspace: Option<&liboxen::repositories::workspaces::TemporaryWorkspace>,
    base_path: PathBuf,
    files: Vec<FileNew>,
) -> Result<(), OxenError> {
    if !files.is_empty() {
        log::debug!(
            "process_and_add_files() processing {} files to base_path: {:?}",
            files.len(),
            base_path
        );
        for file in files.clone() {
            let contents = &file.contents;

            // The base_path from the URL is the definitive path for the file.
            // The filename from multipart is ignored to avoid ambiguity.
            let full_path_in_dest = if let Some(ws) = workspace {
                ws.dir().join(&base_path)
            } else {
                repo.path.join(&base_path)
            };

            log::debug!(
                "process_and_add_files() full_path_in_dest: {:?}",
                full_path_in_dest
            );

            // Create parent directory if it doesn't exist
            if let Some(parent) = full_path_in_dest.parent() {
                if !parent.exists() {
                    log::debug!("process_and_add_files() creating parent dir: {:?}", parent);
                    util::fs::create_dir_all(parent)?;
                }
            }

            // Write the file contents
            match contents {
                FileContents::Text(text) => {
                    util::fs::write(&full_path_in_dest, text.as_bytes())?;
                }
                FileContents::Binary(bytes) => {
                    util::fs::write(&full_path_in_dest, bytes)?;
                }
            }

            // Add the file to staging
            if let Some(ws) = workspace {
                repositories::workspaces::files::add(ws, &full_path_in_dest).await?;
            } else {
                repositories::add(repo, &full_path_in_dest).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use actix_web::{web, App};
    use liboxen::view::CommitResponse;

    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;
    
    #[actix_web::test]
    async fn test_controllers_file_import() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let author = "test_user";
        let email = "ox@oxen.ai";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let uri = format!("/oxen/{namespace}/{repo_name}/file/import/main/data");

        // import a file from oxen for testing
        let body = serde_json::json!({"download_url": "https://hub.oxen.ai/api/repos/datasets/GettingStarted/file/main/tables/cats_vs_dogs.tsv"});

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("repo_name", repo_name)
            .insert_header(("oxen-commit-author", author))
            .insert_header(("oxen-commit-email", email))
            .set_json(&body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/import/{resource:.*}",
                    web::post().to(controllers::file::import),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        let entry = repositories::entries::get_file(
            &repo,
            &resp.commit,
            PathBuf::from("data/cats_vs_dogs.tsv"),
        )?
        .unwrap();
        let version_path = util::fs::version_path_from_hash(&repo, entry.hash().to_string());
        assert!(version_path.exists());

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }
}
