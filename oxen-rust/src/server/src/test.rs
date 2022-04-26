use crate::app_data::SyncDir;
use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use serde::Serialize;
use std::borrow::Cow;
use std::path::{Path, PathBuf};

pub fn get_sync_dir() -> Result<PathBuf, OxenError> {
    let sync_dir = PathBuf::from(format!("/tmp/oxen/tests/{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&sync_dir)?;
    Ok(sync_dir)
}

pub fn create_local_repo(sync_dir: &Path, name: &str) -> Result<LocalRepository, OxenError> {
    let repo_dir = sync_dir.join(name);
    let repo = command::init(&repo_dir)?;
    Ok(repo)
}

pub fn request(sync_dir: &Path, uri: &str) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(SyncDir {
            path: sync_dir.to_path_buf(),
        })
        .to_http_request()
}

pub fn request_with_param(
    sync_dir: &Path,
    uri: &str,
    key: impl Into<Cow<'static, str>>,
    val: impl Into<Cow<'static, str>>,
) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(SyncDir {
            path: sync_dir.to_path_buf(),
        })
        .param(key, val)
        .to_http_request()
}

pub fn request_with_json(
    sync_dir: &Path,
    uri: &str,
    data: impl Serialize,
) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(SyncDir {
            path: sync_dir.to_path_buf(),
        })
        .set_json(data)
        .to_http_request()
}

pub fn request_with_payload_and_entry(
    sync_dir: &Path,
    uri: &str,
    filename: impl Into<Cow<'static, str>>,
    hash: impl Into<Cow<'static, str>>,
    data: impl Into<actix_web::web::Bytes>,
) -> (actix_web::HttpRequest, actix_web::dev::Payload) {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(SyncDir {
            path: sync_dir.to_path_buf(),
        })
        .param("filename", filename)
        .param("hash", hash)
        .set_payload(data)
        .to_http_parts()
}
