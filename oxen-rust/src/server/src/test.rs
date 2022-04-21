use liboxen::api::local::repositories::RepositoryAPI;
use liboxen::error::OxenError;
use liboxen::model::RepositoryNew;
use crate::app_data::SyncDir;
use std::path::{Path, PathBuf};
use std::borrow::Cow;
use serde::Serialize;

pub fn get_sync_dir() -> PathBuf {
    let sync_dir = PathBuf::from(format!("/tmp/oxen/tests/{}", uuid::Uuid::new_v4()));
    sync_dir
}

pub fn create_repo(sync_dir: &Path, name: &str) -> Result<RepositoryNew, OxenError> {
    let api = RepositoryAPI::new(sync_dir);
    let repo = RepositoryNew {
        name: String::from(name),
    };
    api.create(&repo)?;
    Ok(repo)
}

pub fn request(sync_dir: &Path, uri: &str) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(&uri)
        .app_data(SyncDir {
            path: sync_dir.to_path_buf(),
        })
        .to_http_request()
}

pub fn request_with_param(
    sync_dir: &Path, 
    uri: &str, 
    key: impl Into<Cow<'static, str>>, 
    val: impl Into<Cow<'static, str>>
) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(&uri)
        .app_data(SyncDir {
            path: sync_dir.to_path_buf(),
        })
        .param(key, val)
        .to_http_request()
}

pub fn request_with_json(
    sync_dir: &Path, 
    uri: &str, 
    data: impl Serialize
) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(&uri)
        .app_data(SyncDir {
            path: sync_dir.to_path_buf(),
        })
        .set_json(data)
        .to_http_request()
}
