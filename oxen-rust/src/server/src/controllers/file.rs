use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::error::OxenError;
use liboxen::util;
use liboxen::view::{EntryMetaDataResponse, StatusMessage};
use liboxen::{api, current_function};

use actix_files::NamedFile;
use actix_web::{HttpRequest, HttpResponse};

pub async fn get(req: HttpRequest) -> actix_web::Result<NamedFile, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );

    let version_path =
        util::fs::version_path_for_commit_id(&repo, &resource.commit.id, &resource.file_path)?;

    log::debug!(
        "get_file_for_commit_id looking for {:?} -> {:?}",
        resource.file_path,
        version_path
    );

    Ok(NamedFile::open(version_path)?)
}

pub async fn meta_data(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let latest_commit = api::local::commits::get_by_id(&repo, &resource.commit.id)?.ok_or(
        OxenError::committish_not_found(resource.commit.id.clone().into()),
    )?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let entry = api::local::entries::get_dir_entry(&repo, &resource.commit, &resource.file_path)?;
    let meta = EntryMetaDataResponse {
        status: StatusMessage::resource_found(),
        entry,
    };
    Ok(HttpResponse::Ok().json(meta))
}
