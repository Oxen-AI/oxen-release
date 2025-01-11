use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::error::OxenError;

use liboxen::view::{MetadataEntryResponse, StatusMessage};
use liboxen::{current_function, repositories};

use actix_web::{HttpRequest, HttpResponse};

pub async fn file(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let latest_commit = repositories::commits::get_by_id(&repo, &commit.id)?
        .ok_or(OxenError::revision_not_found(commit.id.clone().into()))?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let mut entry = repositories::entries::get_meta_entry(&repo, &commit, &resource.path)?;
    entry.resource = Some(resource.clone());
    let meta = MetadataEntryResponse {
        status: StatusMessage::resource_found(),
        entry,
    };
    Ok(HttpResponse::Ok().json(meta))
}
