use std::path::Path;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::api;

use actix_web::{HttpRequest, HttpResponse};
use liboxen::error::OxenError;
use liboxen::view::{ListSchemaResponse, SchemaResponse, StatusMessage};

pub async fn list_or_get(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    // Try to see if they are asking for a specific file
    if let Ok(resource) = parse_resource(&req, &repo) {
        if resource.file_path != Path::new("") {
            let commit = resource.commit;

            log::debug!("schemas::list_or_get commit {}", commit);

            let schema = api::local::schemas::get(&repo, &commit.id, &resource.file_path)?
                .ok_or(OxenError::path_does_not_exist(resource.file_path.clone()))?;
            let response = SchemaResponse {
                status: StatusMessage::resource_found(),
                schema,
            };
            return Ok(HttpResponse::Ok().json(response));
        }
    }

    // Otherwise, list all schemas
    let revision = path_param(&req, "resource")?;

    let commit = api::local::revisions::get(&repo, &revision)?
        .ok_or(OxenError::revision_not_found(revision.to_owned().into()))?;

    log::debug!(
        "schemas::list_or_get revision {} commit {}",
        revision,
        commit
    );

    let schemas = api::local::schemas::list(&repo, Some(&commit.id))?;
    let response = ListSchemaResponse {
        status: StatusMessage::resource_found(),
        schemas,
    };
    Ok(HttpResponse::Ok().json(response))
}
