use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};
use actix_web::{web, HttpRequest, HttpResponse, Result};
use liboxen::repositories;
use liboxen::view::fork::ForkRequest;

pub async fn fork(
    req: HttpRequest,
    body: web::Json<ForkRequest>,
) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("Forking repository with request: {:?}", req);
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    let original_repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    // Here we need to make sure that the hub checks that the user has the permissions to fork to the organization namespace
    let new_repo_namespace = body.namespace.clone();

    let new_repo_name = body.new_repo_name.clone().unwrap_or(repo_name.clone());

    let new_repo_path = app_data.path.join(&new_repo_namespace).join(&new_repo_name);

    match repositories::fork(&original_repo.path, &new_repo_path) {
        Ok(new_repo) => {
            log::info!("Successfully forked repository to {:?}", new_repo_path);
            Ok(HttpResponse::Created().json(new_repo))
        }
        Err(err) => {
            log::error!("Failed to fork repository: {:?}", err);
            Err(OxenHttpError::from(err))
        }
    }
}
