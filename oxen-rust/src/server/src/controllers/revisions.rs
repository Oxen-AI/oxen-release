use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{HttpRequest, HttpResponse};

use liboxen::view::{ParseResourceResponse, StatusMessage};

use liboxen::api;

pub async fn resolve_resource_attributes(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;

    let repo_name = path_param(&req, "repo_name")?;
    let resource = PathBuf::from(path_param(&req, "resource")?);

    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let parse_result = api::local::resource::parse_resource(&repository, &resource)?;

    if let Some((commit_id, branch_name, file_path)) = parse_result {
        let response = ParseResourceResponse {
            status: StatusMessage::resource_found(),
            commit_id,
            branch_name,
            resource: file_path.to_string_lossy().into_owned(),
        };
        log::debug!("Response: {:?}", response);
        Ok(HttpResponse::Ok().json(response))
    } else {
        Err(OxenHttpError::NotFound)
    }
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::command;
    use liboxen::error::OxenError;
    use liboxen::util;

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_resolve_resource_attributes() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Repo";
        let resource_str = "main/to/resource";

        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let path = liboxen::test::add_txt_file_to_dir(&repo.path, resource_str)?;
        command::add(&repo, path)?;
        command::commit(&repo, "first commit")?;

        let uri = format!(
            "/oxen/{namespace}/{repo_name}/branches/resolve_resource_attributes/{resource_str}"
        );

        let req = test::repo_request_with_param(
            &sync_dir,
            queue,
            &uri,
            namespace,
            repo_name,
            "resource",
            resource_str,
        );

        let resp = controllers::revisions::resolve_resource_attributes(req)
            .await
            .unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);

        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let parse_resp: liboxen::view::ParseResourceResponse =
            serde_json::from_str(text).map_err(|e| OxenError::from(e))?;

        assert_eq!(parse_resp.branch_name, "main");
        assert_eq!(parse_resp.resource, "to/resource");

        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
