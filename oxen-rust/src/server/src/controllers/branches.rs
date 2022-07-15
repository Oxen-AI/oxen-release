use crate::app_data::OxenAppData;

use actix_web::{HttpRequest, HttpResponse};

use liboxen::api;
use liboxen::view::http::{MSG_RESOURCE_CREATED, MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{BranchNew, BranchResponse, ListBranchesResponse, StatusMessage};

pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    match api::local::repositories::get_by_name(&app_data.path, name) {
        Ok(Some(repository)) => match api::local::branches::list(&repository) {
            Ok(branches) => {
                let view = ListBranchesResponse {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_FOUND),
                    branches,
                };
                HttpResponse::Ok().json(view)
            }
            Err(err) => {
                log::error!("Unable to list branches. Err: {}", err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        },
        Ok(None) => {
            log::debug!(
                "404 api::local::branches::create could not get repo {}",
                name,
            );
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!(
                "Err api::local::branches::create could not get repo {} {:?}",
                name,
                err
            );
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

pub async fn show(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let name: Option<&str> = req.match_info().get("repo_name");
    let branch_name: Option<&str> = req.match_info().get("branch_name");
    if let (Some(name), Some(branch_name)) = (name, branch_name) {
        match api::local::repositories::get_by_name(&app_data.path, name) {
            Ok(Some(repository)) => {
                match api::local::branches::get_by_name(&repository, branch_name) {
                    Ok(Some(branch)) => HttpResponse::Ok().json(BranchResponse {
                        status: String::from(STATUS_SUCCESS),
                        status_message: String::from(MSG_RESOURCE_CREATED),
                        branch,
                    }),
                    Ok(None) => {
                        log::debug!(
                            "branch_name {} does not exist for repo: {}",
                            branch_name,
                            name
                        );
                        HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                    }
                    Err(err) => {
                        log::debug!("Err getting branch_name {}: {}", branch_name, err);
                        HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                    }
                }
            }
            Ok(None) => {
                log::debug!("404 api::local::branches::show could not get repo {}", name,);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::debug!("Could not find repo [{}]: {}", name, err);
                HttpResponse::NotFound().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Must supply `repo_name` and `branch_name` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn create_or_get(req: HttpRequest, body: String) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let data: Result<BranchNew, serde_json::Error> = serde_json::from_str(&body);

    let name: &str = req.match_info().get("repo_name").unwrap();
    match data {
        Ok(data) => match api::local::repositories::get_by_name(&app_data.path, name) {
            Ok(Some(repository)) => {
                match api::local::branches::get_by_name(&repository, &data.name) {
                    Ok(Some(branch)) => {
                        // Set the remote to this server
                        HttpResponse::Ok().json(BranchResponse {
                            status: String::from(STATUS_SUCCESS),
                            status_message: String::from(MSG_RESOURCE_FOUND),
                            branch,
                        })
                    }
                    Ok(None) => match api::local::branches::create(&repository, &data.name) {
                        Ok(branch) => {
                            // Set the remote to this server
                            HttpResponse::Ok().json(BranchResponse {
                                status: String::from(STATUS_SUCCESS),
                                status_message: String::from(MSG_RESOURCE_CREATED),
                                branch,
                            })
                        }
                        Err(err) => {
                            log::error!("Err api::local::branches::create: {:?}", err);
                            HttpResponse::InternalServerError()
                                .json(StatusMessage::internal_server_error())
                        }
                    },
                    Err(err) => {
                        log::error!("Err api::local::branches::create: {:?}", err);
                        HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error())
                    }
                }
            }
            Ok(None) => {
                log::debug!(
                    "404 api::local::branches::create_or_get could not get repo {}",
                    name,
                );
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::error!(
                    "Err api::local::branches::create could not get repo {} {:?}",
                    name,
                    err
                );
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        },
        Err(_) => HttpResponse::BadRequest().json(StatusMessage::error("Invalid body.")),
    }
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::api;
    use liboxen::constants::DEFAULT_BRANCH_NAME;
    use liboxen::error::OxenError;
    use liboxen::view::http::STATUS_SUCCESS;
    use liboxen::view::{BranchResponse, ListBranchesResponse};

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_branches_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let name = "Testing-Branches-1";
        test::create_local_repo(&sync_dir, name)?;
        let uri = format!("/repositories/{}/branches", name);
        let req = test::request_with_param(&sync_dir, &uri, "repo_name", name);

        let resp = controllers::branches::index(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListBranchesResponse = serde_json::from_str(text)?;
        assert_eq!(list.status, STATUS_SUCCESS);
        // Should have main branch initialized
        assert_eq!(list.branches.len(), 1);
        assert_eq!(list.branches.first().unwrap().name, DEFAULT_BRANCH_NAME);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_branches_index_multiple_branches() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, name)?;
        api::local::branches::create(&repo, "branch-1")?;
        api::local::branches::create(&repo, "branch-2")?;

        let uri = format!("/repositories/{}/branches", name);
        let req = test::request_with_param(&sync_dir, &uri, "repo_name", name);

        let resp = controllers::branches::index(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListBranchesResponse = serde_json::from_str(text)?;
        // main + branch-1 + branch-2
        assert_eq!(list.branches.len(), 3);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_branch_show() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let repo_name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, repo_name)?;
        let branch_name = "branch-1";
        api::local::branches::create(&repo, branch_name)?;

        let uri = format!("/repositories/{}/branches", repo_name);
        let req = test::request_with_two_params(
            &sync_dir,
            &uri,
            "repo_name",
            repo_name,
            "branch_name",
            branch_name,
        );

        let resp = controllers::branches::show(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let branch_resp: BranchResponse = serde_json::from_str(text)?;
        assert_eq!(branch_resp.branch.name, branch_name);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_branch_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let name = "Testing-Branches-Create";
        test::create_local_repo(&sync_dir, name)?;

        let data = r#"
        {
            "name": "My-Branch-Name"
        }"#;
        let uri = format!("/repositories/{}/branches", name);
        let req = test::request_with_param(&sync_dir, &uri, "repo_name", name);

        let resp = controllers::branches::create_or_get(req, String::from(data)).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();

        let repo_response: BranchResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.branch.name, "My-Branch-Name");

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
