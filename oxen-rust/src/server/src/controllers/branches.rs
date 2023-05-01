use crate::app_data::OxenAppData;

use actix_web::{HttpRequest, HttpResponse};

use liboxen::api;
use liboxen::view::http::{
    MSG_RESOURCE_CREATED, MSG_RESOURCE_DELETED, MSG_RESOURCE_FOUND, MSG_RESOURCE_UPDATED,
    STATUS_SUCCESS,
};
use liboxen::view::{BranchNew, BranchResponse, BranchUpdate, ListBranchesResponse, StatusMessage};

pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
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
                "404 api::local::branches::index could not get repo {}",
                name,
            );
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!(
                "Err api::local::branches::index could not get repo {} {:?}",
                name,
                err
            );
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

pub async fn show(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    let branch_name: Option<&str> = req.match_info().get("branch_name");
    if let (Some(namespace), Some(name), Some(branch_name)) = (namespace, name, branch_name) {
        match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
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
        let msg = "Must supply `namespace`, `repo_name` and `branch_name` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn create_or_get(req: HttpRequest, body: String) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    println!("controllers::branches::create_or_get() got body: {body}");
    let data: Result<BranchNew, serde_json::Error> = serde_json::from_str(&body);

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    match data {
        Ok(data) => match api::local::repositories::get_by_namespace_and_name(
            &app_data.path,
            namespace,
            name,
        ) {
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
                    Ok(None) => {
                        match api::local::branches::create_from_head(&repository, &data.name) {
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
                        }
                    }
                    Err(err) => {
                        log::error!(
                            "Err api::local::branches::create_or_get get_by_name {:?}",
                            err
                        );
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
                    "Err api::local::branches::create_or_get could not get repo {} {:?}",
                    name,
                    err
                );
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        },
        Err(_) => HttpResponse::BadRequest().json(StatusMessage::error("Invalid body.")),
    }
}

pub async fn delete(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    let branch_name: Option<&str> = req.match_info().get("branch_name");
    if let (Some(name), Some(namespace), Some(branch_name)) = (name, namespace, branch_name) {
        match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(repository)) => {
                match api::local::branches::get_by_name(&repository, branch_name) {
                    Ok(Some(branch)) => {
                        match api::local::branches::force_delete(&repository, branch_name) {
                            Ok(_) => HttpResponse::Ok().json(BranchResponse {
                                status: String::from(STATUS_SUCCESS),
                                status_message: String::from(MSG_RESOURCE_DELETED),
                                branch,
                            }),
                            Err(err) => {
                                log::error!("Delete could not delete branch: {}", err);
                                HttpResponse::InternalServerError()
                                    .json(StatusMessage::internal_server_error())
                            }
                        }
                    }
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
                log::debug!("404 Could not find repo: {}", name);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::error!("Delete could not find repo: {}", err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Neet to supply `name`, `namespace`, and `branch` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn update(req: HttpRequest, body: String) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    let branch_name: Option<&str> = req.match_info().get("branch_name");
    let data: Result<BranchUpdate, serde_json::Error> = serde_json::from_str(&body);

    if let (Some(namespace), Some(name), Some(branch_name)) = (namespace, name, branch_name) {
        match data {
            Ok(data) => match api::local::repositories::get_by_namespace_and_name(
                &app_data.path,
                namespace,
                name,
            ) {
                Ok(Some(repo)) => {
                    match api::local::branches::update(&repo, branch_name, &data.commit_id) {
                        Ok(branch) => HttpResponse::Ok().json(BranchResponse {
                            status: String::from(STATUS_SUCCESS),
                            status_message: String::from(MSG_RESOURCE_UPDATED),
                            branch,
                        }),
                        Err(err) => {
                            log::debug!("Error updating branch {}: {}", branch_name, err);
                            HttpResponse::InternalServerError()
                                .json(StatusMessage::internal_server_error())
                        }
                    }
                }
                Ok(None) => {
                    log::debug!(
                        "404 api::local::branches::update could not get repo {}",
                        name,
                    );
                    HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                }
                Err(err) => {
                    log::debug!("Could not find repo [{}]: {}", name, err);
                    HttpResponse::NotFound().json(StatusMessage::internal_server_error())
                }
            },
            Err(err) => {
                log::debug!("Could not parse body: {}", err);
                HttpResponse::BadRequest().json(StatusMessage::error("Invalid body."))
            }
        }
    } else {
        let msg = "Must supply `namespace`, `repo_name` and `branch_name` params";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
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
    async fn test_controllers_branches_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Branches-1";
        test::create_local_repo(&sync_dir, namespace, name)?;
        let uri = format!("/oxen/{namespace}/{name}/branches");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

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
    async fn test_controllers_branches_index_multiple_branches() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;
        api::local::branches::create_from_head(&repo, "branch-1")?;
        api::local::branches::create_from_head(&repo, "branch-2")?;

        let uri = format!("/oxen/{namespace}/{name}/branches");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

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
    async fn test_controllers_branch_show() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let branch_name = "branch-1";
        api::local::branches::create_from_head(&repo, branch_name)?;

        let uri = format!("/oxen/{namespace}/{repo_name}/branches");
        let req = test::repo_request_with_param(
            &sync_dir,
            &uri,
            namespace,
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
    async fn test_controllers_branch_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Branches-Create";
        test::create_local_repo(&sync_dir, namespace, name)?;

        let data = r#"
        {
            "name": "My-Branch-Name"
        }"#;
        let uri = format!("/oxen/{namespace}/{name}/branches");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

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
