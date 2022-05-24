use crate::app_data::OxenAppData;


use actix_web::{HttpRequest, HttpResponse};


use liboxen::api;
use liboxen::view::{
    ListBranchResponse, BranchResponse, BranchNew, StatusMessage,
};
use liboxen::view::http::{
    MSG_RESOURCE_CREATED, MSG_RESOURCE_DELETED, MSG_RESOURCE_FOUND, STATUS_SUCCESS,
};

pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let name: &str = req.match_info().get("name").unwrap();
    match api::local::repositories::get_by_name(&app_data.path, name) {
        Ok(repository) => {
            match api::local::branches::list(&repository) {
                Ok(branches) => {
                    let view = ListBranchResponse {
                        status: String::from(STATUS_SUCCESS),
                        status_message: String::from(MSG_RESOURCE_FOUND),
                        branches: branches,
                    };
                    HttpResponse::Ok().json(view)
                }
                Err(err) => {
                    log::error!("Unable to list repositories. Err: {}", err);
                    HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
                }
            }
        },
        Err(err) => {
            log::error!("Err api::local::branches::create could not get repo {} {:?}", name, err);
            HttpResponse::InternalServerError().json(StatusMessage::resource_not_found())
        }
    }

}

pub async fn create_or_get(req: HttpRequest, body: String) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let data: Result<BranchNew, serde_json::Error> = serde_json::from_str(&body);

    let name: &str = req.match_info().get("name").unwrap();
    match data {
        Ok(data) => match api::local::repositories::get_by_name(&app_data.path, name) {
            Ok(repository) => {
                match api::local::branches::get_by_name(&repository, &data.name) {
                    Ok(Some(branch)) => {
                        // Set the remote to this server
                        HttpResponse::Ok().json(BranchResponse {
                            status: String::from(STATUS_SUCCESS),
                            status_message: String::from(MSG_RESOURCE_FOUND),
                            branch: branch,
                        })
                    }
                    Ok(None) => match api::local::branches::create(&repository, &data.name) {
                        Ok(branch) => {
                            // Set the remote to this server
                            HttpResponse::Ok().json(BranchResponse {
                                status: String::from(STATUS_SUCCESS),
                                status_message: String::from(MSG_RESOURCE_CREATED),
                                branch: branch,
                            })
                        }
                        Err(err) => {
                            log::error!("Err api::local::branches::create: {:?}", err);
                            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
                        }
                    },
                    Err(err) => {
                        log::error!("Err api::local::branches::create: {:?}", err);
                        HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
                    }
                }
            },
            Err(err) => {
                log::error!("Err api::local::branches::create could not get repo {} {:?}", name, err);
                HttpResponse::InternalServerError().json(StatusMessage::resource_not_found())
            }
        },
        Err(_) => HttpResponse::BadRequest().json(StatusMessage::error("Invalid body.")),
    }
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::error::OxenError;

    use liboxen::constants::DEFAULT_BRANCH_NAME;
    use liboxen::view::http::STATUS_SUCCESS;
    use liboxen::view::{ListBranchResponse, BranchResponse};

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_branches_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let name = "Testing-Branches-1";
        test::create_local_repo(&sync_dir, name)?;
        let uri = format!("/repositories/{}/branches", name);
        let req = test::request_with_param(&sync_dir, &uri, "name", name);

        let resp = controllers::branches::index(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListBranchResponse = serde_json::from_str(text)?;
        assert_eq!(list.status, STATUS_SUCCESS);
        // Should have main branch initialized
        assert_eq!(list.branches.len(), 1);
        assert_eq!(list.branches.first().unwrap().name, DEFAULT_BRANCH_NAME);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    // #[actix_web::test]
    // async fn test_respository_index_multiple_repos() -> Result<(), OxenError> {
    //     let sync_dir = test::get_sync_dir()?;

    //     test::create_local_repo(&sync_dir, "Testing-1")?;
    //     test::create_local_repo(&sync_dir, "Testing-2")?;

    //     let req = test::request(&sync_dir, "/repositories");
    //     let resp = controllers::repositories::index(req).await;
    //     assert_eq!(resp.status(), http::StatusCode::OK);
    //     let body = to_bytes(resp.into_body()).await.unwrap();
    //     let text = std::str::from_utf8(&body).unwrap();
    //     let list: ListRemoteRepositoryResponse = serde_json::from_str(text)?;
    //     assert_eq!(list.repositories.len(), 2);

    //     // cleanup
    //     std::fs::remove_dir_all(sync_dir)?;

    //     Ok(())
    // }

    // #[actix_web::test]
    // async fn test_respository_show() -> Result<(), OxenError> {
    //     let sync_dir = test::get_sync_dir()?;

    //     let name = "Testing-Name";
    //     test::create_local_repo(&sync_dir, name)?;

    //     let uri = format!("/repositories/{}", name);
    //     let req = test::request_with_param(&sync_dir, &uri, "name", name);

    //     let resp = controllers::repositories::show(req).await;
    //     assert_eq!(resp.status(), http::StatusCode::OK);
    //     let body = to_bytes(resp.into_body()).await.unwrap();
    //     let text = std::str::from_utf8(&body).unwrap();
    //     let repo_response: RepositoryResponse = serde_json::from_str(text)?;
    //     assert_eq!(repo_response.status, STATUS_SUCCESS);
    //     assert_eq!(repo_response.repository.name, name);

    //     // cleanup
    //     std::fs::remove_dir_all(sync_dir)?;

    //     Ok(())
    // }

    // #[actix_web::test]
    // async fn test_respository_create() -> Result<(), OxenError> {
    //     let sync_dir = test::get_sync_dir()?;
    //     let data = r#"
    //     {
    //         "name": "Testing-Name"
    //     }"#;
    //     let req = test::request(&sync_dir, "/repositories");

    //     let resp = controllers::repositories::create_or_get(req, String::from(data)).await;
    //     assert_eq!(resp.status(), http::StatusCode::OK);
    //     let body = to_bytes(resp.into_body()).await.unwrap();
    //     let text = std::str::from_utf8(&body).unwrap();

    //     let repo_response: RepositoryResponse = serde_json::from_str(text)?;
    //     assert_eq!(repo_response.status, STATUS_SUCCESS);
    //     assert_eq!(repo_response.repository.name, "Testing-Name");

    //     // cleanup
    //     std::fs::remove_dir_all(sync_dir)?;

    //     Ok(())
    // }
}
