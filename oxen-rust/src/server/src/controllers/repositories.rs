use crate::app_data::SyncDir;

use liboxen::api;

use liboxen::view::http::{
    MSG_RESOURCE_CREATED,
    MSG_RESOURCE_DELETED,
    MSG_RESOURCE_FOUND,
    STATUS_SUCCESS
};
use liboxen::view::{
    ListRemoteRepositoryResponse, RepositoryNew, RemoteRepositoryResponse, StatusMessage,
};

use liboxen::model::RemoteRepository;

use actix_files::NamedFile;
use actix_web::{HttpRequest, HttpResponse};
use std::path::PathBuf;

pub async fn index(req: HttpRequest) -> HttpResponse {
    let sync_dir = req.app_data::<SyncDir>().unwrap();
    match api::local::repositories::list(&sync_dir.path) {
        Ok(repos) => {
            let repos: Vec<RemoteRepository> = repos
                .iter()
                .map(|repo| RemoteRepository::from_local(&repo.clone()))
                .collect();
            let view = ListRemoteRepositoryResponse {
                status: String::from(STATUS_SUCCESS),
                status_message: String::from(MSG_RESOURCE_FOUND),
                repositories: repos,
            };
            HttpResponse::Ok().json(view)
        }
        Err(err) => {
            let msg = format!("Unable to list repositories. Err: {}", err);
            HttpResponse::Ok().json(StatusMessage::error(&msg))
        }
    }
}

pub async fn show(req: HttpRequest) -> HttpResponse {
    let sync_dir = req.app_data::<SyncDir>().unwrap();

    let name: Option<&str> = req.match_info().get("name");
    if let Some(name) = name {
        match api::local::repositories::get_by_name(&sync_dir.path, name) {
            Ok(repository) => HttpResponse::Ok().json(RemoteRepositoryResponse {
                status: String::from(STATUS_SUCCESS),
                status_message: String::from(MSG_RESOURCE_FOUND),
                repository: RemoteRepository::from_local(&repository),
            }),
            Err(err) => {
                let msg = format!("Err: {}", err);
                HttpResponse::Ok().json(StatusMessage::error(&msg))
            }
        }
    } else {
        let msg = "Could not find `name` param...";
        HttpResponse::Ok().json(StatusMessage::error(msg))
    }
}

pub async fn create_or_get(req: HttpRequest, body: String) -> HttpResponse {
    let sync_dir = req.app_data::<SyncDir>().unwrap();

    let data: Result<RepositoryNew, serde_json::Error> = serde_json::from_str(&body);
    match data {
        Ok(data) => match api::local::repositories::get_by_name(&sync_dir.path, &data.name) {
            Ok(repository) => {
                HttpResponse::Ok().json(RemoteRepositoryResponse {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_FOUND),
                    repository: RemoteRepository::from_local(&repository),
                })
            },
            Err(_) => match api::local::repositories::create(&sync_dir.path, &data.name) {
                Ok(repository) => HttpResponse::Ok().json(RemoteRepositoryResponse {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_CREATED),
                    repository: RemoteRepository::from_local(&repository),
                }),
                Err(err) => {
                    let msg = format!("Error: {:?}", err);
                    HttpResponse::Ok().json(StatusMessage::error(&msg))
                }
            }
        },
        Err(_) => HttpResponse::Ok().json(StatusMessage::error("Invalid body.")),
    }
}

pub async fn delete(req: HttpRequest) -> HttpResponse {
    let sync_dir = req.app_data::<SyncDir>().unwrap();

    let name: Option<&str> = req.match_info().get("name");
    if let Some(name) = name {
        match api::local::repositories::get_by_name(&sync_dir.path, name) {
            Ok(repository) => {
                
                HttpResponse::Ok().json(RemoteRepositoryResponse {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_DELETED),
                    repository: RemoteRepository::from_local(&repository),
                })
            },
            Err(err) => {
                let msg = format!("Err: {}", err);
                HttpResponse::Ok().json(StatusMessage::error(&msg))
            }
        }
    } else {
        let msg = "Could not find `name` param...";
        HttpResponse::Ok().json(StatusMessage::error(msg))
    }
}

pub async fn get_file(req: HttpRequest) -> Result<NamedFile, actix_web::Error> {
    let sync_dir = req.app_data::<SyncDir>().unwrap();

    let filepath: PathBuf = req.match_info().query("filename").parse().unwrap();
    let name: &str = req.match_info().get("name").unwrap();
    match api::local::repositories::get_by_name(&sync_dir.path, name) {
        Ok(repo) => {
            let full_path = repo.path.join(&filepath);
            Ok(NamedFile::open(full_path)?)
        }
        Err(_) => {
            // gives a 404
            Ok(NamedFile::open("")?)
        }
    }
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::error::OxenError;

    use liboxen::view::http::STATUS_SUCCESS;
    use liboxen::view::{ListRemoteRepositoryResponse, RepositoryResponse};

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_repository_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let req = test::request(&sync_dir, "/repositories");

        let resp = controllers::repositories::index(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRemoteRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 0);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_respository_index_multiple_repos() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        test::create_local_repo(&sync_dir, "Testing-1")?;
        test::create_local_repo(&sync_dir, "Testing-2")?;

        let req = test::request(&sync_dir, "/repositories");
        let resp = controllers::repositories::index(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRemoteRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 2);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_respository_show() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, name)?;

        let uri = format!("/repositories/{}", name);
        let req = test::request_with_param(&sync_dir, &uri, "name", name);

        let resp = controllers::repositories::show(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let repo_response: RepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, name);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_respository_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let data = r#"
        {
            "name": "Testing-Name"
        }"#;
        let req = test::request(&sync_dir, "/repositories");

        let resp = controllers::repositories::create_or_get(req, String::from(data)).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();

        let repo_response: RepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, "Testing-Name");

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
