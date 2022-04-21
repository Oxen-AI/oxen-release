use actix_web::{HttpRequest, HttpResponse};

use liboxen::api::local::RepositoryAPI;
use liboxen::http;
use liboxen::model::RepositoryNew;

use crate::app_data::SyncDir;

use actix_files::NamedFile;
use std::path::{Path, PathBuf};

pub async fn index(req: HttpRequest) -> HttpResponse {
    let sync_dir = req.app_data::<SyncDir>().unwrap();
    let api = RepositoryAPI::new(&sync_dir.path);
    let repositories = api.list();
    match repositories {
        Ok(repositories) => HttpResponse::Ok().json(repositories),
        Err(err) => {
            let msg = format!("Unable to list repositories. Err: {}", err);
            HttpResponse::Ok().json(http::StatusMessage::error(&msg))
        }
    }
}

pub async fn show(req: HttpRequest) -> HttpResponse {
    let sync_dir = req.app_data::<SyncDir>().unwrap();

    let api = RepositoryAPI::new(Path::new(&sync_dir.path));
    let path: Option<&str> = req.match_info().get("name");
    if let Some(path) = path {
        let response = api.get_by_path(Path::new(&path));
        match response {
            Ok(response) => HttpResponse::Ok().json(response),
            Err(err) => {
                let msg = format!("Err: {}", err);
                HttpResponse::Ok().json(http::StatusMessage::error(&msg))
            }
        }
    } else {
        let msg = "Could not find `name` param...";
        HttpResponse::Ok().json(http::StatusMessage::error(msg))
    }
}

pub async fn create(req: HttpRequest, body: String) -> HttpResponse {
    let sync_dir = req.app_data::<SyncDir>().unwrap();

    let repository: Result<RepositoryNew, serde_json::Error> = serde_json::from_str(&body);
    match repository {
        Ok(repository) => {
            let api = RepositoryAPI::new(Path::new(&sync_dir.path));
            let repository = api.create(&repository);
            match repository {
                Ok(repository) => HttpResponse::Ok().json(repository),
                Err(err) => {
                    let msg = format!("Error: {:?}", err);
                    HttpResponse::Ok().json(http::StatusMessage::error(&msg))
                }
            }
        }
        Err(_) => HttpResponse::Ok().json(http::StatusMessage::error("Invalid body.")),
    }
}

pub async fn get_file(req: HttpRequest) -> Result<NamedFile, actix_web::Error> {
    let sync_dir = req.app_data::<SyncDir>().unwrap();

    let filepath: PathBuf = req.match_info().query("filename").parse().unwrap();
    let repo_path: PathBuf = req.match_info().query("name").parse().unwrap();

    let api = RepositoryAPI::new(Path::new(&sync_dir.path));
    match api.get_by_path(Path::new(&repo_path)) {
        Ok(result) => {
            let repo_dir = Path::new(&sync_dir.path).join(result.repository.name);
            let full_path = repo_dir.join(&filepath);
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

    use liboxen::http::response::{ListRepositoriesResponse, RepositoryResponse};
    use liboxen::http::STATUS_SUCCESS;

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_respository_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir();

        let req = test::request(&sync_dir, "/repositories");

        let resp = controllers::repositories::index(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoriesResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 0);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_respository_index_multiple_repos() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir();

        test::create_repo(&sync_dir, "Testing-1")?;
        test::create_repo(&sync_dir, "Testing-2")?;

        let req = test::request(&sync_dir, "/repositories");
        let resp = controllers::repositories::index(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoriesResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 2);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_respository_show() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir();

        let name = "Testing-Name";
        test::create_repo(&sync_dir, name)?;

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
        let sync_dir = test::get_sync_dir();
        let data = r#"
        {
            "name": "Testing-Name"
        }"#;
        let req = test::request(&sync_dir, "/repositories");

        let resp = controllers::repositories::create(req, String::from(data)).await;
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
