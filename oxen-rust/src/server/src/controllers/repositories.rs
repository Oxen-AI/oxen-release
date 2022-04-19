use actix_web::{HttpResponse, HttpRequest};

use liboxen::model::{HTTPStatusMsg, RepositoryNew, SyncDir};
use crate::api::local::RepositoryAPI;

pub async fn index(req: HttpRequest) -> HttpResponse {
    let sync_dir = req.app_data::<SyncDir>();
    if let Some(dir) = sync_dir {
        let api = RepositoryAPI::new(&dir.path);
        let repositories = api.list();
        match repositories {
            Ok(repositories) => HttpResponse::Ok().json(repositories),
            Err(err) => {
                let msg = format!("Unable to list repositories. Err: {}", err);
                HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
            }
        }
    } else {
        let msg = format!("Sync dir not in data: {:?}", sync_dir);
        HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
    }
}
/*
pub async fn create(body: String) -> HttpResponse {
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let repository: Result<RepositoryNew, serde_json::Error> = serde_json::from_str(&body);
    match repository {
        Ok(repository) => {
            let api = RepositoryAPI::new(Path::new(&sync_dir));
            let repository = api.create(&repository);
            match repository {
                Ok(repository) => HttpResponse::Ok().json(repository),
                Err(err) => {
                    let msg = format!("Error: {:?}", err);
                    HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
                }
            }
        }
        Err(_) => HttpResponse::Ok().json(HTTPStatusMsg::error("Invalid body.")),
    }
}

pub async fn show(req: HttpRequest) -> HttpResponse {
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let api = RepositoryAPI::new(Path::new(&sync_dir));

    let path: Option<&str> = req.match_info().get("name");
    if let Some(path) = path {
        let response = api.get_by_path(Path::new(&path));
        match response {
            Ok(response) => HttpResponse::Ok().json(response),
            Err(err) => {
                let msg = format!("Err: {}", err);
                HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
            }
        }
    } else {
        let msg = "Could not find `name` param...";
        HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
    }
}

#[cfg(test)]
mod tests {
    
    use actix_web::{
        http::{self},
        test,
    };
    
    use actix_web::body::to_bytes;
    use crate::error::OxenError;
    use crate::server::controllers::RepositoryController;
    use crate::model::{RepositoryNew, ListRepositoriesResponse, RepositoryResponse};
    use crate::model::http_response::{
        STATUS_SUCCESS,
    };
    use crate::api::local::repositories::RepositoryAPI;

    use std::path::{PathBuf};

    fn get_sync_dir() -> PathBuf {
        let sync_dir = PathBuf::from(format!("/tmp/oxen/tests/{}", uuid::Uuid::new_v4()));
        std::env::set_var("SYNC_DIR", sync_dir.to_str().unwrap());
        sync_dir
    }

    #[actix_web::test]
    async fn test_respository_index_empty() -> Result<(), OxenError> {
        let sync_dir = get_sync_dir();
        let controller = RepositoryController::new(&sync_dir);

        let resp = controllers::repositories::index().await;
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
    async fn test_respository_show() -> Result<(), OxenError> {
        let sync_dir = get_sync_dir();

        let name = "Testing-Name";
        let api = RepositoryAPI::new(&sync_dir);
        let repo = RepositoryNew {name: String::from(name)};
        api.create(&repo)?;

        let uri = format!("/repositories/{}", name);
        let req = test::TestRequest::with_uri(&uri).param("name", name).to_http_request();
        
        let resp = controllers::repositories::show(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        println!("RESPONSE {}", text);
        let repo_response: RepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, name);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}*/