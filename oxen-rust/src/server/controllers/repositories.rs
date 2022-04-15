

use actix_web::{web, HttpResponse, Responder};

use liboxen::model::{HTTPStatusMsg, RepositoryNew};
use liboxen::api::local::RepositoryAPI;

use std::path::Path;

pub async fn index() -> impl Responder {
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let api = RepositoryAPI::new(Path::new(&sync_dir));
    let repositories = api.list();
    match repositories {
        Ok(repositories) => HttpResponse::Ok().json(repositories),
        Err(err) => {
            let msg = format!("Unable to list repositories. Err: {}", err);
            HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
        }
    }
}

pub async fn create(body: String) -> impl Responder {
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

pub async fn show(path_param: web::Path<String>) -> impl Responder {
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let api = RepositoryAPI::new(Path::new(&sync_dir));

    let path = path_param.into_inner();

    let response = api.get_by_path(Path::new(&path));
    match response {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(err) => {
            let msg = format!("Err: {}", err);
            HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
        }
    }
}
