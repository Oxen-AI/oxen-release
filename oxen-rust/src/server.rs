extern crate dotenv;

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use liboxen::api;
use liboxen::api::local::RepositoryAPI;
use liboxen::model::{HTTPErrorMsg, RepositoryNew};

use std::path::Path;

async fn repositories_index() -> impl Responder {
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let api = RepositoryAPI::new(Path::new(&sync_dir));
    let repositories = api.list();
    match repositories {
        Ok(repositories) => HttpResponse::Ok().json(repositories),
        Err(err) => {
            let msg = format!("Unable to list repositories. Err: {}", err);
            HttpResponse::Ok().json(HTTPErrorMsg::with_message(&msg))
        }
    }
}

async fn repositories_create(body: String) -> impl Responder {
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
                    HttpResponse::Ok().json(HTTPErrorMsg::with_message(&msg))
                }
            }
        }
        Err(_) => HttpResponse::Ok().json(HTTPErrorMsg::with_message("Invalid body.")),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let host: &str = &api::endpoint::host();
    let port: u16 = api::endpoint::port()
        .parse::<u16>()
        .expect("Port must be number");
    println!("Running 🐂 server on {}:{}", host, port);
    HttpServer::new(|| {
        App::new()
            .route("/repositories", web::get().to(repositories_index))
            .route("/repositories", web::post().to(repositories_create))
    })
    .bind((host, port))?
    .run()
    .await
}
