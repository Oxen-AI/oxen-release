extern crate dotenv;

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use liboxen::api;
use liboxen::api::local::RepositoryAPI;
use liboxen::model::{HTTPErrorMsg, RepositoryNew};

use actix_web::middleware::Logger;
use env_logger::Env;
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

async fn repository_show(path_param: web::Path<String>) -> impl Responder {
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let api = RepositoryAPI::new(Path::new(&sync_dir));

    let path = path_param.into_inner();
    println!("GOT PATH {:?}", path);

    // path.into_inner().0

    let response = api.get_by_path(Path::new(&path));
    match response {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(err) => {
            let msg = format!("Err: {}", err);
            HttpResponse::Ok().json(HTTPErrorMsg::with_message(&msg))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let host: &str = &api::endpoint::host();
    let port: u16 = api::endpoint::port()
        .parse::<u16>()
        .expect("Port must be number");
    println!("Running 🐂 server on {}:{}", host, port);

    env_logger::init_from_env(Env::default().default_filter_or("info"));

    HttpServer::new(|| {
        App::new()
            .service(web::resource("/repositories/{name}").route(web::get().to(repository_show)))
            .route("/repositories", web::get().to(repositories_index))
            .route("/repositories", web::post().to(repositories_create))
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
    })
    .bind((host, port))?
    .run()
    .await
}
