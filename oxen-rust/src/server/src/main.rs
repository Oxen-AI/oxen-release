extern crate dotenv;

use liboxen::api;
use liboxen::api::local::RepositoryAPI;
use liboxen::model::{HTTPStatusMsg, SyncDir};

pub mod controllers;

use actix_files::NamedFile;
use actix_web::middleware::Logger;

use actix_web::{
    web, App, Error,
    HttpRequest, HttpResponse, HttpServer,
    Result
};
use env_logger::Env;
use std::path::{Path, PathBuf};

async fn index(req: HttpRequest) -> Result<NamedFile, Error> {
    println!("GOT FILE REQUEST");
    let filepath: PathBuf = req.match_info().query("filename").parse().unwrap();
    let repo_path: PathBuf = req.match_info().query("name").parse().unwrap();
    println!("looking for {:?} in repo {:?}", filepath, repo_path);
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let api = RepositoryAPI::new(Path::new(&sync_dir));
    match api.get_by_path(Path::new(&repo_path)) {
        Ok(result) => {
            let repo_dir = Path::new(&sync_dir).join(result.repository.name);
            let full_path = repo_dir.join(&filepath);
            Ok(NamedFile::open(full_path)?)
        }
        Err(_) => {
            // gives a 404
            Ok(NamedFile::open("")?)
        }
    }
}

async fn test_app_data(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<SyncDir>();
    if let Some(data) = app_data {
        println!("GOT DATA {:?}", data);
        HttpResponse::Ok().json(HTTPStatusMsg::success("Got data!"))
    } else {
        println!("WTF... {:?}", app_data);
        HttpResponse::Ok().json(HTTPStatusMsg::error("unimplemented"))
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let host: &str = &api::endpoint::host();
    let port: u16 = api::endpoint::port()
        .parse::<u16>()
        .expect("Port must be number");
    println!("Running 🐂 server on {}:{}", host, port);

    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let data = SyncDir::from(&sync_dir);
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            // .route(
            //     "/repositories/{name}/commits",
            //     web::get().to(controllers::commits::index),
            // )
            // .route(
            //     "/repositories/{name}/commits",
            //     web::post().to(controllers::commits::upload),
            // )
            // .route(
            //     "/repositories/{name}/entries",
            //     web::post().to(controllers::entries::create),
            // )
            .route("/repositories/{name}/{filename:.*}", web::get().to(index))
            // .route(
            //     "/repositories/{name}",
            //     web::get().to(controllers::repositories::show),
            // )
            .route(
                "/repositories",
                web::get().to(controllers::repositories::index),
            )
            // .route(
            //     "/repositories",
            //     web::post().to(controllers::repositories::create),
            // )
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
    })
    .bind((host, port))?
    .run()
    .await
}
