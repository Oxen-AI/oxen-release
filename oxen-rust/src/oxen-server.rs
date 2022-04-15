extern crate dotenv;

use liboxen::api;
use liboxen::api::local::RepositoryAPI;

use actix_web::middleware::Logger;
use env_logger::Env;
use actix_web::{web, App, HttpServer, HttpRequest, Result, Error};
use actix_files::NamedFile;

use std::path::{PathBuf, Path};

pub mod server;

use server::controllers;

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
        },
        Err(_) => {
            // gives a 404
            Ok(NamedFile::open("")?)
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
            .route("/repositories/{name}/commits", web::get().to(controllers::commits::list))
            .route("/repositories/{name}/commits", web::post().to(controllers::commits::upload))
            .route("/repositories/{name}/entries", web::post().to(controllers::entries::create))
            .route("/repositories/{name}/{filename:.*}", web::get().to(index))
            .route("/repositories/{name}", web::get().to(controllers::repositories::show))
            .route("/repositories", web::get().to(controllers::repositories::index))
            .route("/repositories", web::post().to(controllers::repositories::create))
            
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
    })
    .bind((host, port))?
    .run()
    .await
}
