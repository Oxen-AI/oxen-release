
use liboxen::api;

pub mod app_data;
pub mod controllers;
pub mod http;
pub mod test;
pub mod auth;

extern crate dotenv;
extern crate log;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use env_logger::Env;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let env = Env::default();
    env_logger::init_from_env(env);

    let host: &str = &api::endpoint::host();
    let port: u16 = api::endpoint::port()
        .parse::<u16>()
        .expect("Port must be number");
    println!("Running 🐂 server on {}:{}", host, port);

    let sync_dir = match std::env::var("SYNC_DIR") {
        Ok(dir) => dir,
        Err(_) => String::from("/tmp/oxen")
    };

    env_logger::init_from_env(Env::default().default_filter_or("info"));
    println!("Syncing to directory: {}", sync_dir);

    let data = app_data::SyncDir::from(&sync_dir);
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route(
                "/repositories/{name}/commits",
                web::get().to(controllers::commits::index),
            )
            .route(
                "/repositories/{name}/commits",
                web::post().to(controllers::commits::upload),
            )
            .route(
                "/repositories/{name}/entries",
                web::post().to(controllers::entries::create),
            )
            .route(
                "/repositories/{name}/{filename:.*}",
                web::get().to(controllers::repositories::get_file),
            )
            .route(
                "/repositories/{name}",
                web::get().to(controllers::repositories::show),
            )
            .route(
                "/repositories",
                web::get().to(controllers::repositories::index),
            )
            .route(
                "/repositories",
                web::post().to(controllers::repositories::create),
            )
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
    })
    .bind((host, port))?
    .run()
    .await
}
