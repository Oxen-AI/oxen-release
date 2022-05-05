
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
use actix_web_httpauth::middleware::HttpAuthentication;
use env_logger::Env;
use clap::{arg, Command};
use std::path::Path;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let sync_dir = match std::env::var("SYNC_DIR") {
        Ok(dir) => dir,
        Err(_) => String::from("/tmp/oxen_sync")
    };

    let command = Command::new("oxen-server")
        .version("0.0.1")
        .about("Oxen server")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .allow_invalid_utf8_for_external_subcommands(true)
        .subcommand(
            Command::new("start")
                .about("Starts server")
        )
        .subcommand(
            Command::new("add-user")
                .about("Generates a token for a user ")
                .arg(arg!(<EMAIL> "The email for the user"))
                .arg_required_else_help(true),
        );
    let matches = command.get_matches();

    match matches.subcommand() {
        Some(("start", _sub_matches)) => {
            let host: &str = &api::endpoint::host();
            let port: u16 = api::endpoint::port()
                .parse::<u16>()
                .expect("Port must be number");
            println!("Running 🐂 server on {}:{}", host, port);
            println!("Syncing to directory: {}", sync_dir);

            let data = app_data::SyncDir::from(&sync_dir);

            HttpServer::new(move || {
                App::new()
                    .app_data(data.clone())
                    .wrap(HttpAuthentication::bearer(auth::validator::validate))
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
        Some(("add-user", sub_matches)) => {
            let email = sub_matches.value_of("EMAIL").expect("required");
            let path = Path::new(&sync_dir);
            if let Ok(keygen) = auth::access_keys::KeyGenerator::new(&path) {
                if let Ok(token) = keygen.create(email) {
                    println!("Added user {} with token: {}", email, token);
                }
            }
            
            Ok(())
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachabe!()
    }
}
