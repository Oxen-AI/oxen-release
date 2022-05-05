use liboxen::api;
use liboxen::config::AuthConfig;
use liboxen::model::NewUser;

pub mod app_data;
pub mod auth;
pub mod controllers;
pub mod http;
pub mod test;

extern crate dotenv;
extern crate log;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use actix_web_httpauth::middleware::HttpAuthentication;
use clap::{Arg, Command};
use env_logger::Env;
use std::path::Path;

const ADD_USER_USAGE: &str =
    "Usage: `oxen-server add-user -e g@oxen.ai -n greg -o auth_config.toml`";

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let sync_dir = match std::env::var("SYNC_DIR") {
        Ok(dir) => dir,
        Err(_) => String::from("/tmp/oxen_sync"),
    };

    let command = Command::new("oxen-server")
        .version("0.0.1")
        .about("Oxen server")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .allow_invalid_utf8_for_external_subcommands(true)
        .subcommand(Command::new("start").about("Starts server"))
        .subcommand(
            Command::new("add-user")
                .about(ADD_USER_USAGE)
                .arg(
                    Arg::new("email")
                        .long("email")
                        .short('e')
                        .help("Users email address")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::new("name")
                        .long("name")
                        .short('n')
                        .help("Users name that will show up in the commits")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::new("output")
                        .long("output")
                        .short('o')
                        .value_name("auth_config.toml")
                        .default_value("auth_config.toml")
                        .default_missing_value("always")
                        .help("Where to write the output config file to give to the user")
                        .takes_value(true),
                ),
        );
    let matches = command.get_matches();

    let host: &str = &api::endpoint::host();
    let port: u16 = api::endpoint::port()
        .parse::<u16>()
        .expect("Port must be number");

    match matches.subcommand() {
        Some(("start", _sub_matches)) => {
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
            match (
                sub_matches.value_of("email"),
                sub_matches.value_of("name"),
                sub_matches.value_of("output"),
            ) {
                (Some(email), Some(name), Some(output)) => {
                    let path = Path::new(&sync_dir);
                    if let Ok(keygen) = auth::access_keys::KeyGenerator::new(path) {
                        let new_user = NewUser {
                            name: name.to_string(),
                            email: email.to_string(),
                        };
                        match keygen.create(&new_user) {
                            Ok(user) => {
                                let auth_config = AuthConfig {
                                    host: format!("{}:{}", host, port),
                                    user,
                                };
                                match auth_config.save(Path::new(output)) {
                                    Ok(_) => {
                                        println!("Saved config to: {}\n\nTo give user access have them put the file in home directory at ~/.oxen/auth_config.toml", output)
                                    }
                                    Err(err) => {
                                        eprintln!("Error saving config: {}", err)
                                    }
                                }
                            }
                            Err(err) => {
                                eprintln!("Error adding user: {}", err)
                            }
                        }
                    }
                }
                _ => {
                    eprintln!("{}", ADD_USER_USAGE)
                }
            }

            Ok(())
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachabe!()
    }
}
