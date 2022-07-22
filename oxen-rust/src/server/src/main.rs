use liboxen::config::RemoteConfig;
use liboxen::model::NewUser;

pub mod app_data;
pub mod auth;
pub mod controllers;
pub mod test;

extern crate dotenv;
extern crate log;

// use actix_http::KeepAlive;
// use std::time;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use actix_web_httpauth::middleware::HttpAuthentication;
use clap::{Arg, Command};
use env_logger::Env;
use std::path::Path;

const VERSION: &str = env!("CARGO_PKG_VERSION");

const ADD_USER_USAGE: &str =
    "Usage: `oxen-server add-user -e <email> -n <name> -o auth_config.toml`";

const START_SERVER_USAGE: &str = "Usage: `oxen-server start -h 0.0.0.0 -p 3000`";

const INVALID_PORT_MSG: &str = "Port must a valid number between 0-65535";

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("info,debug"));

    let sync_dir = match std::env::var("SYNC_DIR") {
        Ok(dir) => dir,
        Err(_) => String::from("/tmp/oxen_sync"),
    };

    let command = Command::new("oxen-server")
        .version(VERSION)
        .about("Oxen Server")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .allow_invalid_utf8_for_external_subcommands(true)
        .subcommand(
            Command::new("start")
                .about(START_SERVER_USAGE)
                .arg(
                    Arg::new("ip")
                        .long("ip")
                        .short('i')
                        .default_value("0.0.0.0")
                        .default_missing_value("always")
                        .help("What host to bind the server to")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("port")
                        .long("port")
                        .short('p')
                        .default_value("3000")
                        .default_missing_value("always")
                        .help("What port to bind the server to")
                        .takes_value(true),
                ),
        )
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
                        .default_value("auth_config.toml")
                        .default_missing_value("always")
                        .help("Where to write the output config file to give to the user")
                        .takes_value(true),
                ),
        );
    let matches = command.get_matches();

    match matches.subcommand() {
        Some(("start", sub_matches)) => {
            match (sub_matches.value_of("ip"), sub_matches.value_of("port")) {
                (Some(host), Some(port)) => {
                    let port: u16 = port.parse::<u16>().expect(INVALID_PORT_MSG);
                    println!("🐂 v{}", VERSION);
                    println!("Running on {}:{}", host, port);
                    println!("Syncing to directory: {}", sync_dir);

                    let data = app_data::OxenAppData::from(&sync_dir);

                    HttpServer::new(move || {
                        App::new()
                            .app_data(data.clone())
                            .route(
                                "/version",
                                web::get().to(controllers::version::index),
                            )
                            .wrap(HttpAuthentication::bearer(auth::validator::validate))
                            .route(
                                "/oxen/{namespace}/{repo_name}/commits",
                                web::get().to(controllers::commits::index),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/commits/{commit_id}",
                                web::post().to(controllers::commits::upload),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/commits/{commit_id}",
                                web::get().to(controllers::commits::show),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/commits/{commit_id}/commit_db",
                                web::get().to(controllers::commits::download_commit_db),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/commits/{commit_id}/parents",
                                web::get().to(controllers::commits::parents),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/commits/{commit_id}/entries",
                                web::get().to(controllers::entries::list_entries),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/commits/{commit_id}/download_page",
                                web::get().to(controllers::entries::download_page),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/commits/{commit_id}/download_content_ids",
                                web::post().to(controllers::entries::download_content_ids),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/branches",
                                web::get().to(controllers::branches::index),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/branches",
                                web::post().to(controllers::branches::create_or_get),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/branches/{branch_name}",
                                web::get().to(controllers::branches::show),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/branches/{branch_name}/commits",
                                web::get().to(controllers::commits::index_branch),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/branches/{branch_name}/commits",
                                web::post().to(controllers::commits::create),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/entries",
                                web::post().to(controllers::entries::create),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}/commits/{commit_id}/entries/{filename:.*}",
                                web::get().to(controllers::repositories::get_file),
                            )
                            .route(
                                "/oxen/{namespace}",
                                web::get().to(controllers::repositories::index),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}",
                                web::get().to(controllers::repositories::show),
                            )
                            .route(
                                "/oxen/{namespace}/{repo_name}",
                                web::delete().to(controllers::repositories::delete),
                            )
                            .route(
                                "/oxen/{namespace}",
                                web::post().to(controllers::repositories::create),
                            )
                            .wrap(Logger::default())
                            .wrap(Logger::new("%a %{User-Agent}i"))
                    })
                    .bind((host, port))?
                    .run()
                    .await
                }
                _ => {
                    eprintln!("{}", START_SERVER_USAGE);
                    Ok(())
                }
            }
        }
        Some(("add-user", sub_matches)) => {
            match (
                sub_matches.value_of("email"),
                sub_matches.value_of("name"),
                sub_matches.value_of("output"),
            ) {
                (Some(email), Some(name), Some(output)) => {
                    let path = Path::new(&sync_dir);
                    if let Ok(keygen) = auth::access_keys::AccessKeyManager::new(path) {
                        let new_user = NewUser {
                            name: name.to_string(),
                            email: email.to_string(),
                        };
                        match keygen.create(&new_user) {
                            Ok(user) => {
                                let remote_config = RemoteConfig::default()
                                    .expect(liboxen::error::REMOTE_CFG_NOT_FOUND);
                                let auth_config = remote_config.to_auth(&user);
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
