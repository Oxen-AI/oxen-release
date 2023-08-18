use liboxen::config::UserConfig;

use liboxen::model::User;

pub mod app_data;
pub mod auth;
pub mod controllers;
pub mod errors;
pub mod helpers;
pub mod params;
pub mod routes;
pub mod tasks;
pub mod test;
pub mod view;

extern crate log;

use actix_web::middleware::{Condition, Logger};
use actix_web::{web, App, HttpServer};
use actix_web_httpauth::middleware::HttpAuthentication;

use clap::{Arg, Command};
use env_logger::Env;

use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;

use crate::tasks::post_push_complete::PostPushComplete;
use liboxen::constants::COMMIT_QUEUE_NAME;

const VERSION: &str = liboxen::constants::OXEN_VERSION;

const ADD_USER_USAGE: &str =
    "Usage: `oxen-server add-user -e <email> -n <name> -o user_config.toml`";

const START_SERVER_USAGE: &str = "Usage: `oxen-server start -i 0.0.0.0 -p 3000`";

const INVALID_PORT_MSG: &str = "Port must a valid number between 0-65535";

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("info,debug"));

    let sync_dir = match std::env::var("SYNC_DIR") {
        Ok(dir) => dir,
        Err(_) => String::from("data"),
    };

    // Redis polling worker setup
    async fn run_redis_poller() {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost/".to_string());
        let redis_client = redis::Client::open(redis_url).expect("Failed to connect to redis");

        let mut con = redis_client.get_connection().unwrap();
        loop {
            let outcome: Option<Vec<u8>>;
            {
                // Pop off of the queue
                outcome = redis::cmd("LPOP")
                    .arg(COMMIT_QUEUE_NAME)
                    .query(&mut con)
                    .unwrap();

                match outcome {
                    Some(data) => {
                        log::debug!("Got queue item: {:?}", data);
                        let task: PostPushComplete = bincode::deserialize(&data).unwrap();
                        println!("{:?}", task);
                        task.run();
                    }
                    None => {
                        sleep(Duration::from_millis(3000)).await;
                    }
                }
            }
        }
    }

    let command = Command::new("oxen-server")
        .version(VERSION)
        .about("Oxen Server")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
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
                        .action(clap::ArgAction::Set),
                )
                .arg(
                    Arg::new("port")
                        .long("port")
                        .short('p')
                        .default_value("3000")
                        .default_missing_value("always")
                        .help("What port to bind the server to")
                        .action(clap::ArgAction::Set),
                )
                .arg(
                    Arg::new("auth")
                        .long("auth")
                        .short('a')
                        .help("Start the server with token-based authentication enforced")
                        .action(clap::ArgAction::SetTrue),
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
                        .action(clap::ArgAction::Set),
                )
                .arg(
                    Arg::new("name")
                        .long("name")
                        .short('n')
                        .help("Users name that will show up in the commits")
                        .required(true)
                        .action(clap::ArgAction::Set),
                )
                .arg(
                    Arg::new("output")
                        .long("output")
                        .short('o')
                        .default_value("user_config.toml")
                        .default_missing_value("always")
                        .help("Where to write the output config file to give to the user")
                        .action(clap::ArgAction::Set),
                ),
        );
    let matches = command.get_matches();

    match matches.subcommand() {
        Some(("start", sub_matches)) => {
            match (
                sub_matches.get_one::<String>("ip"),
                sub_matches.get_one::<String>("port"),
            ) {
                (Some(host), Some(port)) => {
                    let port: u16 = port.parse::<u16>().expect(INVALID_PORT_MSG);
                    println!("🐂 v{VERSION}");
                    println!("Running on {host}:{port}");
                    println!("Syncing to directory: {sync_dir}");
                    let enable_auth = sub_matches.get_flag("auth");

                    // Poll for post-commit tasks in background
                    tokio::spawn(async { run_redis_poller().await });

                    let data = app_data::OxenAppData::from(&sync_dir);
                    HttpServer::new(move || {
                        App::new()
                            .app_data(data.clone())
                            .route("/api/version", web::get().to(controllers::version::index))
                            .route("/api/health", web::get().to(controllers::health::index))
                            .route(
                                "/api/namespaces",
                                web::get().to(controllers::namespaces::index),
                            )
                            .route(
                                "/api/namespaces/{namespace}",
                                web::get().to(controllers::namespaces::show),
                            )
                            .wrap(Condition::new(
                                enable_auth,
                                HttpAuthentication::bearer(auth::validator::validate),
                            ))
                            .service(web::scope("/api/repos").configure(routes::config))
                            .wrap(Logger::default())
                            .wrap(Logger::new("%a %{User-Agent}i"))
                    })
                    .bind((host.to_owned(), port))?
                    .run()
                    .await
                }
                _ => {
                    eprintln!("{START_SERVER_USAGE}");
                    Ok(())
                }
            }
        }
        Some(("add-user", sub_matches)) => {
            match (
                sub_matches.get_one::<String>("email"),
                sub_matches.get_one::<String>("name"),
                sub_matches.get_one::<String>("output"),
            ) {
                (Some(email), Some(name), Some(output)) => {
                    let path = Path::new(&sync_dir);
                    log::debug!("Saving to sync dir: {:?}", path);
                    if let Ok(keygen) = auth::access_keys::AccessKeyManager::new(path) {
                        let new_user = User {
                            name: name.to_string(),
                            email: email.to_string(),
                        };
                        match keygen.create(&new_user) {
                            Ok((user, token)) => {
                                let cfg = UserConfig::from_user(&user);
                                match cfg.save(Path::new(output)) {
                                    Ok(_) => {
                                        println!("User access token created:\n\n{token}\n\nTo give user access have them run the command `oxen config --auth <HOST> <TOKEN>`")
                                    }
                                    Err(error) => {
                                        eprintln!("Err: {error:?}");
                                    }
                                }
                            }
                            Err(err) => {
                                eprintln!("Err: {err}")
                            }
                        }
                    }
                }
                _ => {
                    eprintln!("{ADD_USER_USAGE}")
                }
            }

            Ok(())
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachabe!()
    }
}
