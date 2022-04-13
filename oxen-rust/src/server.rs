extern crate dotenv;

use actix_web::{web, App, Error, HttpResponse, HttpServer, Responder};

use futures_util::stream::StreamExt as _;
use serde::Deserialize;
use std::io::Write;

use chrono::{DateTime, NaiveDateTime, Utc};
use liboxen::api;
use liboxen::api::local::RepositoryAPI;
use liboxen::cli::indexer::OXEN_HIDDEN_DIR;
use liboxen::cli::Committer;
use liboxen::model::{CommitMsg, HTTPStatusMsg, RepositoryNew};

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
            HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
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
                    HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
                }
            }
        }
        Err(_) => HttpResponse::Ok().json(HTTPStatusMsg::error("Invalid body.")),
    }
}

async fn repository_show(path_param: web::Path<String>) -> impl Responder {
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

async fn commit_list(path_param: web::Path<String>) -> impl Responder {
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

#[derive(Deserialize)]
struct CommitQuery {
    filename: String,
    commit_id: String,
    parent_id: Option<String>,
    message: String,
    author: String,
    date: String,
}

// TODO: API to create commit, given a commit object, and a zipped rocksdb file,
// create the proper dirs, unzip to the history dir, and add an entry to the commits db
async fn commit_upload(
    path_param: web::Path<String>,
    mut body: web::Payload,
    data: web::Query<CommitQuery>,
) -> Result<HttpResponse, Error> {
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let api = RepositoryAPI::new(Path::new(&sync_dir));

    let path = path_param.into_inner();
    println!("commit_upload path: {:?}", path);
    println!("commit_upload filename: {:?}", data.filename);

    let response = api.get_by_path(Path::new(&path));
    match response {
        Ok(response) => {
            let repo_dir = Path::new(&sync_dir).join(response.repository.name);
            let hidden_dir = repo_dir.join(OXEN_HIDDEN_DIR);
            let outfile = hidden_dir.join(&data.filename);

            // Create Commit
            match Committer::new(&repo_dir) {
                Ok(committer) => {
                    let no_timezone =
                        NaiveDateTime::parse_from_str(&data.date, "%Y-%m-%d %H:%M:%S").unwrap();

                    let commit = CommitMsg {
                        id: data.commit_id.clone(),
                        parent_id: data.parent_id.clone(),
                        message: data.message.clone(),
                        author: data.author.clone(),
                        date: DateTime::<Utc>::from_utc(no_timezone, Utc),
                    };
                    match committer.add_commit_to_db(&commit) {
                        Ok(_) => {
                            println!("Added commit to db!");
                        }
                        Err(err) => {
                            eprintln!("Error adding commit to db: {:?}", err);
                        }
                    }
                }
                Err(err) => {
                    eprintln!("Error creating committer: {:?}", err);
                }
            };

            // Write data blob to file to unzip
            println!("Writing to file: {:?}", outfile);
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&outfile)
                .unwrap();

            // let mut bytes = web::BytesMut::new();
            let mut total_bytes = 0;
            while let Some(item) = body.next().await {
                // bytes.extend_from_slice(&item?);
                let bytes = &item?;
                total_bytes += file.write(bytes)?;
            }

            let response_str = format!("Wrote {:?} bytes to {:?}", total_bytes, outfile);

            Ok(HttpResponse::Ok().json(HTTPStatusMsg::success(&response_str)))
        }
        Err(err) => {
            let msg = format!("Err: {}", err);
            Ok(HttpResponse::Ok().json(HTTPStatusMsg::error(&msg)))
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
            .service(
                web::resource("/repositories/{name}/commits")
                    .route(web::get().to(commit_list))
                    .route(web::post().to(commit_upload)),
            )
            .route("/repositories", web::get().to(repositories_index))
            .route("/repositories", web::post().to(repositories_create))
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
    })
    .bind((host, port))?
    .run()
    .await
}
