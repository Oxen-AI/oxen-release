use crate::api;
use crate::api::local::RepositoryAPI;
use liboxen::http;
use liboxen::http::response::EntryResponse;
use liboxen::http::{MSG_RESOURCE_CREATED, STATUS_SUCCESS};
use liboxen::model::Entry;
use serde::Deserialize;

use actix_web::{web, Error, HttpResponse};
use futures_util::stream::StreamExt as _;

use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

#[derive(Deserialize, Debug)]
pub struct EntryQuery {
    filename: String,
    hash: String,
}

pub async fn create(
    path_param: web::Path<String>,
    mut body: web::Payload,
    data: web::Query<EntryQuery>,
) -> Result<HttpResponse, Error> {
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let api = RepositoryAPI::new(Path::new(&sync_dir));

    // path to the repo
    let path = path_param.into_inner();
    match api.get_by_path(Path::new(&path)) {
        Ok(result) => {
            let repo_dir = Path::new(&sync_dir).join(result.repository.name);

            let filepath = repo_dir.join(&data.filename);

            if let Some(parent) = filepath.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }
            }

            let mut file = File::create(&filepath)?;
            let mut total_bytes = 0;
            while let Some(item) = body.next().await {
                total_bytes += file.write(&item?)?;
            }
            if let Some(extension) = filepath.extension() {
                println!(
                    "Wrote {} bytes to {:?} with extension",
                    total_bytes, filepath,
                );
                let url = format!("{}/{}", api::endpoint::url_from(&path), &data.filename);

                Ok(HttpResponse::Ok().json(EntryResponse {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_CREATED),
                    entry: Entry {
                        id: format!("{}", uuid::Uuid::new_v4()), // generate a new one on the server for now
                        data_type: data_type_from_ext(extension.to_str().unwrap()),
                        url,
                        filename: data.filename.clone(),
                        hash: data.hash.clone(),
                    },
                }))
            } else {
                let msg = format!("Invalid file extension: {:?}", &data.filename);
                Ok(HttpResponse::BadRequest().json(http::StatusMessage::error(&msg)))
            }
        }
        Err(err) => {
            let msg = format!("Err: {}", err);
            Ok(HttpResponse::BadRequest().json(http::StatusMessage::error(&msg)))
        }
    }
}

fn data_type_from_ext(ext: &str) -> String {
    match ext {
        "jpg" | "png" => String::from("image"),
        "txt" => String::from("text"),
        _ => String::from("binary"),
    }
}
