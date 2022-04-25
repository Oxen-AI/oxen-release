use crate::app_data::SyncDir;

use liboxen::api;
use liboxen::model::{Entry, LocalRepository};
use liboxen::view::http::{MSG_RESOURCE_CREATED, STATUS_SUCCESS};
use liboxen::view::{EntryResponse, StatusMessage};
use serde::Deserialize;

use actix_web::{web, HttpRequest, HttpResponse};
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
    req: HttpRequest,
    body: web::Payload,
    data: web::Query<EntryQuery>,
) -> Result<HttpResponse, actix_web::Error> {
    let sync_dir = req.app_data::<SyncDir>().unwrap();

    // name of the repo
    let name: &str = req.match_info().get("name").unwrap();
    match api::local::repositories::get_by_name(&sync_dir.path, name) {
        Ok(local_repo) => {
            create_entry(&sync_dir.path, &local_repo, body, data).await
        },
        Err(err) => {
            let msg = format!("Could not find repo at path\nErr: {}", err);
            Ok(HttpResponse::BadRequest().json(StatusMessage::error(&msg)))
        }
    }
}

async fn create_entry(
    sync_dir: &Path,
    repository: &LocalRepository,
    mut body: web::Payload,
    data: web::Query<EntryQuery>,
) -> Result<HttpResponse, actix_web::Error> {
    let repo_dir = &sync_dir.join(&repository.name);

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
        let url = format!(
            "{}/{}",
            api::endpoint::url_from(&repository.name),
            &data.filename
        );

        Ok(HttpResponse::Ok().json(EntryResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_CREATED),
            entry: Entry {
                id: format!("{}", uuid::Uuid::new_v4()), // generate a new one on the server for now
                data_type: data_type_from_ext(extension.to_str().unwrap()),
                url,
                filename: String::from(&data.filename),
                hash: String::from(&data.hash),
            },
        }))
    } else {
        let msg = format!("Invalid file extension: {:?}", &data.filename);
        Ok(HttpResponse::BadRequest().json(StatusMessage::error(&msg)))
    }
}

fn data_type_from_ext(ext: &str) -> String {
    match ext {
        "jpg" | "png" => String::from("image"),
        "txt" => String::from("text"),
        _ => String::from("binary"),
    }
}

#[cfg(test)]
mod tests {

    use actix_web::{web, App};

    use liboxen::error::OxenError;
    use liboxen::util;
    use liboxen::view::EntryResponse;

    use crate::app_data::SyncDir;
    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_entries_create_text_file() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, name)?;

        let filename = "test.txt";
        let hash = "1234";
        let payload = "🐂 💨";
        let uri = format!(
            "/repositories/{}/entries?filename={}&hash={}",
            name, filename, hash
        );
        let app = actix_web::test::init_service(
            App::new()
                .app_data(SyncDir {
                    path: sync_dir.clone(),
                })
                .route(
                    "/repositories/{name}/entries",
                    web::post().to(controllers::entries::create),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .set_payload(payload)
            .to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let entry_resp: EntryResponse = serde_json::from_str(body)?;

        // Make sure entry gets populated
        assert_eq!(entry_resp.entry.filename, filename);
        assert_eq!(entry_resp.entry.hash, hash);

        // Make sure file actually exists on disk
        let repo_dir = sync_dir.join(repo.name);
        let uploaded_file = repo_dir.join(filename);
        assert!(uploaded_file.exists());
        // Make sure file contents are the same as the payload
        assert_eq!(util::fs::read_from_path(&uploaded_file)?, payload);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
