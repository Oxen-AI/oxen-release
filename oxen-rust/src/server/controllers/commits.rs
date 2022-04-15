

use actix_web::{web, Error, HttpResponse, Responder};

use liboxen::model::{CommitMsg, CommitMsgResponse, HTTPStatusMsg};
use liboxen::api::local::RepositoryAPI;
use liboxen::cli::indexer::OXEN_HIDDEN_DIR;
use liboxen::cli::Committer;
use liboxen::model::http_response::{
    MSG_RESOURCE_CREATED, STATUS_SUCCESS,
};

use std::path::Path;

use futures_util::stream::StreamExt as _;
use serde::Deserialize;
use flate2::read::GzDecoder;
use tar::Archive;


#[derive(Deserialize, Debug)]
pub struct CommitQuery {
    commit_id: String,
    parent_id: Option<String>,
    message: String,
    author: String,
    date: String,
}

pub async fn list(path_param: web::Path<String>) -> impl Responder {
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

pub async fn upload(
    path_param: web::Path<String>,
    mut body: web::Payload,
    data: web::Query<CommitQuery>,
) -> Result<HttpResponse, Error> {
    let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
    let api = RepositoryAPI::new(Path::new(&sync_dir));

    // path to the repo
    let path = path_param.into_inner();
    match api.get_by_path(Path::new(&path)) {
        Ok(result) => {
            let repo_dir = Path::new(&sync_dir).join(result.repository.name);
            let hidden_dir = repo_dir.join(OXEN_HIDDEN_DIR);

            // Create Commit
            let commit = CommitMsg {
                id: data.commit_id.clone(),
                parent_id: data.parent_id.clone(),
                message: data.message.clone(),
                author: data.author.clone(),
                date: CommitMsg::date_from_str(&data.date),
            };
            create_commit(&repo_dir, &commit);
            
            // Get tar.gz bytes for history/COMMIT_ID data
            let mut bytes = web::BytesMut::new();
            while let Some(item) = body.next().await {
                bytes.extend_from_slice(&item?);
            }

            // Unpack tarball
            let mut archive = Archive::new(GzDecoder::new(&bytes[..]));
            archive.unpack(hidden_dir)?;

            Ok(HttpResponse::Ok().json(CommitMsgResponse {
                status: String::from(STATUS_SUCCESS),
                status_message: String::from(MSG_RESOURCE_CREATED),
                commit: commit
            }))
        }
        Err(err) => {
            let msg = format!("Err: {}", err);
            Ok(HttpResponse::Ok().json(HTTPStatusMsg::error(&msg)))
        }
    }
}

fn create_commit(repo_dir: &Path, commit: &CommitMsg) {
    match Committer::new(&repo_dir) {
        Ok(committer) => {
            match committer.add_commit_to_db(&commit) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("Error adding commit to db: {:?}", err);
                }
            }
        }
        Err(err) => {
            eprintln!("Error creating committer: {:?}", err);
        }
    };
}