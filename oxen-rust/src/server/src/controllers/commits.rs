// use actix_web::{web, Error, HttpResponse, HttpRequest};

// use crate::api::local::RepositoryAPI;
// use crate::index::indexer::OXEN_HIDDEN_DIR;
// use crate::index::Committer;
// use crate::model::http_response::{MSG_RESOURCE_CREATED, STATUS_SUCCESS};
// use crate::model::{CommitMsg, CommitMsgResponse, HTTPStatusMsg};

// use std::path::Path;

// use flate2::read::GzDecoder;
// use futures_util::stream::StreamExt as _;
// use serde::Deserialize;
// use tar::Archive;

// #[derive(Deserialize, Debug)]
// pub struct CommitQuery {
//     commit_id: String,
//     parent_id: Option<String>,
//     message: String,
//     author: String,
//     date: String,
// }

// // List commits for a repository
// pub async fn index(req: HttpRequest) -> HttpResponse {
//     let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
//     let api = RepositoryAPI::new(Path::new(&sync_dir));

//     println!("GOT REQ {:?}", req);

//     let path: Option<&str> = req.match_info().get("name");
//     if let Some(path) = path {
//         let response = api.get_by_path(Path::new(path));
//         match response {
//             Ok(response) => HttpResponse::Ok().json(response),
//             Err(err) => {
//                 let msg = format!("Err: {}", err);
//                 HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
//             }
//         }
//     } else {
//         let msg = "Could not find `name` param...";
//         HttpResponse::Ok().json(HTTPStatusMsg::error(&msg))
//     }
// }

// pub async fn upload(
//     path_param: web::Path<String>,
//     mut body: web::Payload,
//     data: web::Query<CommitQuery>,
// ) -> Result<HttpResponse, Error> {
//     let sync_dir = std::env::var("SYNC_DIR").expect("Set env SYNC_DIR");
//     let api = RepositoryAPI::new(Path::new(&sync_dir));

//     // path to the repo
//     let path = path_param.into_inner();
//     match api.get_by_path(Path::new(&path)) {
//         Ok(result) => {
//             let repo_dir = Path::new(&sync_dir).join(result.repository.name);
//             let hidden_dir = repo_dir.join(OXEN_HIDDEN_DIR);

//             // Create Commit
//             let commit = CommitMsg {
//                 id: data.commit_id.clone(),
//                 parent_id: data.parent_id.clone(),
//                 message: data.message.clone(),
//                 author: data.author.clone(),
//                 date: CommitMsg::date_from_str(&data.date),
//             };
//             create_commit(&repo_dir, &commit);

//             // Get tar.gz bytes for history/COMMIT_ID data
//             let mut bytes = web::BytesMut::new();
//             while let Some(item) = body.next().await {
//                 bytes.extend_from_slice(&item?);
//             }

//             // Unpack tarball
//             let mut archive = Archive::new(GzDecoder::new(&bytes[..]));
//             archive.unpack(hidden_dir)?;

//             Ok(HttpResponse::Ok().json(CommitMsgResponse {
//                 status: String::from(STATUS_SUCCESS),
//                 status_message: String::from(MSG_RESOURCE_CREATED),
//                 commit,
//             }))
//         }
//         Err(err) => {
//             let msg = format!("Err: {}", err);
//             Ok(HttpResponse::Ok().json(HTTPStatusMsg::error(&msg)))
//         }
//     }
// }

// fn create_commit(repo_dir: &Path, commit: &CommitMsg) {
//     let result = Committer::new(repo_dir);
//     match result {
//         Ok(mut committer) => match committer.add_commit_to_db(commit) {
//             Ok(_) => {}
//             Err(err) => {
//                 eprintln!("Error adding commit to db: {:?}", err);
//             }
//         },
//         Err(err) => {
//             eprintln!("Error creating committer: {:?}", err);
//         }
//     };
// }


// #[cfg(test)]
// mod tests {
    
//     use actix_web::{
//         http::{self},
//         test,
//     };
    
//     use actix_web::body::to_bytes;
//     use liboxen::error::OxenError;
//     use crate::server::controllers;
//     use liboxen::model::ListCommitMsgResponse;

//     use std::path::{PathBuf};

//     fn get_sync_dir() -> PathBuf {
//         let sync_dir = PathBuf::from(format!("/tmp/oxen/tests/{}", uuid::Uuid::new_v4()));
//         std::env::set_var("SYNC_DIR", sync_dir.to_str().unwrap());
//         sync_dir
//     }

//     #[actix_web::test]
//     async fn test_respository_commits_index_empty() -> Result<(), OxenError> {
//         let sync_dir = get_sync_dir();

//         let name = "Testing-Name";
//         let uri = format!("/repositories/{}", name);
//         let req = test::TestRequest::with_uri(&uri).param("name", name).to_http_request();
        
//         let resp = controllers::commits::index(req).await;
//         assert_eq!(resp.status(), http::StatusCode::OK);
//         let body = to_bytes(resp.into_body()).await.unwrap();
//         let text = std::str::from_utf8(&body).unwrap();
//         let list: ListCommitMsgResponse = serde_json::from_str(text)?;
//         assert_eq!(list.commits.len(), 0);

//         // cleanup
//         std::fs::remove_dir_all(sync_dir)?;

//         Ok(())
//     }
// }