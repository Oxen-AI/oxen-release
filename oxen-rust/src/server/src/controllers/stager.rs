use crate::app_data::OxenAppData;

use liboxen::model::User;
use liboxen::view::http::{MSG_RESOURCE_CREATED, STATUS_SUCCESS};
use liboxen::view::{FilePathResponse, StatusMessage};
use liboxen::{api, index};

use actix_web::{web, HttpRequest, HttpResponse};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug)]
pub struct WriteOpts {
    append: bool,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AppendContents {
    contents: String,
    file_extension: String,
    user: User,
}

pub async fn stage(
    req: HttpRequest,
    data: web::Json<AppendContents>,
    query: web::Query<WriteOpts>,
) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let branch_name: &str = req.match_info().get("branch_name").unwrap();
    let directory: PathBuf = req.match_info().query("resource").parse().unwrap();
    let should_append: bool = query.append;

    log::debug!("stager::stage repo name {repo_name}/{branch_name} append? {should_append}");
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => match api::local::branches::get_by_name(&repo, &branch_name) {
            Ok(Some(branch)) => {
                log::debug!(
                    "stager::stage file branch_name [{}] in directory {:?}",
                    branch_name,
                    directory
                );

                match index::remote_stager::stage_file(
                    &repo,
                    &branch,
                    &directory,
                    &data.file_extension,
                    &data.contents,
                ) {
                    Ok(file_path) => HttpResponse::Ok().json(FilePathResponse {
                        status: String::from(STATUS_SUCCESS),
                        status_message: String::from(MSG_RESOURCE_CREATED),
                        path: file_path,
                    }),
                    Err(err) => {
                        log::error!("unable to update file {:?}. Err: {}", directory, err);
                        HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error())
                    }
                }
            }
            Ok(None) => {
                log::debug!("stager::stage could not find branch {:?}", branch_name);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::error!("unable to get branch {:?}. Err: {}", branch_name, err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        },
        Ok(None) => {
            log::debug!("stager::stage could not find repo with name {}", repo_name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("unable to get repo {:?}. Err: {}", repo_name, err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}
