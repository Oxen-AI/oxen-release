use crate::app_data::OxenAppData;
use crate::controllers::entries::PageNumQuery;

use liboxen::api;
use liboxen::model::{Commit, LocalRepository};
use liboxen::util;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{PaginatedDirEntries, StatusMessage};

use actix_web::{web, HttpRequest, HttpResponse};

use std::path::{Path, PathBuf};

pub async fn get(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    log::debug!("got here");
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();

    // default to first page with first ten values
    let page_num: usize = query.page_num.unwrap_or(1);
    let page_size: usize = query.page_size.unwrap_or(10);

    log::debug!(
        "dir::get repo name [{}] resource [{:?}] page_num {} page_size {}",
        name,
        resource,
        page_num,
        page_size,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            log::debug!("dir::get repo [{}]", name);
            if let Ok(Some((commit_id, filepath))) =
                util::resource::parse_resource(&repo, &resource)
            {
                log::debug!(
                    "dir::get commit_id [{}] and filepath {:?}",
                    commit_id,
                    filepath
                );
                match list_directory_for_commit(&repo, &commit_id, &filepath, page_num, page_size) {
                    Ok((entries, _commit)) => HttpResponse::Ok().json(entries),
                    Err(status_message) => HttpResponse::InternalServerError().json(status_message),
                }
            } else {
                log::debug!("dir::get could not find resource from uri {:?}", resource);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
        }
        Ok(None) => {
            log::debug!("dir::get could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("unable to get directory {:?}. Err: {}", resource, err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

fn list_directory_for_commit(
    repo: &LocalRepository,
    commit_id: &str,
    directory: &Path,
    page_num: usize,
    page_size: usize,
) -> Result<(PaginatedDirEntries, Commit), StatusMessage> {
    match api::local::commits::get_by_id(repo, commit_id) {
        Ok(Some(commit)) => {
            log::debug!(
                "list_directory_for_commit got commit [{}] '{}'",
                commit.id,
                commit.message
            );
            match api::local::entries::list_directory(
                repo, &commit, directory, &page_num, &page_size,
            ) {
                Ok((entries, total_entries)) => {
                    log::debug!(
                        "list_directory_for_commit commit {} got {} entries",
                        commit_id,
                        entries.len()
                    );

                    let total_pages = total_entries as f64 / page_size as f64;
                    let view = PaginatedDirEntries {
                        status: String::from(STATUS_SUCCESS),
                        status_message: String::from(MSG_RESOURCE_FOUND),
                        page_size,
                        page_number: page_num,
                        total_pages: total_pages as usize,
                        total_entries,
                        entries,
                    };
                    Ok((view, commit))
                }
                Err(err) => {
                    log::error!("Unable to list repositories. Err: {}", err);
                    Err(StatusMessage::internal_server_error())
                }
            }
        }
        Ok(None) => {
            log::debug!(
                "list_directory_for_commit Could not find commit with id {}",
                commit_id
            );

            Err(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!(
                "list_directory_for_commit Unable to get commit id {}. Err: {}",
                commit_id,
                err
            );
            Err(StatusMessage::internal_server_error())
        }
    }
}
