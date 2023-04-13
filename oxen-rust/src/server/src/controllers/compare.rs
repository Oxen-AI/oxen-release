use crate::app_data::OxenAppData;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::index::differ;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{CompareResponse, StatusMessage};
use liboxen::{api, constants, util};

use super::entries::PageNumQuery;
use crate::params::{parse_base_head, resolve_base_head};

pub async fn show(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let base_head: &str = req.match_info().get("base_head").unwrap();

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repository)) => match parse_base_head(base_head) {
            Ok((base, head)) => match resolve_base_head(&repository, &base, &head) {
                Ok((Some(base_commit), Some(head_commit))) => {
                    match differ::list_diff_entries(&repository, &base_commit, &head_commit) {
                        Ok(entries) => {
                            let total_entries = entries.len();
                            let (paginated, total_pages) = util::paginate(entries, page, page_size);
                            let view = CompareResponse {
                                status: String::from(STATUS_SUCCESS),
                                status_message: String::from(MSG_RESOURCE_FOUND),
                                base_commit,
                                head_commit,
                                page_size,
                                total_entries,
                                page_number: page,
                                total_pages,
                                entries: paginated,
                            };
                            HttpResponse::Ok().json(view)
                        }
                        Err(err) => {
                            log::error!("Unable to list diff entries. Err: {}", err);
                            HttpResponse::InternalServerError()
                                .json(StatusMessage::internal_server_error())
                        }
                    }
                }
                Ok((_, _)) => {
                    log::error!(
                        "Unable to resolve commits. Base or head not found {}",
                        base_head
                    );
                    HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                }
                Err(err) => {
                    log::error!("Unable to resolve commits. Err: {}", err);
                    HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
                }
            },
            Err(err) => {
                log::error!("Unable to list branches. Err: {}", err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        },
        Ok(None) => {
            log::debug!(
                "404 api::local::branches::index could not get repo {}",
                name,
            );
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!(
                "Err api::local::branches::index could not get repo {} {:?}",
                name,
                err
            );
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}
