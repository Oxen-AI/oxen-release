use crate::app_data::OxenAppData;
use crate::params::df_opts_query::{self, DFOptsQuery};

use liboxen::{api, constants};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::df::{tabular, DFOpts};
use liboxen::model::Schema;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::json_data_frame::JsonDataSize;
use liboxen::view::{JsonDataFrame, JsonDataFrameSliceResponse, StatusMessage};
use std::path::PathBuf;

use liboxen::util;

pub async fn get(req: HttpRequest, query: web::Query<DFOptsQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!("file::get repo name [{}] resource [{:?}]", name, resource,);
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            if let Ok(Some((commit_id, _, filepath))) =
                util::resource::parse_resource(&repo, &resource)
            {
                log::debug!(
                    "dir::get commit_id [{}] and filepath {:?}",
                    commit_id,
                    filepath
                );

                match util::fs::version_path_for_commit_id(&repo, &commit_id, &filepath) {
                    Ok(version_path) => match tabular::scan_df(&version_path) {
                        Ok(lazy_df) => {
                            log::debug!("Read version file {:?}", version_path);
                            let polars_schema = lazy_df.schema().unwrap();
                            let schema = Schema::from_polars(&polars_schema);
                            let mut filter = DFOpts::from_schema_columns_exclude_hidden(&schema);
                            log::debug!("Initial filter {:?}", filter);
                            filter = df_opts_query::parse_opts(&query, &mut filter);

                            log::debug!("Got filter {:?}", filter);
                            let lazy_cp = lazy_df.clone();
                            let mut df = tabular::transform_lazy(lazy_cp, filter).unwrap();
                            let full_df = lazy_df.collect().unwrap();
                            let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
                            let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

                            let total_pages = (full_df.height() / page_size) + 1;

                            let response = JsonDataFrameSliceResponse {
                                status: String::from(STATUS_SUCCESS),
                                status_message: String::from(MSG_RESOURCE_FOUND),
                                df: JsonDataFrame::from_df(&mut df),
                                full_size: JsonDataSize {
                                    width: full_df.width(),
                                    height: full_df.height(),
                                },
                                page_number: page,
                                page_size,
                                total_pages,
                                total_entries: full_df.height(),
                            };
                            HttpResponse::Ok().json(response)
                        }
                        Err(err) => {
                            log::error!("unable to read data frame {:?}. Err: {}", resource, err);
                            HttpResponse::InternalServerError()
                                .json(StatusMessage::internal_server_error())
                        }
                    },
                    Err(err) => {
                        log::error!("df::get err: {:?}", err);
                        HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                    }
                }
            } else {
                log::debug!(
                    "schema::get could not find resource from uri {:?}",
                    resource
                );
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
        }
        Ok(None) => {
            log::debug!("schema::get could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("schema::get Err: {}", err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}
