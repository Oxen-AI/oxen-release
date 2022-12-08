use crate::app_data::OxenAppData;

use liboxen::api;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::df::{tabular, DFOpts};
use liboxen::model::Schema;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::json_data_frame::JsonDataSize;
use liboxen::view::{JsonDataFrame, JsonDataFrameSliceResponse, StatusMessage};
use serde::Deserialize;
use std::path::PathBuf;

use liboxen::util;

#[derive(Deserialize, Debug)]
pub struct DFOptsQuery {
    pub slice: Option<String>,
    pub take: Option<String>,
    pub columns: Option<String>,
    pub filter: Option<String>,
    pub aggregate: Option<String>,
    pub sort_by: Option<String>,
    pub randomize: Option<bool>,
    pub reverse: Option<bool>,
}

pub async fn get(req: HttpRequest, query: web::Query<DFOptsQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!("file::get repo name [{}] resource [{:?}]", name, resource,);
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            if let Ok(Some((commit_id, filepath))) =
                util::resource::parse_resource(&repo, &resource)
            {
                log::debug!(
                    "dir::get commit_id [{}] and filepath {:?}",
                    commit_id,
                    filepath
                );

                match util::fs::version_path_for_commit_id(&repo, &commit_id, &filepath) {
                    Ok(version_path) => match tabular::scan_df(version_path) {
                        Ok(lazy_df) => {
                            let polars_schema = lazy_df.schema().unwrap();
                            let schema = Schema::from_polars(&polars_schema);
                            let mut filter = DFOpts::from_filter_schema_exclude_hidden(&schema);
                            log::debug!("Initial filter {:?}", filter);
                            filter = parse_opts(query, &mut filter);

                            log::debug!("Got filter {:?}", filter);
                            let lazy_cp = lazy_df.clone();
                            let mut df = tabular::transform_df(lazy_cp, filter).unwrap();
                            let full_df = lazy_df.collect().unwrap();
                            let response = JsonDataFrameSliceResponse {
                                status: String::from(STATUS_SUCCESS),
                                status_message: String::from(MSG_RESOURCE_FOUND),
                                df: JsonDataFrame::from_df(&mut df),
                                full_size: JsonDataSize {
                                    width: full_df.width(),
                                    height: full_df.height(),
                                },
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

/// Provide some default vals for opts
fn parse_opts(query: web::Query<DFOptsQuery>, filter_ops: &mut DFOpts) -> DFOpts {
    // Default to 0..10 unless they ask for "all"
    if let Some(slice) = query.slice.clone() {
        if slice == "all" {
            // Return everything...probably don't want to do this unless explicitly asked for
            filter_ops.slice = None;
        } else {
            // Return what they asked for
            filter_ops.slice = Some(slice);
        }
    } else {
        // No slice val supplied, only return first 10
        filter_ops.slice = Some(String::from("0..10"));
    }

    // we are already filtering the hidden columns
    if let Some(columns) = query.columns.clone() {
        filter_ops.columns = Some(columns);
    }

    filter_ops.take = query.take.clone();
    filter_ops.filter = query.filter.clone();
    filter_ops.aggregate = query.aggregate.clone();
    filter_ops.sort_by = query.sort_by.clone();
    filter_ops.should_randomize = query.randomize.unwrap_or(false);
    filter_ops.should_reverse = query.reverse.unwrap_or(false);

    filter_ops.clone()
}
