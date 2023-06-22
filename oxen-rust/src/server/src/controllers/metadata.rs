use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param, AggregateQuery};

use liboxen::core;
use liboxen::error::OxenError;
use liboxen::view::json_data_frame::JsonDataSize;
use liboxen::view::{
    JsonDataFrame, JsonDataFrameSliceResponse, MetadataEntryResponse, StatusMessage,
};
use liboxen::{api, current_function};

use actix_web::{web, HttpRequest, HttpResponse};

pub async fn file(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let latest_commit = api::local::commits::get_by_id(&repo, &resource.commit.id)?.ok_or(
        OxenError::committish_not_found(resource.commit.id.clone().into()),
    )?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let entry = api::local::entries::get_meta_entry(&repo, &resource.commit, &resource.file_path)?;
    let meta = MetadataEntryResponse {
        status: StatusMessage::resource_found(),
        entry,
    };
    Ok(HttpResponse::Ok().json(meta))
}

pub async fn dir(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let latest_commit = api::local::commits::get_by_id(&repo, &resource.commit.id)?.ok_or(
        OxenError::committish_not_found(resource.commit.id.clone().into()),
    )?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let directory = resource.file_path;
    let offset = 0;
    let limit = 100;
    let mut sliced_df =
        core::index::commit_metadata_db::select(&repo, &latest_commit, &directory, offset, limit)?;
    let (num_rows, num_cols) =
        core::index::commit_metadata_db::full_size(&repo, &latest_commit, &directory)?;
    let response = JsonDataFrameSliceResponse {
        status: StatusMessage::resource_found(),
        full_size: JsonDataSize {
            width: num_cols,
            height: num_rows,
        },
        df: JsonDataFrame::from_df(&mut sliced_df),
        page_number: 0,
        page_size: limit,
        total_pages: 0,
        total_entries: limit,
    };
    Ok(HttpResponse::Ok().json(response))
}

pub async fn agg_dir(
    req: HttpRequest,
    query: web::Query<AggregateQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    let column = query.column.clone().ok_or(OxenHttpError::BadRequest(
        "Must supply column query parameter".into(),
    ))?;

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let latest_commit = api::local::commits::get_by_id(&repo, &resource.commit.id)?.ok_or(
        OxenError::committish_not_found(resource.commit.id.clone().into()),
    )?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let directory = resource.file_path;
    let mut sliced_df =
        core::index::commit_metadata_db::aggregate_col(&repo, &latest_commit, &directory, column)?;

    let response = JsonDataFrameSliceResponse {
        status: StatusMessage::resource_found(),
        full_size: JsonDataSize {
            width: sliced_df.width(),
            height: sliced_df.height(),
        },
        df: JsonDataFrame::from_df(&mut sliced_df),
        page_number: 1,
        page_size: sliced_df.height(),
        total_pages: 1,
        total_entries: sliced_df.height(),
    };
    Ok(HttpResponse::Ok().json(response))
}

pub async fn images(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let latest_commit = api::local::commits::get_by_id(&repo, &resource.commit.id)?.ok_or(
        OxenError::committish_not_found(resource.commit.id.clone().into()),
    )?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    // TODO: get stats dataframe given the directory...figure out what the best API and response is for this...
    let entry = api::local::entries::get_meta_entry(&repo, &resource.commit, &resource.file_path)?;
    let meta = MetadataEntryResponse {
        status: StatusMessage::resource_found(),
        entry,
    };
    Ok(HttpResponse::Ok().json(meta))
}
