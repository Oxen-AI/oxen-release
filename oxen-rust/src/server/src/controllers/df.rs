use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::df_opts_query::{self, DFOptsQuery};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::{constants, current_function};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::df::{tabular, DFOpts};
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::json_data_frame::JsonDataSize;
use liboxen::view::{JsonDataFrame, JsonDataFrameSliceResponse};

use liboxen::util;

pub async fn get(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
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

    let mut opts = DFOpts::empty();
    log::debug!("Initial opts {:?}", opts);
    opts = df_opts_query::parse_opts(&query, &mut opts);

    let version_path =
        util::fs::version_path_for_commit_id(&repo, &resource.commit.id, &resource.file_path)?;
    log::debug!("Reading version file {:?}", version_path);
    let mut df = tabular::read_df(&version_path, opts)?;
    log::debug!("Read df {:?}", df);

    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

    let total_pages = (df.height() as f64 / page_size as f64).ceil() as usize;

    let response = JsonDataFrameSliceResponse {
        status: String::from(STATUS_SUCCESS),
        status_message: String::from(MSG_RESOURCE_FOUND),
        full_size: JsonDataSize {
            width: df.width(),
            height: df.height(),
        },
        df: JsonDataFrame::from_df(&mut df),
        page_number: page,
        page_size,
        total_pages,
        total_entries: df.height(),
    };
    Ok(HttpResponse::Ok().json(response))
}
