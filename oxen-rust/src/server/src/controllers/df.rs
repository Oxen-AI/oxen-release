use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::df_opts_query::{self, DFOptsQuery};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::{constants, current_function};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::df::tabular;
use liboxen::opts::DFOpts;
use liboxen::view::json_data_frame::JsonDataSize;
use liboxen::view::{JsonDataFrame, JsonDataFrameSliceResponse, StatusMessage};

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

    let version_path =
        util::fs::version_path_for_commit_id(&repo, &resource.commit.id, &resource.file_path)?;
    log::debug!("Reading version file {:?}", version_path);

    // Have to read full df to get the full size
    let df = tabular::read_df(&version_path, DFOpts::empty())?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    log::debug!("Full df {:?}", df);

    let full_height = df.height();
    let full_width = df.width();

    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

    let total_pages = (full_height as f64 / page_size as f64).ceil() as usize;

    let start = if page <= 0 { 0 } else { page_size * (page - 1) };
    let end = page_size * page;

    opts.slice = Some(format!("{}..{}", start, end));
    let mut sliced_df = tabular::transform(df, opts)?;
    log::debug!("Sliced df {:?}", sliced_df);

    let response = JsonDataFrameSliceResponse {
        status: StatusMessage::resource_found(),
        full_size: JsonDataSize {
            width: full_width,
            height: full_height,
        },
        df: JsonDataFrame::from_df(&mut sliced_df),
        page_number: page,
        page_size,
        total_pages,
        total_entries: full_height,
    };
    Ok(HttpResponse::Ok().json(response))
}
