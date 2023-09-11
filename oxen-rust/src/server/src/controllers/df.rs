use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::df_opts_query::{self, DFOptsQuery};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::{DataFrameSize, Schema};
use liboxen::{constants, current_function};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::df::tabular;
use liboxen::opts::DFOpts;
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

    // Have to read full df to get the full size, may be able to optimize later
    let df = tabular::read_df(&version_path, DFOpts::empty())?;

    // Try to get the schema from disk
    let og_schema =
        if let Some(schema) = api::local::schemas::get_by_path(&repo, resource.file_path)? {
            schema
        } else {
            Schema::from_polars(&df.schema())
        };

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);
    // Clear these for the first transform
    opts.page = None;
    opts.page_size = None;

    log::debug!("Full df {:?}", df);

    let full_height = df.height();
    let full_width = df.width();

    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

    let start = if page == 0 { 0 } else { page_size * (page - 1) };
    let end = page_size * page;

    // We have to run the query param transforms, then paginate separately
    match tabular::transform(df, opts) {
        Ok(sliced_df) => {
            log::debug!("Sliced df {:?}", sliced_df);

            let sliced_width = sliced_df.width();
            let sliced_height = sliced_df.height();

            // Paginate after transform
            let mut paginate_opts = DFOpts::empty();
            paginate_opts.slice = Some(format!("{}..{}", start, end));
            let mut paginated_df = tabular::transform(sliced_df, paginate_opts)?;

            let total_pages = (sliced_height as f64 / page_size as f64).ceil() as usize;
            let full_size = DataFrameSize {
                width: full_width,
                height: full_height,
            };

            // Merge the metadata from the original schema
            let mut slice_schema = Schema::from_polars(&paginated_df.schema());
            log::debug!("OG schema {:?}", og_schema);
            log::debug!("Pre-Slice schema {:?}", slice_schema);
            slice_schema.update_metadata_from_schema(&og_schema);

            log::debug!("Slice schema {:?}", slice_schema);

            let response = JsonDataFrameSliceResponse {
                status: StatusMessage::resource_found(),
                full_size: full_size.to_owned(),
                slice_size: DataFrameSize {
                    width: sliced_width,
                    height: sliced_height,
                },
                df: JsonDataFrame::from_slice(
                    &mut paginated_df,
                    og_schema,
                    full_size,
                    slice_schema,
                ),
                page_number: page,
                page_size,
                total_pages,
                total_entries: sliced_height,
            };
            Ok(HttpResponse::Ok().json(response))
        }
        Err(OxenError::SQLParseError(sql)) => {
            log::error!("Error parsing SQL: {}", sql);
            Err(OxenHttpError::SQLParseError(sql))
        }
        Err(e) => {
            log::error!("Error transforming df: {}", e);
            Err(OxenHttpError::InternalServerError)
        }
    }
}
