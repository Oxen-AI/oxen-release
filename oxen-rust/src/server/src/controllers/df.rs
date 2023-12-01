use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::df_opts_query::{self, DFOptsQuery};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::{DataFrameSize, Schema};
use liboxen::opts::df_opts::DFOptsView;
use liboxen::view::entry::ResourceVersion;
use liboxen::view::json_data_frame_view::JsonDataFrameSource;
use liboxen::{constants, current_function};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::df::tabular;
use liboxen::opts::DFOpts;
use liboxen::view::{
    JsonDataFrame, JsonDataFrameView, JsonDataFrameViewResponse, JsonDataFrameViews, Pagination,
    StatusMessage,
};

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
    let og_schema = if let Some(schema) =
        api::local::schemas::get_by_path_from_ref(&repo, &resource.commit.id, &resource.file_path)?
    {
        schema
    } else {
        Schema::from_polars(&df.schema())
    };

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);
    // Clear these for the first transform
    opts.page = None;
    opts.page_size = None;

    let opts_view = DFOptsView::from_df_opts(&opts);

    log::debug!("Full df {:?}", df);

    let full_height = df.height();
    let full_width = df.width();

    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

    let start = if page == 0 { 0 } else { page_size * (page - 1) };
    let end = page_size * page;

    // We have to run the query param transforms, then paginate separately
    match tabular::transform(df, opts) {
        Ok(view_df) => {
            log::debug!("View df {:?}", view_df);

            let view_width = view_df.width();
            let view_height = view_df.height();

            // Paginate after transform
            let mut paginate_opts = DFOpts::empty();
            paginate_opts.slice = Some(format!("{}..{}", start, end));
            let mut paginated_df = tabular::transform(view_df, paginate_opts)?;

            let total_pages = (view_height as f64 / page_size as f64).ceil() as usize;
            let full_size = DataFrameSize {
                width: full_width,
                height: full_height,
            };

            // Merge the metadata from the original schema
            let mut view_schema = Schema::from_polars(&paginated_df.schema());
            log::debug!("OG schema {:?}", og_schema);
            log::debug!("Pre-Slice schema {:?}", view_schema);
            view_schema.update_metadata_from_schema(&og_schema);

            log::debug!("View schema {:?}", view_schema);

            let resource_version = ResourceVersion {
                path: resource.file_path.to_string_lossy().into(),
                version: resource.version().to_owned(),
            };

            let df = JsonDataFrame::from_slice(
                &mut paginated_df,
                og_schema.clone(),
                full_size.clone(),
                view_schema.clone(),
            );

            let source_df = JsonDataFrameSource {
                schema: og_schema,
                size: full_size,
            };

            let view_df = JsonDataFrameView {
                schema: view_schema,
                size: DataFrameSize {
                    width: view_width,
                    height: view_height,
                },
                data: df.data,
                pagination: Pagination {
                    page_number: page,
                    page_size,
                    total_pages,
                    total_entries: view_height,
                },
                opts: opts_view,
            };

            let response = JsonDataFrameViewResponse {
                status: StatusMessage::resource_found(),
                data_frame: JsonDataFrameViews {
                    source: source_df,
                    view: view_df,
                },
                commit: Some(resource.commit.clone()),
                resource: Some(resource_version),
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
