use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::df_opts_query::{self, DFOptsQuery};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::api;
use liboxen::core::cache::cachers;
use liboxen::error::OxenError;
use liboxen::model::Schema;
use liboxen::view::entry::ResourceVersion;
use liboxen::view::json_data_frame_view::JsonDataFrameSource;
use liboxen::{constants, current_function};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::df::tabular;
use liboxen::opts::{DFOpts, PaginateOpts};
use liboxen::view::{
    JsonDataFrameView, JsonDataFrameViewResponse, JsonDataFrameViews, StatusMessage,
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
    log::debug!("controllers::data_frames Reading version file {:?}", version_path);

    let data_frame_size =
        cachers::df_size::get_cache_for_version(&repo, &resource.commit, &version_path)?;
    log::debug!("controllers::data_frames got data frame size {:?}", data_frame_size);

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    // Clear these for the first transform
    opts.page = None;
    opts.page_size = None;

    let mut page_opts = PaginateOpts {
        page_num: constants::DEFAULT_PAGE_NUM,
        page_size: constants::DEFAULT_PAGE_SIZE,
    };

    if let Some((start, end)) = opts.slice_indices() {
        // let slice_size = end - start;
        // page_opts.page_num = ((start / slice_size) + 1) as usize;
        // page_opts.page_size = slice_size as usize;
        log::debug!("controllers::data_frames Got slice params {}..{}", start, end);
    } else {
        let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
        let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

        page_opts.page_num = page;
        page_opts.page_size = page_size;
        // let start = if page == 0 { 0 } else { page_size * (page - 1) };
        // let end = page_size * page;
        // opts.slice = Some(format!("{}..{}", start, end));
    }

    // if let Some(page) = query.page {
    //     opts.page = Some(page);
    // }


    let df = tabular::scan_df(&version_path, &opts)?;

    // If there are no transforms, get a slice of the dataframe of size `page_size`
    // for building the response more efficiently on large files. If there are more
    // transforms ie. sorting, then we process on the whole df.
    // let mut process_on_slice = false;

    // let mut df = if !opts.has_transform() {
    //     log::debug!("controllers::data_frames No transforms, processing on slice");
    //     process_on_slice = true;
    //     page_opts.page_num = 1;
    //     page_opts.page_size = page_size;
    //     opts.slice = None;

    //     let page: i64 = page.try_into().unwrap();
    //     let page_size: i64 = page_size.try_into().unwrap();
    //     let page_size_u32: u32 = page_size.try_into().unwrap();

    //     let slice_offset = (page - 1) * page_size;
    //     let slice_len = page_size_u32;
    //     // let mut df = tabular::scan_df(&version_path, &DFOpts::empty())?;
    //     let df = tabular::scan_df_parquet_n_rows(&version_path, 300)?;
    //     log::debug!("controllers::data_frames Done scanning df {:?}", version_path);
    //     df
    // } else {

    // }

    // Try to get the schema from disk
    let og_schema = if let Some(schema) =
        api::local::schemas::get_by_path_from_ref(&repo, &resource.commit.id, &resource.file_path)?
    {
        schema
    } else {
        match df.schema() {
            Ok(schema) => Ok(Schema::from_polars(&schema.to_owned())),
            Err(e) => {
                log::error!("Error reading df: {}", e);
                Err(OxenHttpError::InternalServerError)
            }
        }?
    };

    log::debug!("controllers::data_frames Done getting schema {:?}", version_path);
    
    // We have to run the query param transforms, then paginate separately
    let og_df_json = JsonDataFrameSource::from_df(&data_frame_size, &og_schema);

    log::debug!("controllers::data_frames BEFORE TRANSFORM LAZY {}", data_frame_size.height);
    match tabular::transform_lazy(df, data_frame_size.height, opts) {
        Ok(df_view) => {
            log::debug!("controllers::data_frames DF view {:?}", df_view);

            // If there were transforms such as a filter, the pagination total entries
            // must be from the filtered df, not the original df.
            // if !process_on_slice {
            //     data_frame_size.height = df_view.height();
            // }

            let resource_version = ResourceVersion {
                path: resource.file_path.to_string_lossy().into(),
                version: resource.version().to_owned(),
            };

            let response = JsonDataFrameViewResponse {
                status: StatusMessage::resource_found(),
                data_frame: JsonDataFrameViews {
                    source: og_df_json,
                    view: JsonDataFrameView::view_from_pagination(
                        df_view,
                        og_schema,
                        data_frame_size,
                        &page_opts,
                    ),
                },
                commit: Some(resource.commit.clone()),
                resource: Some(resource_version),
                derived_resource: None,
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
