use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::df_opts_query::{self, DFOptsQuery};
use crate::params::{app_data, parse_resource, path_param};

use async_std::path::PathBuf;
use liboxen::api;
use liboxen::constants;
use liboxen::constants::DUCKDB_DF_TABLE_NAME;
use liboxen::core::cache::cachers;
use liboxen::core::db::df_db;
use liboxen::core::index::CommitEntryReader;
use liboxen::error::{OxenError, PathBufError};
use liboxen::model::{Commit, DataFrameSize, ParsedResource, Schema};
use liboxen::opts::df_opts::DFOptsView;
use liboxen::view::entry::ResourceVersion;
use liboxen::view::json_data_frame_view::JsonDataFrameSource;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::df::{sql, tabular};
use liboxen::opts::{DFOpts, PaginateOpts};
use liboxen::view::{
    JsonDataFrameView, JsonDataFrameViewResponse, JsonDataFrameViews, Pagination, StatusMessage,
};

use liboxen::util;
use polars::frame::DataFrame;

use liboxen::core::index;
use uuid::Uuid;

// TODO: This is getting long...condense and factor out whatever possible here
pub async fn get(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;
    let entry_reader = CommitEntryReader::new(&repo, &commit)?;
    let entry = entry_reader
        .get_entry(&resource.path)?
        .ok_or(OxenHttpError::NotFound)?;

    // Get the path to the versioned file on disk
    let version_path = util::fs::version_path_for_commit_id(&repo, &commit.id, &resource.path)?;
    log::debug!(
        "controllers::data_frames Reading version file {:?}",
        version_path
    );

    // Get the cached size of the data frame
    let data_frame_size = cachers::df_size::get_cache_for_version(&repo, &commit, &version_path)?;

    log::debug!(
        "controllers::data_frames got data frame size {:?}",
        data_frame_size
    );

    // Parse the query params
    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);
    log::debug!("controllers::data_frames got opts {:?}", opts);

    // Paginate or slice, after we do the original transform
    let mut page_opts = PaginateOpts {
        page_num: constants::DEFAULT_PAGE_NUM,
        page_size: constants::DEFAULT_PAGE_SIZE,
    };

    // Block big big dfs
    if opts.sql.is_some() || opts.text2sql.is_some() {
        if data_frame_size.height > constants::MAX_QUERYABLE_ROWS {
            return Err(OxenHttpError::NotQueryable);
        }

        if !sql::df_is_indexed(&repo, &entry)? {
            return Err(OxenHttpError::DatasetNotIndexed(resource.path.into()));
        }
    }

    let resource_version = ResourceVersion {
        path: resource.path.to_string_lossy().into(),
        version: resource.version.to_string_lossy().into(),
    };

    // If we have slice params, use them
    if let Some((start, end)) = opts.slice_indices() {
        log::debug!(
            "controllers::data_frames Got slice params {}..{}",
            start,
            end
        );
    } else {
        // Otherwise use the query params for pagination
        let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
        let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

        page_opts.page_num = page;
        page_opts.page_size = page_size;

        // Must translate page params to slice params
        let start = if page == 0 { 0 } else { page_size * (page - 1) };
        let end = page_size * page;
        opts.slice = Some(format!("{}..{}", start, end));
        log::debug!("imputed slice params {:?}", opts.slice);
    }

    if let Some(sql) = opts.sql.clone() {
        let mut conn = sql::get_conn(&repo, &entry)?;
        let db_schema = df_db::get_schema(&conn, DUCKDB_DF_TABLE_NAME)?;
        let df = sql::query_df(&repo, &entry, sql, &mut conn)?;

        let json_df = format_sql_df_response(
            df,
            &commit,
            &opts,
            &page_opts,
            &resource,
            &db_schema,
            &data_frame_size,
        )?;
        return Ok(HttpResponse::Ok().json(json_df));
    }

    let mut df = tabular::scan_df(&version_path, &opts, data_frame_size.height)?;

    if let Some(text2sql) = opts.text2sql.clone() {
        let mut conn = sql::get_conn(&repo, &entry)?;
        let db_schema = df_db::get_schema(&conn, DUCKDB_DF_TABLE_NAME)?;

        let df = sql::text2sql_df(
            &repo,
            &entry,
            &db_schema,
            text2sql,
            &mut conn,
            opts.get_host(),
        )?;
        let json_df = format_sql_df_response(
            df,
            &commit,
            &opts,
            &page_opts,
            &resource,
            &db_schema,
            &data_frame_size,
        )?;
        return Ok(HttpResponse::Ok().json(json_df));
    }

    // Try to get the schema from disk
    let og_schema = if let Some(schema) =
        api::local::schemas::get_by_path_from_ref(&repo, &commit.id, &resource.path)?
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

    log::debug!(
        "controllers::data_frames Done getting schema {:?}",
        version_path
    );

    // We have to run the query param transforms, then paginate separately
    let og_df_json = JsonDataFrameSource::from_df_size(&data_frame_size, &og_schema);

    log::debug!(
        "controllers::data_frames BEFORE TRANSFORM LAZY {}",
        data_frame_size.height
    );

    match tabular::transform_lazy(df, data_frame_size.height, opts.clone()) {
        Ok(df_view) => {
            log::debug!("controllers::data_frames DF view {:?}", df_view);

            // Have to do the pagination after the transform
            let view_height = if opts.has_filter_transform() {
                df_view.height()
            } else {
                data_frame_size.height
            };

            let total_pages = (view_height as f64 / page_opts.page_size as f64).ceil() as usize;

            let mut df = tabular::transform_slice(df_view, data_frame_size.height, opts.clone())?;
            log::debug!("here's our post-slice df {:?}", df);

            let mut slice_schema = Schema::from_polars(&df.schema());
            log::debug!("OG schema {:?}", og_schema);
            log::debug!("Pre-Slice schema {:?}", slice_schema);
            slice_schema.update_metadata_from_schema(&og_schema);
            log::debug!("Slice schema {:?}", slice_schema);
            let opts_view = DFOptsView::from_df_opts(&opts);

            let response = JsonDataFrameViewResponse {
                status: StatusMessage::resource_found(),
                data_frame: JsonDataFrameViews {
                    source: og_df_json,
                    view: JsonDataFrameView {
                        schema: slice_schema,
                        size: DataFrameSize {
                            height: df.height(),
                            width: df.width(),
                        },
                        data: JsonDataFrameView::json_from_df(&mut df),
                        pagination: Pagination {
                            page_number: page_opts.page_num,
                            page_size: page_opts.page_size,
                            total_pages,
                            total_entries: view_height,
                        },
                        opts: opts_view,
                    },
                },
                commit: Some(commit.clone()),
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

pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;

    let path = resource.clone().path;

    match index::workspaces::data_frames::get_queryable_data_frame_workspace(&repo, resource) {
        Ok(_) => {
            return Err(OxenHttpError::DatasetAlreadyIndexed(PathBufError::from(
                path,
            )));
        }
        Err(e) => match e {
            OxenError::QueryableWorkspaceNotFound() => {
                let workspace_id = Uuid::new_v4().to_string();
                let workspace = index::workspaces::create(&repo, &commit, &workspace_id, false)?;
                index::workspaces::data_frames::index(&workspace, &path)?;
            }
            _ => return Err(OxenHttpError::from(e)),
        },
    }

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}

fn format_sql_df_response(
    df: DataFrame,
    commit: &Commit,
    opts: &DFOpts,
    page_opts: &PaginateOpts,
    resource: &ParsedResource,
    og_schema: &Schema,
    data_frame_size: &DataFrameSize,
) -> Result<JsonDataFrameViewResponse, OxenHttpError> {
    let resource_version = ResourceVersion {
        path: resource.path.to_string_lossy().into(),
        version: resource.version.to_string_lossy().into(),
    };

    // For sql, paginate before the view to avoid double-slicing and get correct view size numbers.
    let og_df_height = df.height();
    let paginated_df = tabular::paginate_df(df, page_opts)?;

    let view = JsonDataFrameView::from_df_opts_unpaginated(
        paginated_df,
        og_schema.clone(),
        og_df_height,
        opts,
    );

    let response = JsonDataFrameViewResponse {
        status: StatusMessage::resource_found(),
        data_frame: JsonDataFrameViews {
            source: JsonDataFrameSource::from_df_size(data_frame_size, og_schema),
            view,
        },
        commit: Some(commit.clone()),
        resource: Some(resource_version),
        derived_resource: None,
    };
    Ok(response)
}
