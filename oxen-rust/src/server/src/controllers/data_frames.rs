use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::df_opts_query::{self, DFOptsQuery};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::api;
use liboxen::constants;
use liboxen::constants::DUCKDB_DF_TABLE_NAME;
use liboxen::core::cache::cachers;
use liboxen::core::db::df_db;
use liboxen::core::index::CommitEntryReader;
use liboxen::error::{OxenError, PathBufError};
use liboxen::model::{
    Commit, CommitEntry, DataFrameSize, LocalRepository, ParsedResource, Schema, Workspace,
};
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

    let version_path = util::fs::version_path_for_commit_id(&repo, &commit.id, &resource.path)?;
    log::debug!(
        "controllers::data_frames Reading version file {:?}",
        version_path
    );

    let data_frame_size = cachers::df_size::get_cache_for_version(&repo, &commit, &version_path)?;
    log::debug!(
        "controllers::data_frames got data frame size {:?}",
        data_frame_size
    );

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    let mut page_opts = PaginateOpts {
        page_num: constants::DEFAULT_PAGE_NUM,
        page_size: constants::DEFAULT_PAGE_SIZE,
    };

    if let Some((start, end)) = opts.slice_indices() {
        log::debug!(
            "controllers::data_frames Got slice params {}..{}",
            start,
            end
        );
    } else {
        let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
        let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

        page_opts.page_num = page;
        page_opts.page_size = page_size;

        let start = if page == 0 { 0 } else { page_size * (page - 1) };
        let end = page_size * page;
        opts.slice = Some(format!("{}..{}", start, end));
    }

    let handle_sql_result = handle_sql_querying(
        &repo,
        &commit,
        &resource,
        &opts,
        &page_opts,
        &data_frame_size,
        &entry,
    );
    if let Ok(response) = handle_sql_result {
        return Ok(response);
    }

    let resource_version = ResourceVersion {
        path: resource.path.to_string_lossy().into(),
        version: resource.version.to_string_lossy().into(),
    };

    let mut df = tabular::scan_df(&version_path, &opts, data_frame_size.height)?;

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

    // Transformation and pagination logic...
    match tabular::transform_lazy(df, opts.clone()) {
        Ok(df_view) => {
            // Have to do the pagination after the transform
            let mut df = tabular::transform_slice_lazy(df_view, opts.clone())?.collect()?;

            let view_height = if opts.has_filter_transform() {
                df.height()
            } else {
                data_frame_size.height
            };

            let total_pages = (view_height as f64 / page_opts.page_size as f64).ceil() as usize;
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

fn handle_sql_querying(
    repo: &LocalRepository,
    commit: &Commit,
    resource: &ParsedResource,
    opts: &DFOpts,
    page_opts: &PaginateOpts,
    data_frame_size: &DataFrameSize,
    entry: &CommitEntry,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let mut workspace: Option<Workspace> = None;

    if opts.sql.is_some() {
        if data_frame_size.height > constants::MAX_QUERYABLE_ROWS {
            return Err(OxenHttpError::NotQueryable);
        }

        match index::workspaces::data_frames::get_queryable_data_frame_workspace(
            repo,
            &resource.path,
            commit,
        ) {
            Ok(found_workspace) => {
                // Assign the found workspace to the workspace variable
                workspace = Some(found_workspace);
            }
            Err(e) => match e {
                OxenError::QueryableWorkspaceNotFound() => {
                    let resource_path = resource.clone().path;
                    return Err(OxenHttpError::DatasetNotIndexed(resource_path.into()));
                }
                _ => return Err(OxenHttpError::from(e)),
            },
        }
    }

    if let (Some(sql), Some(workspace)) = (opts.sql.clone(), workspace) {
        let db_path = index::workspaces::data_frames::duckdb_path(&workspace, &entry.path);
        let mut conn = df_db::get_connection(db_path)?;

        let mut db_schema = df_db::get_schema(&conn, DUCKDB_DF_TABLE_NAME)?;
        let df = sql::query_df(sql, &mut conn)?;

        let og_schema = if let Some(schema) =
            api::local::schemas::get_by_path_from_ref(repo, &workspace.commit.id, &resource.path)?
        {
            schema
        } else {
            Schema::from_polars(&df.schema())
        };

        db_schema.update_metadata_from_schema(&og_schema);

        let json_df = format_sql_df_response(
            df,
            commit,
            opts,
            page_opts,
            resource,
            &db_schema,
            data_frame_size,
        )?;
        return Ok(HttpResponse::Ok().json(json_df));
    }

    Err(OxenHttpError::InternalServerError)
}

pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;

    let path = resource.clone().path;

    // Check if the data frame is already indexed.
    if index::workspaces::data_frames::is_queryable_data_frame_indexed(
        &repo,
        &resource.path,
        &commit,
    )? {
        // If the data frame is already indexed, return the appropriate error.
        return Err(OxenHttpError::DatasetAlreadyIndexed(PathBufError::from(
            path,
        )));
    } else {
        // If not, proceed to create a new workspace and index the data frame.
        let workspace_id = Uuid::new_v4().to_string();
        let workspace = index::workspaces::create(&repo, &commit, workspace_id, false)?;
        index::workspaces::data_frames::index(&workspace, &path)?;
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
