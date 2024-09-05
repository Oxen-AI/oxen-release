use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, df_opts_query, path_param, DFOptsQuery, PageNumQuery};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::constants::TABLE_NAME;
use liboxen::core::db::data_frames::{df_db, workspace_df_db};
use liboxen::core::v0_10_0::index::workspaces;
use liboxen::error::OxenError;
use liboxen::model::Schema;
use liboxen::opts::DFOpts;
use liboxen::repositories;
use liboxen::util::paginate;
use liboxen::view::data_frames::DataFramePayload;
use liboxen::view::entries::ResourceVersion;
use liboxen::view::entries::{PaginatedMetadataEntries, PaginatedMetadataEntriesResponse};
use liboxen::view::json_data_frame_view::WorkspaceJsonDataFrameViewResponse;
use liboxen::view::{JsonDataFrameViewResponse, JsonDataFrameViews, StatusMessage};
use liboxen::{constants, core::v0_10_0::index};

pub mod columns;
pub mod rows;

pub async fn get_by_resource(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let workspace = index::workspaces::get(&repo, workspace_id)?;
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    let is_indexed = index::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_indexed {
        let response = WorkspaceJsonDataFrameViewResponse {
            status: StatusMessage::resource_found(),
            data_frame: None,
            resource: None,
            commit: None, // Not at a committed state
            derived_resource: None,
            is_indexed,
        };

        return Ok(HttpResponse::Ok().json(response));
    }

    let staged_db_path = workspaces::data_frames::duckdb_path(&workspace, &file_path);

    let conn = df_db::get_connection(staged_db_path)?;

    let count = index::workspaces::data_frames::count(&workspace, &file_path)?;

    let df = index::workspaces::data_frames::query(&workspace, &file_path, &opts)?;

    let mut df_schema = df_db::get_schema(&conn, TABLE_NAME)?;

    let is_indexed = index::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    let resource = ResourceVersion {
        path: file_path.to_string_lossy().to_string(),
        version: workspace.commit.id.to_string(),
    };

    let og_schema = if let Some(schema) =
        repositories::data_frames::schemas::get_by_path(&repo, &workspace.commit, &resource.path)?
    {
        schema
    } else {
        Schema::from_polars(&df.schema())
    };

    df_schema.update_metadata_from_schema(&og_schema);

    let mut df_views =
        JsonDataFrameViews::from_df_and_opts_unpaginated(df, df_schema, count, &opts);

    index::workspaces::data_frames::columns::decorate_fields_with_column_diffs(
        &workspace,
        &file_path,
        &mut df_views,
    )?;

    let response = WorkspaceJsonDataFrameViewResponse {
        status: StatusMessage::resource_found(),
        data_frame: Some(df_views),
        resource: Some(resource),
        commit: None, // Not at a committed state
        derived_resource: None,
        is_indexed,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn get_by_branch(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req).unwrap();

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let branch_name: &str = req.match_info().query("branch");
    let workspace = index::workspaces::get(&repo, workspace_id)?;

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Staged dataframes must be on a branch.
    let branch = repositories::branches::get_by_name(&repo, branch_name)?
        .ok_or(OxenError::remote_branch_not_found(branch_name))?;

    let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?
        .ok_or(OxenError::resource_not_found(&branch.commit_id))?;

    let entries = repositories::entries::list_tabular_files_in_repo(&repo, &commit)?;

    let mut editable_entries = vec![];
    for entry in entries {
        if let Some(resource) = entry.resource.clone() {
            if index::workspaces::data_frames::is_indexed(&workspace, &resource.path)? {
                editable_entries.push(entry);
            }
        }
    }

    let (paginated_entries, pagination) = paginate(editable_entries, page, page_size);
    Ok(HttpResponse::Ok().json(PaginatedMetadataEntriesResponse {
        status: StatusMessage::resource_found(),
        entries: PaginatedMetadataEntries {
            entries: paginated_entries,
            pagination,
        },
    }))
}

pub async fn diff(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let file_path = PathBuf::from(path_param(&req, "path")?);
    let workspace = index::workspaces::get(&repo, workspace_id)?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    // TODO: Let's not expose dbs right in the controller
    let staged_db_path =
        liboxen::core::v0_10_0::index::workspaces::data_frames::duckdb_path(&workspace, &file_path);

    let conn = df_db::get_connection(staged_db_path)?;

    let df = index::workspaces::data_frames::query(&workspace, &file_path, &opts)?;

    let diff_df = workspace_df_db::df_diff(&conn)?;

    let mut df_schema = df_db::get_schema(&conn, TABLE_NAME)?;

    let resource = ResourceVersion {
        path: file_path.to_string_lossy().to_string(),
        version: workspace.commit.id.to_string(),
    };

    let og_schema = if let Some(schema) =
        repositories::data_frames::schemas::get_by_path(&repo, &workspace.commit, resource.path)?
    {
        schema
    } else {
        Schema::from_polars(&df.schema())
    };

    df_schema.update_metadata_from_schema(&og_schema);

    let mut df_views = JsonDataFrameViews::from_df_and_opts(diff_df, df_schema, &opts);

    index::workspaces::data_frames::columns::decorate_fields_with_column_diffs(
        &workspace,
        &file_path,
        &mut df_views,
    )?;

    let resource = ResourceVersion {
        path: file_path.to_string_lossy().to_string(),
        version: workspace.commit.id.to_string(),
    };

    let resource = JsonDataFrameViewResponse {
        data_frame: df_views,
        status: StatusMessage::resource_found(),
        resource: Some(resource),
        commit: None,
        derived_resource: None,
    };

    Ok(HttpResponse::Ok().json(resource))
}

pub async fn put(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let file_path = PathBuf::from(path_param(&req, "path")?);

    log::debug!("workspace {} data frame put {:?}", workspace_id, file_path);
    let workspace = index::workspaces::get(&repo, &workspace_id)?;
    let data: DataFramePayload = serde_json::from_str(&body)?;
    log::debug!("workspace {} data frame put {:?}", workspace_id, data);

    let to_index = data.is_indexed;
    let is_indexed = index::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_indexed && to_index {
        index::workspaces::data_frames::index(&workspace, &file_path)?;
    } else if is_indexed && !to_index {
        index::workspaces::data_frames::unindex(&workspace, &file_path)?;
    }

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let file_path = PathBuf::from(path_param(&req, "path")?);
    let workspace = index::workspaces::get(&repo, workspace_id)?;

    index::workspaces::data_frames::restore(&workspace, file_path)?;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
}
