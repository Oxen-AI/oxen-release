use std::path::{Path, PathBuf};

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{
    app_data, df_opts_query, parse_resource, path_param, DFOptsQuery, PageNumQuery,
};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::constants::TABLE_NAME;
use liboxen::core::db::{df_db, staged_df_db};
use liboxen::error::OxenError;
use liboxen::model::{Commit, LocalRepository, ParsedResource, Schema};
use liboxen::opts::DFOpts;
use liboxen::util::paginate;
use liboxen::view::data_frames::DataFramePayload;
use liboxen::view::entry::ResourceVersion;
use liboxen::view::entry::{PaginatedMetadataEntries, PaginatedMetadataEntriesResponse};
use liboxen::view::json_data_frame_view::EditableJsonDataFrameViewResponse;
use liboxen::view::{JsonDataFrameViewResponse, JsonDataFrameViews, StatusMessage};
use liboxen::{api, constants, core::index};

pub mod rows;

pub async fn get_by_resource(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource
        .clone()
        .commit
        .ok_or(OxenError::resource_not_found(
            resource.version.to_string_lossy(),
        ))?;

    log::debug!(
        "controllers::workspaces::data_frames::get resource: {:?}",
        resource
    );
    log::debug!(
        "controllers::workspaces::data_frames::get commit: {:?}",
        commit
    );

    let schema = api::local::schemas::get_by_path_from_ref(&repo, &commit.id, &resource.path)?
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    log::debug!("got this schema for the endpoint {:?}", schema);

    log::debug!(
        "{} indexing dataset for resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );

    // Staged dataframes must be on a branch.
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;
    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?
        .ok_or(OxenError::resource_not_found(&branch.commit_id))?;
    let _workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    if !index::workspaces::data_frames::is_indexed(&repo, &commit, &workspace_id, &resource.path)? {
        return Err(OxenHttpError::DatasetNotIndexed(resource.path.into()));
    }

    let count = index::workspaces::data_frames::count(
        &repo,
        &commit,
        resource.path.clone(),
        &workspace_id,
    )?;

    let df = index::workspaces::data_frames::query(
        &repo,
        &commit,
        &workspace_id,
        &resource.path,
        &opts,
    )?;

    let df_schema = Schema::from_polars(&df.schema());

    let is_editable =
        index::workspaces::data_frames::is_indexed(&repo, &commit, &workspace_id, &resource.path)?;

    let df_views = JsonDataFrameViews::from_df_and_opts_unpaginated(df, df_schema, count, &opts);
    let resource = ResourceVersion {
        path: resource.path.to_string_lossy().to_string(),
        version: resource.version.to_string_lossy().to_string(),
    };

    let response = EditableJsonDataFrameViewResponse {
        status: StatusMessage::resource_found(),
        data_frame: df_views,
        resource: Some(resource),
        commit: None, // Not at a committed state
        derived_resource: None,
        is_editable,
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

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Staged dataframes must be on a branch.
    let branch = api::local::branches::get_by_name(&repo, branch_name)?
        .ok_or(OxenError::remote_branch_not_found(branch_name))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?
        .ok_or(OxenError::resource_not_found(&branch.commit_id))?;

    let _workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;

    let entries = api::local::entries::list_tabular_files_in_repo(&repo, &commit)?;

    let mut editable_entries = vec![];

    for entry in entries {
        if let Some(resource) = entry.resource.clone() {
            if index::workspaces::data_frames::is_indexed(
                &repo,
                &commit,
                &workspace_id,
                &resource.path,
            )? {
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
    let resource = parse_resource(&req, &repo)?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    // Remote staged calls must be on a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?
        .ok_or(OxenError::resource_not_found(&branch.commit_id))?;
    let _workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;

    // TODO: Let's not expose dbs right in the controller
    let staged_db_path = liboxen::core::index::workspaces::data_frames::mods_db_path(
        &repo,
        &commit,
        &workspace_id,
        &resource.path,
    );

    let conn = df_db::get_connection(staged_db_path)?;

    let diff_df = staged_df_db::df_diff(&conn)?;

    let df_schema = df_db::get_schema(&conn, TABLE_NAME)?;

    let df_views = JsonDataFrameViews::from_df_and_opts(diff_df, df_schema, &opts);

    let resource = ResourceVersion {
        path: resource.path.to_string_lossy().to_string(),
        version: resource.version.to_string_lossy().to_string(),
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
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let data: DataFramePayload = serde_json::from_str(&body)?;

    let branch = resource
        .branch
        .clone()
        .ok_or_else(|| OxenError::parsed_resource_not_found(resource.to_owned()))?;
    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?
        .ok_or(OxenError::resource_not_found(&branch.commit_id))?;

    // Must instantiate the workspace
    // TODO: Might be better to return a Workspace object that we then need to pass into subsequent calls
    //       to ensure it is initialized rather than relying on the consumer to call this
    let _workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;

    let to_index = data.is_indexed;
    let is_indexed =
        index::workspaces::data_frames::is_indexed(&repo, &commit, &workspace_id, &resource.path)?;

    if !is_indexed && to_index {
        handle_indexing(
            &repo,
            &commit,
            &resource.path,
            &workspace_id,
            &namespace,
            &repo_name,
            &resource,
        )
        .await
    } else if is_indexed && !to_index {
        handle_unindexing(
            &repo,
            &commit,
            &workspace_id,
            &resource.path,
            &namespace,
            &repo_name,
            &resource,
        )
        .await
    } else {
        Ok(HttpResponse::Ok().json(StatusMessage::resource_found()))
    }
}

async fn handle_indexing(
    repo: &LocalRepository,
    commit: &Commit,
    resource_path: &Path,
    workspace_id: &str,
    namespace: &str,
    repo_name: &str,
    resource: &ParsedResource,
) -> Result<HttpResponse, OxenHttpError> {
    match index::workspaces::data_frames::index(repo, commit, workspace_id, resource_path) {
        Ok(_) => {
            log::info!(
                "Dataset indexing completed successfully for {namespace}/{repo_name}/{resource}"
            );
            Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
        }
        Err(err) => {
            log::error!("Failed to index dataset for {namespace}/{repo_name}/{resource}: {err}");
            Err(OxenHttpError::InternalServerError)
        }
    }
}

async fn handle_unindexing(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    resource_path: &PathBuf,
    namespace: &str,
    repo_name: &str,
    resource: &ParsedResource,
) -> Result<HttpResponse, OxenHttpError> {
    match index::workspaces::data_frames::unindex(repo, commit, workspace_id, resource_path) {
        Ok(_) => {
            log::info!(
                "Dataset unindexing completed successfully for {namespace}/{repo_name}/{resource}"
            );
            Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
        }
        Err(err) => {
            log::error!("Failed to unindex dataset for {namespace}/{repo_name}/{resource}: {err}");
            Err(OxenHttpError::InternalServerError)
        }
    }
}
