
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{
    app_data, df_opts_query, parse_resource, path_param, DFOptsQuery, PageNumQuery,
};

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::constants::TABLE_NAME;
use liboxen::core::db::{df_db, staged_df_db};
use liboxen::error::OxenError;
use liboxen::model::Schema;
use liboxen::opts::DFOpts;
use liboxen::util::paginate;
use liboxen::view::entry::ResourceVersion;
use liboxen::view::entry::{PaginatedMetadataEntries, PaginatedMetadataEntriesResponse};
use liboxen::view::remote_staged_status::DFIsEditableResponse;
use liboxen::view::{JsonDataFrameViewResponse, JsonDataFrameViews, StatusMessage};
use liboxen::{api, constants, core::index};

pub mod row;

pub async fn get(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let identifier = path_param(&req, "identifier")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource
        .clone()
        .commit
        .ok_or(OxenError::resource_not_found(
            resource.version.to_string_lossy(),
        ))?;

    log::debug!(
        "controllers::workspace::data_frame::get resource: {:?}",
        resource
    );
    log::debug!(
        "controllers::workspace::data_frame::get commit: {:?}",
        commit
    );

    let entry = api::local::entries::get_commit_entry(&repo, &commit, &resource.path)?
        .ok_or(OxenError::entry_does_not_exist(resource.path.clone()))?;

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

    let _branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch, &identifier)?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    if !index::remote_df_stager::dataset_is_indexed(&repo, &branch, &identifier, &resource.path)? {
        return Err(OxenHttpError::DatasetNotIndexed(resource.path.into()));
    }

    let count = index::remote_df_stager::count(&repo, &branch, resource.path.clone(), &identifier)?;

    let df = index::remote_df_stager::query_staged_df(&repo, &entry, &branch, &identifier, &opts)?;

    let df_schema = Schema::from_polars(&df.schema());

    let df_views = JsonDataFrameViews::from_df_and_opts_unpaginated(df, df_schema, count, &opts);
    let resource = ResourceVersion {
        path: resource.path.to_string_lossy().to_string(),
        version: resource.version.to_string_lossy().to_string(),
    };

    let response = JsonDataFrameViewResponse {
        status: StatusMessage::resource_found(),
        data_frame: df_views,
        resource: Some(resource),
        commit: None, // Not at a committed state
        derived_resource: None,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn list(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req).unwrap();

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let identifier = path_param(&req, "identifier")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let branch_name: &str = req.match_info().query("branch");

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Staged dataframes must be on a branch.
    let branch = api::local::branches::get_by_name(&repo, branch_name)?
        .ok_or(OxenError::remote_branch_not_found(branch_name))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?
        .ok_or(OxenError::resource_not_found(&branch.commit_id))?;

    let _branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch, &identifier)?;

    let entries = api::local::entries::list_tabular_files_in_repo(&repo, &commit)?;

    let mut editable_entries = vec![];

    for entry in entries {
        if let Some(resource) = entry.resource.clone() {
            if index::remote_df_stager::dataset_is_indexed(
                &repo,
                &branch,
                &identifier,
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

// TODO: This should be a more generic info or metadata api about a resource
pub async fn is_editable(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req).unwrap();

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let identifier = path_param(&req, "identifier")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} indexing dataset for resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );

    // Staged dataframes must be on a branch.
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let _branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch, &identifier)?;

    let is_editable =
        index::remote_df_stager::dataset_is_indexed(&repo, &branch, &identifier, &resource.path)?;

    Ok(HttpResponse::Ok().json(DFIsEditableResponse {
        status: StatusMessage::resource_found(),
        is_editable,
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
    let identifier = path_param(&req, "identifier")?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    // Remote staged calls must be on a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let _branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch, &identifier)?;

    let staged_db_path = liboxen::core::index::mod_stager::mods_df_db_path(
        &repo,
        &branch,
        &identifier,
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
