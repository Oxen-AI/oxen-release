use crate::controllers::workspaces::get_content_type;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use actix_web::{web::Bytes, HttpRequest, HttpResponse};
use liboxen::error::OxenError;
use liboxen::model::entry::mod_entry::{ModType, NewMod};
use liboxen::model::{Commit, CommitEntry, ContentType, LocalRepository, Schema};
use liboxen::opts::DFOpts;
use liboxen::view::json_data_frame_view::{JsonDataFrameRowResponse, JsonDataFrameSource};
use liboxen::view::{JsonDataFrameView, JsonDataFrameViews, StatusMessage};
use liboxen::{api, core::index};

pub async fn get(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    log::debug!("get_row with namespace {:?}", namespace);
    log::debug!("get_row with repo_name {:?}", repo_name);
    log::debug!("get_row with workspace_id {:?}", workspace_id);

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let row_id = path_param(&req, "row_id")?;

    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = resource
        .commit
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    // Have to initialize this workspace before we can do any operations on it
    let _workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.ok_or(
        OxenError::revision_not_found(branch.commit_id.to_owned().into()),
    )?;

    // If entry does not exist, create it, and stage it with the first row being the data.

    let entry = api::local::entries::get_commit_entry(&repo, &commit, &resource.path)?
        .ok_or(OxenError::entry_does_not_exist(resource.path.clone()))?;

    let row_df = index::workspaces::data_frames::rows::get_row_by_id(
        &repo,
        &workspace_id,
        entry.path,
        row_id,
    )?;

    let row_id = index::workspaces::data_frames::rows::get_row_id(&row_df)?;
    let row_index = index::workspaces::data_frames::rows::get_row_idx(&row_df)?;

    let opts = DFOpts::empty();
    let row_schema = Schema::from_polars(&row_df.schema().clone());
    let row_df_source = JsonDataFrameSource::from_df(&row_df, &row_schema);
    let row_df_view = JsonDataFrameView::from_df_opts(row_df, row_schema, &opts);

    let response = JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: row_df_source,
            view: row_df_view,
        },
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_found(),
        resource: None,
        row_id,
        row_index,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn create(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace.clone(), repo_name.clone())?;
    let resource = parse_resource(&req, &repo)?;

    // TODO: better error handling for content-types
    let content_type_str = get_content_type(&req).unwrap_or("text/plain");
    let content_type = ContentType::from_http_content_type(content_type_str)?;

    let data = String::from_utf8(bytes.to_vec()).expect("Could not parse bytes as utf8");

    // TODO clean up
    if content_type != ContentType::Json {
        return Err(OxenHttpError::BadRequest(
            "Unsupported content type, must be json".to_string().into(),
        ));
    }

    // If the json has an outer property of "data", serialize the inner object
    let json_value: serde_json::Value = serde_json::from_str(&data)?;
    // TODO yeesh
    let data = if let Some(data_obj) = json_value.get("data") {
        serde_json::to_string(data_obj)?
    } else {
        data
    };

    log::debug!("we got data {:?}", data);

    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = resource
        .commit
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    log::info!(
        "df_add_row {namespace}/{repo_name} on branch {} with commit {} with id {}",
        branch.name,
        commit.id,
        workspace_id
    );

    // Have to initialize this workspace before we can do any operations on it
    let workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;
    log::debug!(
        "stager::df_add_row repo {resource} -> staged repo path {:?}",
        repo.path
    );
    log::debug!(
        "stager::df_add_row workspace {resource} -> staged repo path {:?}",
        workspace.path
    );

    // Make sure the data frame is indexed
    let is_editable =
        index::workspaces::data_frames::is_indexed(&repo, &workspace_id, &resource.path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(resource.path.into()));
    }

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.ok_or(
        OxenError::revision_not_found(branch.commit_id.to_owned().into()),
    )?;

    let entry = api::local::entries::get_commit_entry(&repo, &commit, &resource.path)?
        .ok_or(OxenError::entry_does_not_exist(resource.path.clone()))?;

    let new_mod = NewMod {
        content_type,
        mod_type: ModType::Append,
        entry,
        data,
    };

    let row_df = index::workspaces::data_frames::rows::add(&repo, &workspace_id, &new_mod)?;
    let row_id: Option<String> = index::workspaces::data_frames::rows::get_row_id(&row_df)?;
    let row_index: Option<usize> = index::workspaces::data_frames::rows::get_row_idx(&row_df)?;

    let opts = DFOpts::empty();
    let row_schema = Schema::from_polars(&row_df.schema().clone());
    let row_df_source = JsonDataFrameSource::from_df(&row_df, &row_schema);
    let row_df_view = JsonDataFrameView::from_df_opts(row_df, row_schema, &opts);

    let response = JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: row_df_source,
            view: row_df_view,
        },
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_found(),
        resource: None,
        row_id,
        row_index,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn update(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req).unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let workspace_id = req.match_info().get("workspace_id").unwrap();
    let row_id = req.match_info().get("row_id").unwrap();

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.ok_or(
        OxenError::revision_not_found(branch.commit_id.to_owned().into()),
    )?;

    let entry = api::local::entries::get_commit_entry(&repo, &commit, &resource.path)?
        .ok_or(OxenError::entry_does_not_exist(resource.path.clone()))?;

    // TODO: better error handling for content-types
    let content_type_str = get_content_type(&req).unwrap_or("text/plain");
    let content_type = ContentType::from_http_content_type(content_type_str)?;

    let data = String::from_utf8(bytes.to_vec()).expect("Could not parse bytes as utf8");

    // TODO clean up
    if content_type != ContentType::Json {
        return Err(OxenHttpError::BadRequest(
            "Unsupported content type, must be json".to_string().into(),
        ));
    }

    // If the json has an outer property of "data", serialize the inner object
    let json_value: serde_json::Value = serde_json::from_str(&data)?;
    // TODO yeesh
    let data = if let Some(data_obj) = json_value.get("data") {
        serde_json::to_string(data_obj)?
    } else {
        data
    };

    log::debug!("we got data {:?}", data);

    // Have to initialize this workspace before we can do any operations on it
    let workspace = index::workspaces::init_or_get(&repo, &commit, workspace_id)?;
    log::debug!(
        "stager::df_modify_row repo {resource} -> staged repo path {:?}",
        repo.path
    );
    log::debug!(
        "stager::df_modify_row workspace {resource} -> staged repo path {:?}",
        workspace.path
    );

    let new_mod = NewMod {
        content_type,
        mod_type: ModType::Modify,
        entry,
        data,
    };

    // TODO: Add, delete, and modify should use the resource schema here.
    let modified_row = index::workspaces::data_frames::rows::update(
        &repo,
        &commit,
        workspace_id,
        row_id,
        &new_mod,
    )?;

    let row_index = index::workspaces::data_frames::rows::get_row_idx(&modified_row)?;
    let row_id = index::workspaces::data_frames::rows::get_row_id(&modified_row)?;

    log::debug!("Modified row in controller is {:?}", modified_row);
    let schema = Schema::from_polars(&modified_row.schema());
    Ok(HttpResponse::Ok().json(JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: JsonDataFrameSource::from_df(&modified_row, &schema),
            view: JsonDataFrameView::from_df_opts(modified_row, schema, &DFOpts::empty()),
        },
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_updated(),
        resource: None,
        row_id,
        row_index,
    }))
}

pub async fn delete(req: HttpRequest, _bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req).unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let workspace_id: &str = req.match_info().get("workspace_id").unwrap();
    let row_id: &str = req.match_info().get("row_id").unwrap();
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.ok_or(
        OxenError::revision_not_found(branch.commit_id.to_owned().into()),
    )?;
    let entry = api::local::entries::get_commit_entry(&repo, &commit, &resource.path)?
        .ok_or(OxenError::entry_does_not_exist(resource.path.clone()))?;

    delete_row(&repo, &commit, workspace_id, &entry, row_id)
}

fn delete_row(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    entry: &CommitEntry,
    row_id: &str,
) -> Result<HttpResponse, OxenHttpError> {
    match index::workspaces::data_frames::rows::delete(
        repo,
        commit,
        workspace_id,
        &entry.path,
        row_id,
    ) {
        Ok(df) => {
            let schema = Schema::from_polars(&df.schema());
            Ok(HttpResponse::Ok().json(JsonDataFrameRowResponse {
                data_frame: JsonDataFrameViews {
                    source: JsonDataFrameSource::from_df(&df, &schema),
                    view: JsonDataFrameView::from_df_opts(df, schema, &DFOpts::empty()),
                },
                commit: None,
                derived_resource: None,
                status: StatusMessage::resource_deleted(),
                resource: None,
                row_id: None,
                row_index: None,
            }))
        }
        Err(OxenError::Basic(err)) => {
            log::error!(
                "unable to delete data to file {:?} workspace_id {}. Err: {}",
                entry.path,
                workspace_id,
                err
            );
            Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())))
        }
        Err(err) => {
            log::error!(
                "unable to delete data to file {:?} workspace_id {}. Err: {}",
                entry.path,
                workspace_id,
                err
            );
            Ok(HttpResponse::BadRequest().json(StatusMessage::error(format!("{err:?}"))))
        }
    }
}

pub async fn restore(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req).unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let workspace_id = req.match_info().get("workspace_id").unwrap();
    let row_id = req.match_info().get("row_id").unwrap();

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(resource.to_owned()))?;

    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.ok_or(
        OxenError::revision_not_found(branch.commit_id.to_owned().into()),
    )?;

    let entry = api::local::entries::get_commit_entry(&repo, &commit, &resource.path)?
        .ok_or(OxenError::entry_does_not_exist(resource.path.clone()))?;

    let restored_row = index::workspaces::data_frames::rows::restore(
        &repo,
        &commit,
        workspace_id,
        &entry,
        row_id,
    )?;

    let row_index = index::workspaces::data_frames::rows::get_row_idx(&restored_row)?;
    let row_id = index::workspaces::data_frames::rows::get_row_id(&restored_row)?;

    log::debug!("Restored row in controller is {:?}", restored_row);
    let schema = Schema::from_polars(&restored_row.schema());
    Ok(HttpResponse::Ok().json(JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: JsonDataFrameSource::from_df(&restored_row, &schema),
            view: JsonDataFrameView::from_df_opts(restored_row, schema, &DFOpts::empty()),
        },
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_updated(),
        resource: None,
        row_id,
        row_index,
    }))
}
