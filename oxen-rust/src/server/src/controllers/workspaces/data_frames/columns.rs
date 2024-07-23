use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{web::Bytes, HttpRequest, HttpResponse};
use liboxen::error::OxenError;
use liboxen::model::Schema;
use liboxen::opts::DFOpts;
use liboxen::view::data_frames::columns::{
    ColumnToDelete, ColumnToRestore, ColumnToUpdate, NewColumn,
};
use liboxen::view::json_data_frame_view::{JsonDataFrameRowResponse, JsonDataFrameSource};
use liboxen::view::{JsonDataFrameView, JsonDataFrameViews, StatusMessage};
use liboxen::{api, core::index};
use serde_json::{json, Value};

pub async fn create(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace.clone(), repo_name.clone())?;
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let new_column: NewColumn = serde_json::from_str(&body).map_err(|_err| {
        OxenHttpError::BadRequest("Failed to parse NewColumn from request body".into())
    })?;

    log::info!(
        "create column {namespace}/{repo_name} for file {:?} on in workspace id {}",
        file_path,
        workspace_id
    );
    log::debug!("create column with data {:?}", new_column);

    // Get the workspace
    let workspace = index::workspaces::get(&repo, &workspace_id)?;

    // Make sure the data frame is indexed
    let is_editable = index::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    let row_df = index::workspaces::data_frames::columns::add(&workspace, &file_path, &new_column)?;
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

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace.clone(), repo_name.clone())?;
    let file_path = PathBuf::from(path_param(&req, "path")?);
    let column_name = path_param(&req, "column_name")
        .map_err(|_| OxenHttpError::BadRequest("Column name missing in path parameters".into()))?;

    let column_to_delete: ColumnToDelete = ColumnToDelete { name: column_name };

    log::info!(
        "Delete column {namespace}/{repo_name} for file {:?} on in workspace id {}",
        file_path,
        workspace_id
    );
    log::debug!("create column with data {:?}", column_to_delete);

    // Get the workspace
    let workspace = index::workspaces::get(&repo, &workspace_id)?;

    // Make sure the data frame is indexed
    let is_editable = index::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    let row_df =
        index::workspaces::data_frames::columns::delete(&workspace, &file_path, &column_to_delete)?;
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

pub async fn update(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace.clone(), repo_name.clone())?;
    let file_path = PathBuf::from(path_param(&req, "path")?);
    let column_name = path_param(&req, "column_name")
        .map_err(|_| OxenHttpError::BadRequest("Column name missing in path parameters".into()))?;

    let mut body_json: Value = serde_json::from_str(&body).map_err(|_err| {
        OxenHttpError::BadRequest("Failed to parse request body into JSON".into())
    })?;

    if let Some(obj) = body_json.as_object_mut() {
        obj.insert("name".to_string(), json!(column_name));
    } else {
        return Err(OxenHttpError::BadRequest(
            "Request body is not a valid JSON object".into(),
        ));
    }

    let column_to_update: ColumnToUpdate = serde_json::from_value(body_json).map_err(|_err| {
        OxenHttpError::BadRequest(
            "Failed to parse ColumnToUpdate from modified request body".into(),
        )
    })?;

    log::info!(
        "Update column {namespace}/{repo_name} for file {:?} on in workspace id {}",
        file_path,
        workspace_id
    );
    log::debug!("update column with data {:?}", column_to_update);

    // Get the workspace
    let workspace = index::workspaces::get(&repo, &workspace_id)?;

    // Make sure the data frame is indexed
    let is_editable = index::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    let row_df =
        index::workspaces::data_frames::columns::update(&workspace, &file_path, &column_to_update)?;
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

pub async fn restore(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let file_path: PathBuf = PathBuf::from(path_param(&req, "path")?);

    let row_id = path_param(&req, "row_id")?;
    let column_name = path_param(&req, "column_name")
        .map_err(|_| OxenHttpError::BadRequest("Column name missing in path parameters".into()))?;

    let column_to_restore: ColumnToRestore = ColumnToRestore { name: column_name };

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let workspace = index::workspaces::get(&repo, &workspace_id)?;

    let is_editable = index::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    let entry = api::local::entries::get_commit_entry(&repo, &workspace.commit, &file_path)?
        .ok_or(OxenError::entry_does_not_exist(file_path.clone()))?;

    let restored_row = index::workspaces::data_frames::columns::restore(&workspace, &entry, row_id)?;

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
