use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{web::Bytes, HttpRequest, HttpResponse};
use liboxen::error::OxenError;
use liboxen::model::Schema;
use liboxen::opts::DFOpts;
use liboxen::view::data_frames::columns::NewColumn;
use liboxen::view::json_data_frame_view::{JsonDataFrameRowResponse, JsonDataFrameSource};
use liboxen::view::{JsonDataFrameView, JsonDataFrameViews, StatusMessage};
use liboxen::{api, core::index};

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
        "create row {namespace}/{repo_name} for file {:?} on in workspace id {}",
        file_path,
        workspace_id
    );
    log::debug!("create row with data {:?}", new_column);

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
