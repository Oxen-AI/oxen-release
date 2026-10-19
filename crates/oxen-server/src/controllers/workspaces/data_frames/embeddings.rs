use std::path::{Path, PathBuf};

use crate::errors::OxenHttpError;
use crate::helpers::get_repo_async;
use crate::params::{app_data, path_param};
use crate::tasks;

use actix_web::{HttpRequest, HttpResponse, web::Bytes};
use liboxen::core::db::data_frames::DataFrameError;
use liboxen::core::repo_locks;
use liboxen::error::OxenError;
use liboxen::model::Schema;
use liboxen::opts::{DFOpts, PaginateOpts};
use liboxen::repositories::data_frames::schemas::{
    get_by_path_async, get_staged_schema_with_staged_db_manager_async,
};
use liboxen::repositories::workspaces;
use liboxen::repositories::workspaces::data_frames;
use liboxen::repositories::workspaces::data_frames::{columns, embeddings, is_indexed_async};
use liboxen::view::data_frames::embeddings::{
    EmbeddingColumnsResponse, EmbeddingQuery, IndexEmbeddingRequest,
};
use liboxen::view::entries::ResourceVersion;
use liboxen::view::json_data_frame_view::WorkspaceJsonDataFrameViewResponse;
use liboxen::view::{JsonDataFrameViews, StatusMessage, StatusMessageDescription};

/// Get the embedding status for a data frame
pub async fn get(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();

    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let Some(workspace) = workspaces::get_async(&repo, &workspace_id).await? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let columns =
        tasks::spawn_blocking(move || embeddings::list_indexed_columns(&workspace, &file_path))
            .await
            .map_err(OxenError::from)??;

    let response = EmbeddingColumnsResponse {
        columns,
        status: StatusMessage::resource_found(),
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn neighbors(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();

    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let Some(workspace) = workspaces::get_async(&repo, &workspace_id).await? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let is_indexed = is_indexed_async(&workspace, &file_path).await?;
    log::debug!("neighbors is_indexed: {is_indexed}");
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

    log::debug!("neighbors: Embedding query: {body:?}");
    let request: EmbeddingQuery = serde_json::from_str(&body)?;

    let opts = {
        let mut opts = DFOpts::empty();
        opts.page = Some(request.page_num);
        opts.page_size = Some(request.page_size);
        opts
    };

    let pagination = PaginateOpts {
        page_num: request.page_num,
        page_size: request.page_size,
    };
    let (count, df) = {
        let workspace = workspace.clone();
        let file_path = file_path.clone();
        tasks::spawn_blocking(move || -> Result<_, DataFrameError> {
            let count = data_frames::count(&workspace, &file_path)?;
            let df = embeddings::nearest_neighbors(
                &workspace,
                &file_path,
                &request.column,
                request.embedding,
                &pagination,
                false,
            )?;
            Ok((count, df))
        })
        .await
        .map_err(OxenError::from)??
    };

    let Some(mut df_schema) = get_by_path_async(&repo, &workspace.commit, &file_path).await? else {
        log::warn!("Failed to get schema for data frame {file_path:?}");
        return Err(OxenHttpError::NotFound);
    };

    let resource = ResourceVersion {
        path: file_path.to_string_lossy().to_string(),
        version: workspace.commit.id.to_string(),
    };

    let og_schema = if let Some(schema) =
        get_by_path_async(&repo, &workspace.commit, Path::new(&resource.path)).await?
    {
        schema
    } else {
        Schema::from_polars(df.schema())
    };

    df_schema.update_metadata_from_schema(&og_schema);

    let mut df_views =
        JsonDataFrameViews::from_df_and_opts_unpaginated(df, df_schema, count, &opts).await?;

    let new_schema =
        get_staged_schema_with_staged_db_manager_async(&workspace.workspace_repo, &file_path)
            .await?;
    columns::update_column_schemas(new_schema, &mut df_views);

    let response = WorkspaceJsonDataFrameViewResponse {
        status: StatusMessage::resource_found(),
        data_frame: Some(df_views),
        resource: Some(resource),
        commit: None, // Not at a committed state
        derived_resource: None,
        is_indexed: true,
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Index a column to enable nearest neighbors search
pub async fn post(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();

    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let _write = repo_locks::begin_write(&repo)?;
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let Some(workspace) = workspaces::get_async(&repo, &workspace_id).await? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let Ok(data) = String::from_utf8(bytes.to_vec()) else {
        return Err(OxenHttpError::BadRequest(
            "Could not parse bytes as utf8".to_string().into(),
        ));
    };

    let request: IndexEmbeddingRequest = serde_json::from_str(&data)?;
    let column = request.column;
    let use_background_thread = request.use_background_thread.unwrap_or(false);

    let columns = tasks::spawn_blocking(move || {
        embeddings::index(&workspace, &file_path, &column, use_background_thread)?;
        embeddings::list_indexed_columns(&workspace, &file_path)
    })
    .await
    .map_err(OxenError::from)??;

    let response = EmbeddingColumnsResponse {
        columns,
        status: StatusMessage::resource_found(),
    };

    Ok(HttpResponse::Ok().json(response))
}
