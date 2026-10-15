use serde_json::json;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::opts::PaginateOpts;
use crate::view::data_frames::embeddings::EmbeddingColumnsResponse;
use crate::view::json_data_frame_view::WorkspaceJsonDataFrameViewResponse;
use std::path::Path;

use crate::model::RemoteRepository;

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<EmbeddingColumnsResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };
    let uri = format!("/workspaces/{workspace_id}/data_frames/embeddings/columns/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("get_embeddings {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<EmbeddingColumnsResponse, serde_json::Error> = serde_json::from_str(&body);
    Ok(response?)
}

pub async fn neighbors(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    column: impl AsRef<str>,
    embedding: &Vec<f32>,
    paginate_opts: &PaginateOpts,
) -> Result<WorkspaceJsonDataFrameViewResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/embeddings/neighbors/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("get_embeddings {url}");

    let body = json!({
        "column": column.as_ref(),
        "embedding": embedding,
        "page_size": paginate_opts.page_size,
        "page_num": paginate_opts.page_num,
    });
    let body = body.to_string();

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).body(body).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<WorkspaceJsonDataFrameViewResponse, serde_json::Error> =
        serde_json::from_str(&body);
    Ok(response?)
}

pub async fn index(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    column: &str,
    use_background_thread: bool,
) -> Result<EmbeddingColumnsResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/embeddings/columns/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("index_embeddings {url}");

    let data = json!({
        "column": column,
        "use_background_thread": use_background_thread,
    });
    let data = data.to_string();
    let client = client::new_for_url(&url)?;
    let res = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<EmbeddingColumnsResponse, serde_json::Error> = serde_json::from_str(&body);
    Ok(response?)
}

#[cfg(test)]
mod tests {
    use crate::api::client::workspaces::{self, data_frames};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use tokio::time::sleep;

    use crate::config::embedding_config::EmbeddingStatus;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::model::RemoteRepository;
    use crate::opts::{DFOpts, PaginateOpts};
    use crate::test;
    use crate::view::data_frames::embeddings::EmbeddingColumnsResponse;

    use super::{get, index, neighbors};

    /// Polls the embedding index for `path` until its first column reports
    /// [`EmbeddingStatus::Complete`], then returns that response with `columns` non-empty. A
    /// failed request is retried until the 10s deadline.
    async fn wait_for_embedding_index(
        remote_repo: &RemoteRepository,
        workspace_id: &str,
        path: &Path,
    ) -> Result<EmbeddingColumnsResponse, OxenError> {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(response) = get(remote_repo, workspace_id, path).await
                && response.columns.first().map(|c| &c.status) == Some(&EmbeddingStatus::Complete)
            {
                return Ok(response);
            }
            sleep(Duration::from_millis(2)).await;
        }
        Err(OxenError::basic_str(
            "embedding index did not reach Complete within 10s",
        ))
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_index_query_and_download_embeddings() -> Result<(), OxenError> {
        test::run_remote_repo_test_embeddings_jsonl_pushed(|remote_repo| async move {
            let path: PathBuf = Path::new("annotations")
                .join("train")
                .join("embeddings.jsonl");
            let column = "embedding";

            // Index on the request thread
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "inline").await?;
            data_frames::index(&remote_repo, "inline", &path).await?;
            let response = get(&remote_repo, "inline", &path).await?;
            assert_eq!(
                response.columns.len(),
                0,
                "no column is indexed for embeddings yet"
            );
            index(&remote_repo, "inline", &path, column, false).await?;
            let response = get(&remote_repo, "inline", &path).await?;
            assert_eq!(response.columns[0].status, EmbeddingStatus::Complete);

            let opts = DFOpts {
                find_embedding_where: Some(
                    "prompt = 'What is great way to version 0 images?'".to_string(),
                ),
                sort_by_similarity_to: Some(column.to_string()),
                page_size: Some(23),
                ..DFOpts::empty()
            };
            let response = data_frames::get(&remote_repo, "inline", &path, &opts).await?;
            assert_eq!(response.data_frame.expect("a page").view.size.height, 23);

            // The fixture's row i holds the embedding [i.0, i.1, i.2]
            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 2,
            };
            let response = neighbors(
                &remote_repo,
                "inline",
                &path,
                column,
                &vec![3.0, 3.1, 3.2],
                &paginate_opts,
            )
            .await?;
            let view = response.data_frame.expect("a page of neighbors").view;
            assert_eq!(view.size.height, paginate_opts.page_size);
            let rows = view.data.as_array().expect("rows");
            assert_eq!(rows.len(), paginate_opts.page_size);
            assert_eq!(rows[0]["prompt"], "What is great way to version 3 images?");

            // Index on a background thread
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "background").await?;
            data_frames::index(&remote_repo, "background", &path).await?;
            index(&remote_repo, "background", &path, column, true).await?;
            let response = wait_for_embedding_index(&remote_repo, "background", &path).await?;
            assert_eq!(response.columns.len(), 1);
            assert_eq!(response.columns[0].name, column);
            assert_eq!(response.columns[0].vector_length, 3);

            let remote_repo_copy = remote_repo.clone();
            test::run_empty_dir_test_async(|sync_dir| async move {
                let output_path = sync_dir.join("test_download.parquet");
                let opts = DFOpts {
                    find_embedding_where: Some(
                        "prompt = 'What is great way to version 0 images?'".to_string(),
                    ),
                    sort_by_similarity_to: Some(column.to_string()),
                    output: Some(output_path.clone()),
                    ..DFOpts::empty()
                };
                data_frames::download(&remote_repo, "background", &path, &opts).await?;
                // prompt, response, embedding, and the row id
                let df = tabular::read_df(&output_path, DFOpts::empty()).await?;
                assert_eq!(df.width(), 4);
                assert_eq!(df.height(), test::EMBEDDINGS_FIXTURE_ROWS);

                // An invalid query is a clean error, and the server still answers afterwards
                let opts = DFOpts {
                    find_embedding_where: Some("non_existent_column = test".to_string()),
                    sort_by_similarity_to: Some(column.to_string()),
                    ..DFOpts::empty()
                };
                let result = data_frames::get(&remote_repo, "background", &path, &opts).await;
                assert!(result.is_err());
                let result =
                    data_frames::get(&remote_repo, "background", &path, &DFOpts::empty()).await;
                assert!(
                    result.is_ok(),
                    "server should survive an invalid embedding query, got {result:?}"
                );
                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
