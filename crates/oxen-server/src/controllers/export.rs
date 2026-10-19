use actix_web::{HttpRequest, HttpResponse};

use crate::controllers;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo_async;
use crate::params::{app_data, parse_resource_async, path_param};
use crate::tasks;

use liboxen::constants;
use liboxen::error::OxenError;
use liboxen::model::merkle_tree::node::EMerkleTreeNode;
use liboxen::repositories::tree;
use liboxen::view::FileWithHash;

/// Export resource as a zip
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/export/download/{resource}",
    tag = "Export",
    description = "Download a directory as a zip archive. Subject to size limits.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "Wiki-Text"),
        ("resource" = String, Path, description = "Path to directory and branch/commit (e.g. main/data)", example = "main/data"),
    ),
    responses(
        (status = 200, description = "Zip archive of the directory", content_type = "application/zip"),
        (status = 400, description = "Download size exceeds limit"),
        (status = 404, description = "Repository or resource not found")
    )
)]
pub async fn download_zip(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;

    let resource = parse_resource_async(&req, &repo).await?;
    let directory = resource.path.clone();
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;

    let listed = {
        let repo = repo.clone();
        tasks::spawn_blocking(move || -> Result<_, OxenError> {
            let Some(dir_node) =
                tree::get_dir_with_children_recursive(&repo, &commit, &directory, None)?
            else {
                return Ok(None);
            };

            log::debug!("download_dir_as_zip found dir node: {dir_node:?}");
            let files = dir_node.list_files()?;

            let total_bytes: u64 = files
                .values()
                .filter_map(|node| {
                    if let EMerkleTreeNode::File(file_node) = &node.node {
                        Some(file_node.num_bytes())
                    } else {
                        None
                    }
                })
                .sum();
            Ok(Some((files, total_bytes)))
        })
        .await
        .map_err(OxenError::from)??
    };
    let Some((files, total_bytes)) = listed else {
        return Err(OxenHttpError::NotFound);
    };
    if total_bytes > constants::MAX_ZIP_DOWNLOAD_SIZE {
        return Err(OxenHttpError::BadRequest(format!("Download request exceeded size limit ({} bytes) for zip file downloads: found {} bytes", constants::MAX_ZIP_DOWNLOAD_SIZE, total_bytes).into()));
    }

    let files_with_hash: Vec<FileWithHash> = files
        .into_iter()
        .map(|f| FileWithHash {
            path: f.0,
            hash: f.1.hash.to_string(),
        })
        .collect();

    log::debug!(
        "download_dir_as_zip found {} files with total size {}",
        files_with_hash.len(),
        total_bytes
    );

    let response = controllers::versions::stream_versions_zip(&repo, files_with_hash).await?;

    Ok(response)
}
