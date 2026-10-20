use crate::errors::OxenHttpError;
use crate::helpers::get_repo_async;
use crate::params::{app_data, parse_two_dot, path_param, resolve_base_head_branches};
use crate::tasks;

use actix_web::{HttpRequest, HttpResponse};

use liboxen::core::repo_locks;
use liboxen::error::OxenError;
use liboxen::model::User;
use liboxen::repositories;
use liboxen::repositories::commits::get_by_id_async;
use liboxen::repositories::merge::list_commits_between_branches;
use liboxen::view::StatusMessage;
use liboxen::view::merge::{
    MergeConflictFile, MergeResult, MergeSuccessResponse, Mergeable, MergeableResponse,
};

/// Parse the merge author (the user who initiated the merge) from the request body. Returns `None`
/// when the body is not a `User` with both a non-empty name and email.
fn parse_merge_author(body: &str) -> Option<User> {
    let user: User = serde_json::from_str(body).ok()?;
    if user.name.trim().is_empty() || user.email.trim().is_empty() {
        return None;
    }
    Some(user)
}

/// Check if branches are mergeable
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/merge/{base_head}",
    tag = "Merge",
    description = "Check if two branches can be merged and list any conflicts.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "satellite-images"),
        ("base_head" = String, Path, description = "The base and head revisions separated by '..'", example = "main..feature/add-labels"),
    ),
    responses(
        (status = 200, description = "Merge status returned successfully", body = MergeableResponse),
        (status = 404, description = "Repository or one of the revisions not found")
    )
)]
pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let base_head = path_param(&req, "base_head")?.to_string();

    // Get the repository or return error
    let repository = get_repo_async(app_data, &namespace, &name).await?;

    // Parse the base and head from the base..head string
    let (base, head) = parse_two_dot(&base_head)?;
    let (base_commit, head_commit) = {
        let repository = repository.clone();
        let (base, head) = (base.clone(), head.clone());
        tasks::spawn_blocking(move || resolve_base_head_branches(&repository, &base, &head))
            .await
            .map_err(OxenError::from)??
    };
    let base = base_commit.ok_or_else(|| OxenError::RevisionNotFound(base.into()))?;
    let head = head_commit.ok_or_else(|| OxenError::RevisionNotFound(head.into()))?;

    // Check if mergeable
    let conflicts =
        repositories::merge::list_conflicts_between_branches(&repository, &base, &head).await?;
    let conflicts: Vec<MergeConflictFile> = conflicts
        .into_iter()
        .map(|path| MergeConflictFile {
            path: path.to_string_lossy().to_string(),
        })
        .collect();
    let is_mergeable = conflicts.is_empty();

    // Get commits
    let commits =
        tasks::spawn_blocking(move || list_commits_between_branches(&repository, &base, &head))
            .await
            .map_err(OxenError::from)??;

    // Create response object
    let response = MergeableResponse {
        status: StatusMessage::resource_found(),
        mergeable: Mergeable {
            is_mergeable,
            conflicts,
            commits,
        },
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Merge branches
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/merge/{base_head}",
    tag = "Merge",
    description = "Merge the head branch into the base branch, creating a merge commit.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "satellite-images"),
        ("base_head" = String, Path, description = "The base and head revisions separated by '..'", example = "main..feature/add-labels"),
    ),
    request_body(
        content = inline(User),
        description = "Author for the merge commit (the user initiating the merge). Required: \
            name and email must both be present and non-empty.",
        example = json!({ "name": "bessie", "email": "bessie@oxen.ai" })
    ),
    responses(
        (status = 200, description = "Branches merged successfully", body = MergeSuccessResponse),
        (status = 409, description = "Merge conflict", body = StatusMessage),
        (status = 404, description = "Repository or one of the revisions not found"),
        (status = 400, description = "Missing merge author (name and email)"),
    )
)]
pub async fn merge(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let base_head = path_param(&req, "base_head")?.to_string();

    // The user who initiated the merge authors the merge commit, and is required.
    let Some(author) = parse_merge_author(&body) else {
        return Err(OxenHttpError::BadRequest(
            "A merge author (name and email) is required".into(),
        ));
    };

    // Get the repository or return error
    let repo = get_repo_async(app_data, &namespace, &name).await?;
    let _write = repo_locks::begin_write(&repo)?;

    // Parse the base and head from the base..head string
    let (base, head) = parse_two_dot(&base_head)?;
    let (maybe_base_branch, maybe_head_branch) = {
        let repo = repo.clone();
        let (base, head) = (base.clone(), head.clone());
        tasks::spawn_blocking(move || resolve_base_head_branches(&repo, &base, &head))
            .await
            .map_err(OxenError::from)??
    };
    let Some(base_branch) = maybe_base_branch else {
        return Err(OxenError::RevisionNotFound(base.into()).into());
    };
    let Some(head_branch) = maybe_head_branch else {
        return Err(OxenError::RevisionNotFound(head.into()).into());
    };

    let base_commit = get_by_id_async(&repo, &base_branch.commit_id)
        .await?
        .ok_or_else(|| OxenError::RevisionNotFound(base_branch.commit_id.clone().into()))?;
    let head_commit = get_by_id_async(&repo, &head_branch.commit_id)
        .await?
        .ok_or_else(|| OxenError::RevisionNotFound(head_branch.commit_id.clone().into()))?;

    let merge_commit =
        repositories::merge::merge_into_base(&repo, &head_branch, &base_branch, &author).await?;

    let response = MergeSuccessResponse {
        status: StatusMessage::resource_found(),
        commits: MergeResult {
            base: base_commit,
            head: head_commit,
            merge: merge_commit,
        },
    };

    Ok(HttpResponse::Ok().json(response))
}
