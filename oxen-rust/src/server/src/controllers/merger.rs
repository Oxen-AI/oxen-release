use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_base_head, path_param, resolve_base_head_branches};

use actix_web::{HttpRequest, HttpResponse};

use liboxen::error::OxenError;
use liboxen::core::index::{CommitReader, Merger};
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::merge::{MergeConflictFile, MergeableResponse};
use liboxen::view::StatusMessage;

pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head_branches(&repository, &base, &head)?;
    let base = base_commit.ok_or(OxenError::committish_not_found(base.into()))?;
    let head = head_commit.ok_or(OxenError::committish_not_found(head.into()))?;

    // Check if mergeable
    let merger = Merger::new(&repository)?;
    let is_mergeable = !merger.has_conflicts(&base, &head)?;

    // Get merge conflicts
    let commit_reader = CommitReader::new(&repository)?;
    let conflicts = merger
        .list_conflicts_between_branches(&commit_reader, &base, &head)?
        .iter()
        .map(|p| MergeConflictFile {
            path: p.to_string_lossy().to_string(),
        })
        .collect();

    // Create response object
    let response = MergeableResponse {
        status: String::from(STATUS_SUCCESS),
        status_message: String::from(MSG_RESOURCE_FOUND),
        is_mergeable,
        conflicts,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn merge(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head_branches(&repository, &base, &head)?;
    let base = base_commit.ok_or(OxenError::committish_not_found(base.into()))?;
    let head = head_commit.ok_or(OxenError::committish_not_found(head.into()))?;

    // Check if mergeable
    let merger = Merger::new(&repository)?;
    merger.merge_into_base(&head, &base)?;

    let response = StatusMessage::resource_created();
    Ok(HttpResponse::Ok().json(response))
}
