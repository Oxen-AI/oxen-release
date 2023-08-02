use crate::errors::OxenHttpError;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::index::{CommitReader, Merger};
use liboxen::error::OxenError;
use liboxen::view::compare::{CompareCommits, CompareCommitsResponse, CompareEntries};
use liboxen::view::{CompareEntriesResponse, StatusMessage};
use liboxen::{api, constants, util};

use crate::helpers::get_repo;
use crate::params::{app_data, parse_base_head, path_param, resolve_base_head, PageNumQuery};

pub async fn commits(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Page size and number
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head(&repository, &base, &head)?;

    let base_commit = base_commit.ok_or(OxenError::revision_not_found(base.into()))?;
    let head_commit = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;

    // Check if mergeable
    let merger = Merger::new(&repository)?;

    // Get commits between base and head
    let commit_reader = CommitReader::new(&repository)?;
    let commits =
        merger.list_commits_between_commits(&commit_reader, &base_commit, &head_commit)?;
    let (paginated, pagination) = util::paginate(commits, page, page_size);

    let compare = CompareCommits {
        base_commit,
        head_commit,
        commits: paginated,
    };

    let view = CompareCommitsResponse {
        status: StatusMessage::resource_found(),
        compare,
        pagination,
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn entries(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Page size and number
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head(&repository, &base, &head)?;

    let base_commit = base_commit.ok_or(OxenError::revision_not_found(base.into()))?;
    let head_commit = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;

    let entries_diff = api::local::diff::list_diff_entries(
        &repository,
        &base_commit,
        &head_commit,
        page,
        page_size,
    )?;
    let entries = entries_diff.entries;
    let pagination = entries_diff.pagination;

    let compare = CompareEntries {
        base_commit,
        head_commit,
        counts: entries_diff.counts,
        entries,
    };
    let view = CompareEntriesResponse {
        status: StatusMessage::resource_found(),
        compare,
        pagination,
    };
    Ok(HttpResponse::Ok().json(view))
}
