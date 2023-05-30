use crate::errors::OxenHttpError;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::error::OxenError;
use liboxen::view::{CompareResponse, StatusMessage};
use liboxen::{api, constants, util};

use super::entries::PageNumQuery;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_base_head, path_param, resolve_base_head_branches};

pub async fn show(
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
    let (base_branch, head_branch) = resolve_base_head_branches(&repository, &base, &head)?;
    let base = base_branch.ok_or(OxenError::committish_not_found(base.into()))?;
    let head = head_branch.ok_or(OxenError::committish_not_found(head.into()))?;

    let base_commit = api::local::commits::get_by_id(&repository, &base.commit_id)?
        .ok_or(OxenError::committish_not_found(base.commit_id.into()))?;
    let head_commit = api::local::commits::get_by_id(&repository, &head.commit_id)?
        .ok_or(OxenError::committish_not_found(head.commit_id.into()))?;

    let entries = api::local::diff::list_diff_entries(&repository, &base_commit, &head_commit)?;

    let total_entries = entries.len();
    let (paginated, total_pages) = util::paginate(entries, page, page_size);
    let view = CompareResponse {
        status: StatusMessage::resource_found(),
        base_commit,
        head_commit,
        page_size,
        total_entries,
        page_number: page,
        total_pages,
        entries: paginated,
    };
    Ok(HttpResponse::Ok().json(view))
}
