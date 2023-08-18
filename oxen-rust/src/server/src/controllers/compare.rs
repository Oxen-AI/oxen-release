use std::path::PathBuf;

use crate::errors::OxenHttpError;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::index::{CommitReader, Merger};
use liboxen::error::OxenError;
use liboxen::model::{Commit, LocalRepository};
use liboxen::opts::PaginateOpts;
use liboxen::view::compare::{
    CompareCommits, CompareCommitsResponse, CompareEntries, CompareEntryResponse,
};
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

    log::debug!("Here is basehead {}", base_head);

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

// List one - log this stuff
pub async fn entries(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    log::debug!("Got base head {:?}", base_head);

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

    log::debug!("Got base commit: {}", base_commit);
    log::debug!("Got head commit: {}", head_commit);

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

pub async fn file(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Parse the base and head from the base..head/resource string
    // For Example)
    //   main..feature/add-data/path/to/file.txt
    let (base_commit, head_commit, resource) = parse_base_head_resource(&repository, &base_head)?;

    log::debug!("Got base commit: {}", base_commit);
    log::debug!("Got head commit: {}", head_commit);
    log::debug!("Got resource: {}", resource.display());

    let base_entry = api::local::entries::get_commit_entry(&repository, &base_commit, &resource)?;
    let head_entry = api::local::entries::get_commit_entry(&repository, &head_commit, &resource)?;

    let pagination = PaginateOpts {
        page_num: query.page.unwrap_or(constants::DEFAULT_PAGE_NUM),
        page_size: query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE),
    };

    let diff = api::local::diff::diff_entries(
        &repository,
        base_entry,
        &base_commit,
        head_entry,
        &head_commit,
        pagination,
    )?;

    let view = CompareEntryResponse {
        status: StatusMessage::resource_found(),
        compare: diff,
    };
    Ok(HttpResponse::Ok().json(view))
}

fn parse_base_head_resource(
    repo: &LocalRepository,
    base_head: &str,
) -> Result<(Commit, Commit, PathBuf), OxenError> {
    log::debug!("Parsing base_head_resource: {}", base_head);

    let mut split = base_head.split("..");
    let base = split
        .next()
        .ok_or(OxenError::resource_not_found(base_head))?;
    let head = split
        .next()
        .ok_or(OxenError::resource_not_found(base_head))?;

    log::debug!("Checking base: {}", base);

    let base_commit = api::local::revisions::get(repo, base)?
        .ok_or(OxenError::revision_not_found(base.into()))?;

    log::debug!("Got base_commit: {}", base_commit);

    log::debug!("Checking head: {}", head);
    // Split on / and find longest branch name
    let split_head = head.split('/');
    let mut longest_str = String::from("");
    let mut head_commit: Option<Commit> = None;
    let mut resource: Option<PathBuf> = None;

    for s in split_head {
        let maybe_revision = format!("{}{}", longest_str, s);
        log::debug!("Checking maybe head revision: {}", maybe_revision);
        let commit = api::local::revisions::get(repo, &maybe_revision)?;
        if commit.is_some() {
            head_commit = commit;
            let mut r_str = head.replace(&maybe_revision, "");
            // remove first char from r_str
            r_str.remove(0);
            resource = Some(PathBuf::from(r_str));
        }
        longest_str = format!("{}/", maybe_revision);
    }

    log::debug!("Got head_commit: {:?}", head_commit);
    log::debug!("Got resource: {:?}", resource);

    let head_commit = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;
    let resource = resource.ok_or(OxenError::revision_not_found(head.into()))?;

    Ok((base_commit, head_commit, resource))
}
