use std::path::PathBuf;

use crate::errors::OxenHttpError;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::df::tabular;
use liboxen::core::index::{CommitReader, Merger};
use liboxen::error::OxenError;
use liboxen::model::compare::tabular_compare::TabularCompareBody;
<<<<<<< HEAD
use liboxen::model::{Commit, DataFrameSize, LocalRepository, Schema};
=======
use liboxen::model::entry::commit_entry::CommitPath;
use liboxen::model::{Commit, LocalRepository};
>>>>>>> main
use liboxen::opts::DFOpts;
use liboxen::view::compare::{
    CompareCommits, CompareCommitsResponse, CompareEntries, CompareEntryResponse,
    CompareTabularResponse,
};
use liboxen::view::json_data_frame::JsonDataFrameOrSlice;
use liboxen::view::{
    CompareEntriesResponse, JsonDataFrame, JsonDataFrameSliceResponse, StatusMessage,
};
use liboxen::{api, constants, util};

use crate::helpers::get_repo;
use crate::params::{
    self, app_data, df_opts_query, parse_base_head, path_param, resolve_base_head, DFOptsQuery,
    PageNumQuery,
};

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

pub async fn file(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
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

    let base_entry = api::local::entries::get_commit_entry(&repository, &base_commit, &resource)?;
    let head_entry = api::local::entries::get_commit_entry(&repository, &head_commit, &resource)?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

    let start = if page == 0 { 0 } else { page_size * (page - 1) };
    let end = page_size * page;
    opts.slice = Some(format!("{}..{}", start, end));

    let diff = api::local::diff::diff_entries(
        &repository,
        base_entry,
        &base_commit,
        head_entry,
        &head_commit,
        opts,
    )?;

    let view = CompareEntryResponse {
        status: StatusMessage::resource_found(),
        compare: diff,
    };
    Ok(HttpResponse::Ok().json(view))
}

// TODONOW, naming - since `compare` namespae already eaten up by diff

pub async fn create_df_compare(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>, // todonow needed?
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let data: Result<TabularCompareBody, serde_json::Error> = serde_json::from_str(&body);
    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!(
                "unable to parse tabular comparison data. Err: {}\n{}",
                err,
                body
            );
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    // TODONOW cleanup
    let resource_1 = PathBuf::from(data.left_resource);
    let resource_2 = PathBuf::from(data.right_resource);
    let keys = data.keys;
    let targets = data.targets;
    let compare_id = data.compare_id;

    let (commit_1, commit_2) = params::parse_base_head(&base_head)?;
    let commit_1 = api::local::revisions::get(&repository, &commit_1)?
        .ok_or_else(|| OxenError::revision_not_found(commit_1.into()))?;
    let commit_2 = api::local::revisions::get(&repository, &commit_2)?
        .ok_or_else(|| OxenError::revision_not_found(commit_2.into()))?;

    let entry_1 = api::local::entries::get_commit_entry(&repository, &commit_1, &resource_1)?
        .ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_1.display(), commit_1).into())
        })?;
    let entry_2 = api::local::entries::get_commit_entry(&repository, &commit_1, &resource_2)?
        .ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_2.display(), commit_2).into())
        })?;

    // Not currently accepting opts from the query string on create,
    // but set up a minimal return of 100 to avoid sending a ton of data on create payload.

    opts.page_size = Some(100);
    opts.page = Some(1);

    let compare = api::local::compare::compare_files(
        &repository,
        Some(&compare_id),
        entry_1,
        entry_2,
        keys,
        targets,
        None,
    )?;

    let view = CompareTabularResponse {
        status: StatusMessage::resource_found(),
        dfs: compare,
    };

    Ok(HttpResponse::Ok().json(view))
}

pub async fn get_df_compare(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let compare_id = path_param(&req, "compare_id")?;
    let repository = get_repo(&app_data.path, namespace, name)?;
    let base_head = path_param(&req, "base_head")?;

    let data: TabularCompareBody = serde_json::from_str(&body)?;

    let (left, right) = parse_base_head(&base_head)?;
    let (left_commit, right_commit) = resolve_base_head(&repository, &left, &right)?;

    let left_commit = left_commit.ok_or(OxenError::revision_not_found(left.into()))?;
    let right_commit = right_commit.ok_or(OxenError::revision_not_found(right.into()))?;

    let left_entry = api::local::entries::get_commit_entry(
        &repository,
        &left_commit,
        &PathBuf::from(data.left_resource.clone()),
    )?
    .ok_or_else(|| {
        OxenError::ResourceNotFound(format!("{}@{}", data.left_resource, left_commit).into())
    })?;
    let right_entry = api::local::entries::get_commit_entry(
        &repository,
        &right_commit,
        &PathBuf::from(data.right_resource.clone()),
    )?
    .ok_or_else(|| {
        OxenError::ResourceNotFound(format!("{}@{}", data.right_resource, right_commit).into())
    })?;

    let maybe_cached_compare = api::local::compare::get_cached_compare(
        &repository,
        &compare_id,
        &left_entry,
        &right_entry,
    )?;

    let view = match maybe_cached_compare {
        Some(compare) => {
            log::debug!("cache hit!");
            CompareTabularResponse {
                status: StatusMessage::resource_found(),
                dfs: compare,
            }
        }
        None => {
            log::debug!("cache miss");
            let compare = api::local::compare::compare_files(
                &repository,
                Some(&compare_id),
                left_entry,
                right_entry,
                data.keys,
                data.targets,
                None,
            )?;
            CompareTabularResponse {
                status: StatusMessage::resource_found(),
                dfs: compare,
            }
        }
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn get_derived_df(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let compare_id = path_param(&req, "compare_id")?;
    let path = path_param(&req, "path")?;

    // TODONOW clean this up...
    let compare_dir = api::local::compare::get_compare_dir(&repo, &compare_id);

    let derived_df_path = compare_dir.join(format!("{}.parquet", path));

    // TODO: If this structure holds for diff + query, there is some amt of reusability with
    // controllers::df::get logic

    let df = tabular::read_df(derived_df_path, DFOpts::empty())?;
    let og_schema = Schema::from_polars(&df.schema());

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);
    // Clear these for the first transform
    opts.page = None;
    opts.page_size = None;

    log::debug!("Full df {:?}", df);

    let full_height = df.height();
    let full_width = df.width();

    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

    let start = if page == 0 { 0 } else { page_size * (page - 1) };
    let end = page_size * page;

    // We have to run the query param transforms, then paginate separately
    match tabular::transform(df, opts) {
        Ok(sliced_df) => {
            log::debug!("Sliced df {:?}", sliced_df);

            let sliced_width = sliced_df.width();
            let sliced_height = sliced_df.height();

            // Paginate after transform
            let mut paginate_opts = DFOpts::empty();
            paginate_opts.slice = Some(format!("{}..{}", start, end));
            let mut paginated_df = tabular::transform(sliced_df, paginate_opts)?;

            let total_pages = (sliced_height as f64 / page_size as f64).ceil() as usize;
            let full_size = DataFrameSize {
                width: full_width,
                height: full_height,
            };

            // Merge the metadata from the original schema
            let mut slice_schema = Schema::from_polars(&paginated_df.schema());
            log::debug!("OG schema {:?}", og_schema);
            log::debug!("Pre-Slice schema {:?}", slice_schema);
            slice_schema.update_metadata_from_schema(&og_schema);

            log::debug!("Slice schema {:?}", slice_schema);

            // TODONOW
            // let resource_version = None;

            let df = JsonDataFrame::from_slice(
                &mut paginated_df,
                og_schema.clone(),
                full_size.clone(),
                slice_schema.clone(),
            );

            let full_df = JsonDataFrameOrSlice {
                data: None,
                schema: og_schema,
                size: full_size,
            };

            let slice_df = JsonDataFrameOrSlice {
                data: Some(df.data),
                schema: slice_schema,
                size: DataFrameSize {
                    width: sliced_width,
                    height: sliced_height,
                },
            };

            let response = JsonDataFrameSliceResponse {
                status: StatusMessage::resource_found(),
                df: full_df,
                slice: slice_df,
                commit: None,
                resource: None,
                page_number: page,
                page_size,
                total_pages,
                total_entries: sliced_height,
            };
            Ok(HttpResponse::Ok().json(response))
        }
        Err(OxenError::SQLParseError(sql)) => {
            log::error!("Error parsing SQL: {}", sql);
            Err(OxenHttpError::SQLParseError(sql))
        }
        Err(e) => {
            log::error!("Error transforming df: {}", e);
            Err(OxenHttpError::InternalServerError)
        }
    }
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

    let base_commit = api::local::revisions::get(repo, base)?
        .ok_or(OxenError::revision_not_found(base.into()))?;

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
