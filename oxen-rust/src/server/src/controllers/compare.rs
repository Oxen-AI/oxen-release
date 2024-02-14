use std::path::PathBuf;

use crate::errors::OxenHttpError;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::df::tabular;
use liboxen::core::index::{CommitReader, Merger};
use liboxen::error::OxenError;
use liboxen::message::OxenMessage;
use liboxen::model::compare::tabular_compare::{
    TabularCompareBody, TabularCompareDisplayBody, TabularCompareTargetBody,
};
use liboxen::model::{Commit, DataFrameSize, LocalRepository, Schema};
use liboxen::opts::df_opts::DFOptsView;
use liboxen::opts::DFOpts;
use liboxen::view::compare::{
    CompareCommits, CompareCommitsResponse, CompareEntries, CompareEntryResponse, CompareResult,
    CompareTabularResponse,
};
use liboxen::view::json_data_frame_view::{DFResourceType, DerivedDFResource, JsonDataFrameSource};
use liboxen::view::{
    CompareEntriesResponse, JsonDataFrame, JsonDataFrameView, JsonDataFrameViewResponse,
    JsonDataFrameViews, Pagination, StatusMessage,
};
use liboxen::{api, constants, util};

use crate::helpers::get_repo;
use crate::params::{
    app_data, df_opts_query, parse_base_head, path_param, resolve_base_head, DFOptsQuery,
    PageNumQuery,
};
use liboxen::model::entry::commit_entry::CompareEntry;

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
    log::debug!("in the compare entries controller");
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

pub async fn create_df_compare(
    req: HttpRequest,
    _query: web::Query<DFOptsQuery>,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
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

    let resource_1 = PathBuf::from(data.left.path);
    let resource_2 = PathBuf::from(data.right.path);
    let keys = data.keys;
    let targets = data.compare;
    let display = data.display;

    log::debug!("display is {:?}", display);

    let display_by_column = get_display_by_columns(display);

    log::debug!("display by col is {:?}", display_by_column);

    let compare_id = data.compare_id;

    let commit_1 = api::local::revisions::get(&repository, &data.left.version)?
        .ok_or_else(|| OxenError::revision_not_found(data.left.version.into()))?;
    let commit_2 = api::local::revisions::get(&repository, &data.right.version)?
        .ok_or_else(|| OxenError::revision_not_found(data.right.version.into()))?;

    let entry_1 = api::local::entries::get_commit_entry(&repository, &commit_1, &resource_1)?
        .ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_1.display(), commit_1).into())
        })?;
    let entry_2 = api::local::entries::get_commit_entry(&repository, &commit_1, &resource_2)?
        .ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_2.display(), commit_2).into())
        })?;

    let cpath_1 = CompareEntry {
        commit_entry: Some(entry_1),
        path: resource_1,
    };

    let cpath_2 = CompareEntry {
        commit_entry: Some(entry_2),
        path: resource_2,
    };

    // TODO: Remove the next two lines when we want to allow mapping
    // different keys and targets from left and right file.
    let keys = keys.iter().map(|k| k.left.clone()).collect();
    let targets = get_targets_from_req(targets);

    let result = api::local::compare::compare_files(
        &repository,
        Some(&compare_id),
        cpath_1,
        cpath_2,
        keys,
        targets,
        display_by_column, // TODONOW: add display handling here
        None,
    )?;

    let view = match result {
        CompareResult::Tabular((compare, _)) => {
            let mut messages: Vec<OxenMessage> = vec![];

            if compare.dupes.left > 0 || compare.dupes.right > 0 {
                messages.push(compare.dupes.clone().to_message());
            }

            CompareTabularResponse {
                status: StatusMessage::resource_found(),
                dfs: compare,
                messages,
            }
        }
        _ => Err(OxenError::basic_str("Wrong comparison type"))?,
    };

    Ok(HttpResponse::Ok().json(view))
}

pub async fn update_df_compare(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let compare_id = path_param(&req, "compare_id")?;
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

    let resource_1 = PathBuf::from(data.left.path);
    let resource_2 = PathBuf::from(data.right.path);
    let keys = data.keys;
    let targets = data.compare;
    let display = data.display;

    log::debug!("display is {:?}", display);

    let display_by_column = get_display_by_columns(display);

    log::debug!("display by col is {:?}", display_by_column);

    let commit_1 = api::local::revisions::get(&repository, &data.left.version)?
        .ok_or_else(|| OxenError::revision_not_found(data.left.version.into()))?;
    let commit_2 = api::local::revisions::get(&repository, &data.right.version)?
        .ok_or_else(|| OxenError::revision_not_found(data.right.version.into()))?;

    let entry_1 = api::local::entries::get_commit_entry(&repository, &commit_1, &resource_1)?
        .ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_1.display(), commit_1).into())
        })?;
    let entry_2 = api::local::entries::get_commit_entry(&repository, &commit_1, &resource_2)?
        .ok_or_else(|| {
            OxenError::ResourceNotFound(format!("{}@{}", resource_2.display(), commit_2).into())
        })?;

    let cpath_1 = CompareEntry {
        commit_entry: Some(entry_1),
        path: resource_1,
    };

    let cpath_2 = CompareEntry {
        commit_entry: Some(entry_2),
        path: resource_2,
    };

    // TODO: Remove the next two lines when we want to allow mapping
    // different keys and targets from left and right file.
    let keys = keys.iter().map(|k| k.left.clone()).collect();
    let targets = get_targets_from_req(targets);

    let result = api::local::compare::compare_files(
        &repository,
        Some(&compare_id),
        cpath_1,
        cpath_2,
        keys,
        targets,
        display_by_column, // TODONOW: add display handling here
        None,
    )?;

    let view = match result {
        CompareResult::Tabular((compare, _)) => {
            let mut messages: Vec<OxenMessage> = vec![];

            if compare.dupes.left > 0 || compare.dupes.right > 0 {
                messages.push(compare.dupes.clone().to_message());
            }

            CompareTabularResponse {
                status: StatusMessage::resource_found(),
                dfs: compare,
                messages,
            }
        }
        _ => Err(OxenError::basic_str("Wrong comparison type"))?,
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
        &PathBuf::from(data.left.path.clone()),
    )?
    .ok_or_else(|| {
        OxenError::ResourceNotFound(format!("{}@{}", data.left.path, left_commit).into())
    })?;
    let right_entry = api::local::entries::get_commit_entry(
        &repository,
        &right_commit,
        &PathBuf::from(data.right.path.clone()),
    )?
    .ok_or_else(|| {
        OxenError::ResourceNotFound(format!("{}@{}", data.right.path, right_commit).into())
    })?;

    let cpath_1 = CompareEntry {
        commit_entry: Some(left_entry.clone()),
        path: left_entry.path,
    };

    let cpath_2 = CompareEntry {
        commit_entry: Some(right_entry.clone()),
        path: right_entry.path,
    };

    let maybe_cached_compare = api::local::compare::get_cached_compare(
        &repository,
        &compare_id,
        cpath_1.clone(),
        cpath_2.clone(),
    )?;

    if let Some(compare) = maybe_cached_compare {
        let mut messages: Vec<OxenMessage> = vec![];

        if compare.dupes.left > 0 || compare.dupes.right > 0 {
            messages.push(compare.dupes.clone().to_message());
        }

        let view = CompareTabularResponse {
            status: StatusMessage::resource_found(),
            dfs: compare,
            messages,
        };
        Ok(HttpResponse::Ok().json(view))
    } else {
        Err(OxenHttpError::NotFound)
    }
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
    // let base_head = path_param(&req, "base_head")?;

    let compare_dir = api::local::compare::get_compare_dir(&repo, &compare_id);

    let derived_df_path = compare_dir.join("diff.parquet");

    // TODO: If this structure holds for diff + query, there is some amt of reusability with
    // controllers::df::get logic

    let df = tabular::read_df(derived_df_path, DFOpts::empty())?;
    let og_schema = Schema::from_polars(&df.schema());

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);
    // Clear these for the first transform
    opts.page = None;
    opts.page_size = None;

    let full_height = df.height();
    let full_width = df.width();

    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

    let start = if page == 0 { 0 } else { page_size * (page - 1) };
    let end = page_size * page;

    // We have to run the query param transforms, then paginate separately
    match tabular::transform(df, opts) {
        Ok(view_df) => {
            log::debug!("View df {:?}", view_df);

            let view_width = view_df.width();
            let view_height = view_df.height();

            // Paginate after transform
            let mut paginate_opts = DFOpts::empty();
            paginate_opts.slice = Some(format!("{}..{}", start, end));
            let opts_view = DFOptsView::from_df_opts(&paginate_opts);
            let mut paginated_df = tabular::transform(view_df, paginate_opts)?;

            let total_pages = (view_height as f64 / page_size as f64).ceil() as usize;
            let source_size = DataFrameSize {
                width: full_width,
                height: full_height,
            };

            // Merge the metadata from the original schema
            let mut view_schema = Schema::from_polars(&paginated_df.schema());
            log::debug!("OG schema {:?}", og_schema);
            log::debug!("Pre-Slice schema {:?}", view_schema);
            view_schema.update_metadata_from_schema(&og_schema);

            log::debug!("View schema {:?}", view_schema);

            let df = JsonDataFrame::from_slice(
                &mut paginated_df,
                og_schema.clone(),
                source_size.clone(),
                view_schema.clone(),
            );

            let source_df = JsonDataFrameSource {
                schema: og_schema,
                size: source_size,
            };

            let view_df = JsonDataFrameView {
                data: df.data,
                schema: view_schema,
                size: DataFrameSize {
                    width: view_width,
                    height: view_height,
                },
                pagination: {
                    Pagination {
                        page_number: page,
                        page_size,
                        total_pages,
                        total_entries: view_height,
                    }
                },
                opts: opts_view,
            };

            let derived_resource = DerivedDFResource {
                resource_type: DFResourceType::Compare,
                resource_id: compare_id.clone(),
                path: format!("/compare/data_frame/{}/diff", compare_id),
            };

            let response = JsonDataFrameViewResponse {
                status: StatusMessage::resource_found(),
                data_frame: JsonDataFrameViews {
                    source: source_df,
                    view: view_df,
                },
                commit: None,
                resource: None,
                derived_resource: Some(derived_resource),
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

fn get_display_by_columns(display: Vec<TabularCompareDisplayBody>) -> Vec<String> {
    let mut display_by_column = vec![];
    for d in display {
        if let Some(left) = d.left {
            display_by_column.push(format!("{}.left", left));
        }
        if let Some(right) = d.right {
            display_by_column.push(format!("{}.right", right));
        }
    }
    display_by_column
}

fn get_targets_from_req(targets: Vec<TabularCompareTargetBody>) -> Vec<String> {
    let mut out_targets: Vec<String> = vec![];
    for t in targets {
        if let Some(left) = t.left {
            out_targets.push(left);
        } else if let Some(right) = t.right {
            out_targets.push(right);
        }
    }
    out_targets
}

#[cfg(test)]
mod tests {
    use liboxen::{command, error::OxenError};

    use crate::test;

    #[actix_web::test]
    async fn test_controllers_compare_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();

        let namepsace = "testing-namespace";
        let repo_name = "testing-repo";

        let repo = test::create_local_repo(&sync_dir, namepsace, repo_name)?;

        let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n9,0,1,2";
        let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,8\n0,1,9,2";

        let path1 = repo.path.join("file1.csv");
        let path2 = repo.path.join("file2.csv");

        liboxen::test::write_txt_file_to_path(&path1, csv1)?;
        liboxen::test::write_txt_file_to_path(&path2, csv2)?;

        command::add(&repo, &repo.path)?;

        let status = command::status(&repo)?;

        let commit = command::commit(&repo, "commit 1")?;

        Ok(())
    }
}
