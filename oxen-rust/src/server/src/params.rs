use std::path::PathBuf;

use liboxen::api;
use liboxen::api::local::resource::parse_resource_from_path;
use liboxen::error::OxenError;
use liboxen::model::{Branch, Commit, LocalRepository, ParsedResource};

use actix_web::HttpRequest;

use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;

pub mod aggregate_query;
pub use aggregate_query::AggregateQuery;

pub mod page_num_query;
pub use page_num_query::PageNumQuery;

pub mod df_opts_query;
pub use df_opts_query::DFOptsQuery;

pub fn app_data(req: &HttpRequest) -> Result<&OxenAppData, OxenHttpError> {
    req.app_data::<OxenAppData>()
        .ok_or(OxenHttpError::AppDataDoesNotExist)
}

pub fn path_param(req: &HttpRequest, param: &str) -> Result<String, OxenHttpError> {
    Ok(req
        .match_info()
        .get(param)
        .ok_or(OxenHttpError::PathParamDoesNotExist(param.into()))?
        .to_string())
}

pub fn parse_resource(
    req: &HttpRequest,
    repo: &LocalRepository,
) -> Result<ParsedResource, OxenHttpError> {
    let resource: PathBuf = PathBuf::from(req.match_info().query("resource"));
    parse_resource_from_path(repo, &resource)?
        .ok_or(OxenError::path_does_not_exist(resource).into())
}

pub fn parse_base_head(base_head: &str) -> Result<(String, String), OxenError> {
    let mut split = base_head.split("..");
    if let (Some(base), Some(head)) = (split.next(), split.next()) {
        Ok((base.to_string(), head.to_string()))
    } else {
        Err(OxenError::basic_str(
            "Could not parse commits. Format should be base..head",
        ))
    }
}

pub fn resolve_base_head_branches(
    repo: &LocalRepository,
    base: &str,
    head: &str,
) -> Result<(Option<Branch>, Option<Branch>), OxenError> {
    let base = resolve_branch(repo, base)?;
    let head = resolve_branch(repo, head)?;
    Ok((base, head))
}

pub fn resolve_base_head(
    repo: &LocalRepository,
    base: &str,
    head: &str,
) -> Result<(Option<Commit>, Option<Commit>), OxenError> {
    let base = resolve_revision(repo, base)?;
    let head = resolve_revision(repo, head)?;
    Ok((base, head))
}

pub fn resolve_revision(
    repo: &LocalRepository,
    revision: &str,
) -> Result<Option<Commit>, OxenError> {
    // Lookup commit by id or branch name
    api::local::revisions::get(repo, revision)
}

pub fn resolve_branch(repo: &LocalRepository, name: &str) -> Result<Option<Branch>, OxenError> {
    // Lookup branch name
    api::local::branches::get_by_name(repo, name)
}
