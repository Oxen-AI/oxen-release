use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::{Commit, LocalRepository};

use actix_web::HttpRequest;

use crate::errors::OxenHttpError;

pub mod df_opts_query;

pub fn get_path_param(req: &HttpRequest, param: &str) -> Result<String, OxenHttpError> {
    Ok(req
        .match_info()
        .get(param)
        .ok_or(OxenHttpError::PathParamDoesNotExist(param.into()))?
        .to_string())
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

pub fn resolve_base_head(
    repo: &LocalRepository,
    base: &str,
    head: &str,
) -> Result<(Option<Commit>, Option<Commit>), OxenError> {
    let base = resolve_committish(repo, base)?;
    let head = resolve_committish(repo, head)?;
    Ok((base, head))
}

pub fn resolve_committish(
    repo: &LocalRepository,
    committish: &str,
) -> Result<Option<Commit>, OxenError> {
    // Lookup commit by id or branch name
    api::local::commits::get_by_id_or_branch(repo, committish)
}
