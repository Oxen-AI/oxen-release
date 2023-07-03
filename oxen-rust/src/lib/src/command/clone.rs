//! # oxen clone
//!
//! Clone data from a remote repository
//!

use std::path::Path;

use crate::constants::DEFAULT_BRANCH_NAME;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::CloneOpts;

pub async fn clone(opts: &CloneOpts) -> Result<LocalRepository, OxenError> {
    match LocalRepository::clone_remote(opts).await {
        Ok(Some(repo)) => Ok(repo),
        Ok(None) => Err(OxenError::remote_repo_not_found(&opts.url)),
        Err(err) => Err(err),
    }
}

pub async fn clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let shallow = false;
    let all = false;
    _clone(url, dst, shallow, all).await
}

pub async fn shallow_clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let shallow = true;
    let all = false;
    _clone(url, dst, shallow, all).await
}

pub async fn deep_clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let shallow = true;
    let all = true;
    _clone(url, dst, shallow, all).await
}

async fn _clone(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
    shallow: bool,
    all: bool,
) -> Result<LocalRepository, OxenError> {
    let opts = CloneOpts {
        url: url.as_ref().to_string(),
        dst: dst.as_ref().to_owned(),
        shallow,
        all,
        branch: DEFAULT_BRANCH_NAME.to_string(),
    };
    clone(&opts).await
}
