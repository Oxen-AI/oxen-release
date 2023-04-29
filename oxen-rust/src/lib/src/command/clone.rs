//! # oxen clone
//!
//! Clone data from a remote repository
//!

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
