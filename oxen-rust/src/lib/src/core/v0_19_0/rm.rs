use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;

pub async fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    Ok(())
}
