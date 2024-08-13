use crate::error::OxenError;
use crate::model::{LocalRepository, StagedData};
use std::path::Path;

pub fn status(repository: &LocalRepository) -> Result<StagedData, OxenError> {
    todo!()
}

pub fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    todo!()
}
