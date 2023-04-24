use std::path::Path;

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, RepositoryNew};

use crate::errors::OxenHttpError;

pub fn get_repo(
    path: &Path,
    namespace: impl AsRef<str>,
    name: impl AsRef<str>,
) -> Result<LocalRepository, OxenHttpError> {
    Ok(
        api::local::repositories::get_by_namespace_and_name(path, &namespace, &name)?.ok_or(
            OxenError::repo_not_found(RepositoryNew::new(&namespace, &name)),
        )?,
    )
}
