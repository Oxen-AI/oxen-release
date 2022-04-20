

use std::path::{PathBuf, Path};
use liboxen::error::OxenError;
use liboxen::model::{RepositoryNew};
use liboxen::api::local::repositories::RepositoryAPI;

pub fn get_sync_dir() -> PathBuf {
    let sync_dir = PathBuf::from(format!("/tmp/oxen/tests/{}", uuid::Uuid::new_v4()));
    sync_dir
}

pub fn create_repo(sync_dir: &Path, name: &str) -> Result<RepositoryNew, OxenError> {
    let api = RepositoryAPI::new(sync_dir);
    let repo = RepositoryNew {name: String::from(name)};
    api.create(&repo)?;
    Ok(repo)
}
