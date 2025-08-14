use std::path::{Path, PathBuf};

// use liboxen::constants::DEFAULT_REDIS_URL;
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, RepoNew};
use liboxen::repositories;

use crate::errors::OxenHttpError;

pub fn get_repo(
    path: &Path,
    namespace: impl AsRef<str>,
    name: impl AsRef<str>,
) -> Result<LocalRepository, OxenHttpError> {
    let repo = repositories::get_by_namespace_and_name(path, &namespace, &name)?;
    let Some(repo) = repo else {
        return Err(
            OxenError::repo_not_found(RepoNew::from_namespace_name(&namespace, &name)).into(),
        );
    };

    Ok(repo)
}

pub fn get_repo_path(
    base_path: &Path,
    namespace: impl AsRef<str>,
    repo_name: impl AsRef<str>,
) -> PathBuf {
    base_path.join(namespace.as_ref()).join(repo_name.as_ref())
}

// #[allow(dependency_on_unit_never_type_fallback)]
// pub fn get_redis_connection() -> Result<r2d2::Pool<redis::Client>, OxenError> {
//     let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());
//     let redis_client = redis::Client::open(redis_url)?;

//     // First, ping redis to see if available - builder retries infinitely and spews error messages
//     let mut test_conn = redis_client.get_connection()?;
//     let _: () = redis::cmd("ECHO").arg("test").query(&mut test_conn)?;

//     // If echo test didn't error, init the builder
//     let pool = r2d2::Pool::builder().build(redis_client)?;
//     Ok(pool)
// }
