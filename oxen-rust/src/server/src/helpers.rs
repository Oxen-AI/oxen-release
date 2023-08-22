use std::path::Path;

use liboxen::api;
use liboxen::constants::DEFAULT_REDIS_URL;
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

pub fn get_redis_connection() -> Result<redis::Connection, OxenError> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());
    let redis_client = redis::Client::open(redis_url.clone())
        .unwrap_or_else(|_| panic!("Could not connect to redis at {}", redis_url));

    let con = redis_client
        .get_connection()
        .unwrap_or_else(|_| panic!("Failed to get redis connection at {}", redis_url));

    Ok(con)
}
