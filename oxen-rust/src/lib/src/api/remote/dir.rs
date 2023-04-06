use std::path::Path;

use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::PaginatedDirEntries;

pub async fn list_dir(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: impl AsRef<Path>,
    page: usize,
    page_size: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let path = path.as_ref().to_string_lossy();
    let uri = format!("/dir/{commit_or_branch}/{path}?page={page}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("list_page got body: {}", body);
            let response: Result<PaginatedDirEntries, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => Err(OxenError::basic_str(format!(
                    "api::dir::list_dir error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::dir::list_dir Err {err:?} request failed: {url}");
            Err(OxenError::basic_str(err))
        }
    }
}
