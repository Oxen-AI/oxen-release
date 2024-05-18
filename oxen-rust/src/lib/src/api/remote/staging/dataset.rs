use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;

use crate::model::RemoteRepository;
use crate::view::StatusMessage;

use std::path::Path;

pub async fn index_dataset(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    path: &Path,
) -> Result<(), OxenError> {
    let file_path_str = path.to_str().unwrap();
    let uri = format!("/staging/{identifier}/df/index/{branch_name}/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("indexing dataset at path {file_path_str}");

    let client = client::new_for_url(&url)?;
    match client.post(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(_) => Ok(()),
                Err(err) => {
                    let err = format!("api::staging::index_dataset error parsing from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::index_dataset Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}
