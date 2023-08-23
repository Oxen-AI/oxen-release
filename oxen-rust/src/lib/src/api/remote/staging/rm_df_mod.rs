use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::{ModEntry, ObjectID, RemoteRepository};
use crate::view::StagedFileModResponse;

use std::path::Path;

pub async fn rm_df_mod(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    path: impl AsRef<Path>,
    uuid: &str,
) -> Result<ModEntry, OxenError> {
    let file_name = path.as_ref().to_string_lossy();
    let uri = format!("/staging/{identifier}/df/rows/{branch_name}/{file_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("rm_df_mod [{}] {}", uuid, url);
    let client = client::new_for_url(&url)?;
    let id = ObjectID {
        id: uuid.to_string(),
    };
    let json_id = serde_json::to_string(&id).unwrap();
    match client.delete(&url).body(json_id).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("rm_df_mod got body: {}", body);
            let response: Result<StagedFileModResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.modification),
                Err(err) => {
                    let err = format!("api::staging::rm_df_mod error parsing response from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("rm_df_mod Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}
