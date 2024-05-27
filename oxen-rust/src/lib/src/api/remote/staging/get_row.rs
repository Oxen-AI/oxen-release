
use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;

use crate::model::RemoteRepository;
use crate::view::json_data_frame_view::JsonDataFrameRowResponse;

use std::path::Path;

pub async fn get_row(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    path: &Path,
    row_id: &str,
) -> Result<JsonDataFrameRowResponse, OxenError> {
    let file_path_str = path.to_str().unwrap();
    let uri = format!("/staging/{identifier}/df/rows/{row_id}/{branch_name}/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("get_row {url}\n{row_id}");

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameRowResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!("api::staging::get_row error parsing response from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::get_row Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}
