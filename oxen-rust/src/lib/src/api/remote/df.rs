use std::path::Path;

use polars::prelude::DataFrame;

use crate::api;
use crate::df::DFOpts;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::json_data_frame::JsonDataSize;
use crate::view::JsonDataFrameSliceResponse;

use super::client;

pub async fn show(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: impl AsRef<Path>,
    opts: DFOpts,
) -> Result<(DataFrame, JsonDataSize), OxenError> {
    let path_str = path.as_ref().to_str().unwrap();
    let query_str = opts.to_http_query_params();
    let uri = format!("/df/{commit_or_branch}/{path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {}", body);
            let response: Result<JsonDataFrameSliceResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got JsonDataFrameSliceResponse: {:?}", val);
                    let df = val.df.to_df();
                    Ok((df, val.full_size))
                }
                Err(err) => Err(OxenError::basic_str(format!(
                    "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}
