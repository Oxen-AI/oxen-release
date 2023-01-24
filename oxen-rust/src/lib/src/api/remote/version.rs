use crate::api::remote::client;
use crate::error::OxenError;
use crate::view::VersionResponse;

pub async fn get_remote_version(host: &str) -> Result<String, OxenError> {
    let url = format!("http://{}/api/version", host);
    log::debug!("Checking version at url {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        log::debug!("get_remote_version got status: {}", res.status());
        let body = client::parse_json_body(&url, res).await?;
        log::debug!("get_remote_version got body: {}", body);
        let response: Result<VersionResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val.oxen_version),
            Err(_) => Err(OxenError::basic_str(format!(
                "api::version::get_remote_version {} Err parsing response \n\n{}",
                url, body
            ))),
        }
    } else {
        let err = format!(
            "api::version::get_remote_version Err request failed: {}",
            url
        );
        Err(OxenError::basic_str(err))
    }
}
