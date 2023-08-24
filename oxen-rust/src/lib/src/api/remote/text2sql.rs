use crate::error::OxenError;
use crate::{api::remote::client, constants::DEFAULT_HOST};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SqlResponse {
    pub sql: String,
}

pub async fn convert(query: &str, schema: &str, host: Option<String>) -> Result<String, OxenError> {
    let query = urlencoding::encode(query);
    let schema = urlencoding::encode(schema);
    let host = match host {
        Some(host) => host,
        None => DEFAULT_HOST.to_string(),
    };
    let url = format!("http://{host}/api/df/text2sql?query={query}&schema={schema}");
    log::debug!("text2sql url: {}", url);
    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("text2sql got body: {}", body);
            let response: Result<SqlResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.sql),
                Err(err) => Err(OxenError::basic_str(format!(
                    "text2sql error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("text2sql Err {err:?} request failed: {url}");
            Err(OxenError::basic_str(err))
        }
    }
}
