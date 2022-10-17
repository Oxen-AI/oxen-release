use crate::config::UserConfig;
use crate::error::OxenError;

use reqwest::header;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const USER_AGENT: &str = "Oxen";

pub fn new() -> Result<reqwest::Client, OxenError> {
    match builder()?.build() {
        Ok(client) => Ok(client),
        Err(reqwest_err) => Err(OxenError::HTTP(reqwest_err)),
    }
}

pub fn builder() -> Result<reqwest::ClientBuilder, OxenError> {
    let mut headers = header::HeaderMap::new();

    let config = UserConfig::default()?;
    let auth_token = format!("Bearer {}", config.auth_token()?);
    let mut auth_value = match header::HeaderValue::from_str(auth_token.as_str()) {
        Ok(header) => header,
        Err(err) => {
            log::debug!("remote::client::new invalid header value: {}", err);
            return Err(OxenError::basic_str(
                "Error setting request auth. Please check your Oxen config.",
            ));
        }
    };
    auth_value.set_sensitive(true);
    headers.insert(header::AUTHORIZATION, auth_value);

    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .user_agent(format!("{}/{}", USER_AGENT, VERSION)))
}
