use crate::config::UserConfig;
use crate::error::OxenError;

pub use reqwest::Url;
use reqwest::{header, Client, ClientBuilder, IntoUrl};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const USER_AGENT: &str = "Oxen";

fn get_host_from_url<U: IntoUrl>(url: U) -> Result<String, OxenError> {
    let parsed_url = url.into_url()?;
    let mut host_str = parsed_url.host_str().unwrap_or_default().to_string();
    if let Some(port) = parsed_url.port() {
        host_str = format!("{}:{}", host_str, port);
    }
    Ok(host_str)
}

// TODO: we probably want to create a pool of clients instead of constructing a
// new one for each request so we can take advantage of keep-alive
pub fn new_for_url<U: IntoUrl>(url: U) -> Result<Client, OxenError> {
    let host = get_host_from_url(url)?;
    new_for_host(host)
}

pub fn new_for_host<S: AsRef<str>>(host: S) -> Result<Client, OxenError> {
    match builder_for_host(host.as_ref())?.build() {
        Ok(client) => Ok(client),
        Err(reqwest_err) => Err(OxenError::HTTP(reqwest_err)),
    }
}

pub fn builder_for_url<U: IntoUrl>(url: U) -> Result<ClientBuilder, OxenError> {
    let host = get_host_from_url(url)?;
    builder_for_host(host)
}

pub fn builder_for_host<S: AsRef<str>>(host: S) -> Result<ClientBuilder, OxenError> {
    let builder = builder();

    let config = UserConfig::default()?;
    if let Some(auth_token) = config.auth_token_for_host(host.as_ref()) {
        let auth_header = format!("Bearer {}", auth_token);
        let mut auth_value = match header::HeaderValue::from_str(auth_header.as_str()) {
            Ok(header) => header,
            Err(err) => {
                log::debug!("remote::client::new invalid header value: {}", err);
                return Err(OxenError::basic_str(
                    "Error setting request auth. Please check your Oxen config.",
                ));
            }
        };
        auth_value.set_sensitive(true);
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, auth_value);
        Ok(builder.default_headers(headers))
    } else {
        Ok(builder)
    }
}

fn builder() -> ClientBuilder {
    Client::builder().user_agent(format!("{}/{}", USER_AGENT, VERSION))
}
