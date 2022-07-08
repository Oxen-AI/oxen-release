
use crate::config::{RemoteConfig, AuthConfig};
use crate::model::{Remote, RemoteRepository};

// TODO: Could do all of these with a trait...
pub fn url_from_remote(remote: &Remote, uri: &str) -> String {
    format!("http://{}{}", remote.url, uri)
}

pub fn url_from_repo(remote: &RemoteRepository, uri: &str) -> String {
    format!("{}{}", remote.url, uri)
}

pub fn url_from_auth_config(config: &AuthConfig, uri: &str) -> String {
    format!("http://{}{}", config.host, uri)
}

pub fn url_from_remote_config(config: &RemoteConfig, uri: &str) -> String {
    format!("http://{}{}", config.host, uri)
}
