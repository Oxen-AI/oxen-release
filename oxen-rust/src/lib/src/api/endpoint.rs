
use crate::config::{AuthConfig, RemoteConfig};
use crate::error::REMOTE_CFG_NOT_FOUND;
use crate::model::{Remote, RemoteRepository};

// TODO: Could do all of these with a trait...
pub fn url_from_remote(remote: &Remote, uri: &str) -> String {
    format!("http://{}{}", remote.url, uri)
}

pub fn url_from_repo(remote: &RemoteRepository, uri: &str) -> String {
    format!("{}{}", remote.url(), uri)
}

pub fn repo_url(remote: &RemoteRepository) -> String {
    let cfg = RemoteConfig::default().expect(REMOTE_CFG_NOT_FOUND);
    let uri = format!("/repositories/{}", remote.name);
    url_from_remote_config(&cfg, &uri)
}

pub fn url_from_auth_config(config: &AuthConfig, uri: &str) -> String {
    format!("http://{}{}", config.host, uri)
}

pub fn url_from_remote_config(config: &RemoteConfig, uri: &str) -> String {
    format!("http://{}{}", config.host, uri)
}
