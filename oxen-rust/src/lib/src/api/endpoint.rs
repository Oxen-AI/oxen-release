use crate::config::AuthConfig;
use crate::constants;
use crate::model::{Remote, RemoteRepository};
use std::env;

pub fn host() -> String {
    match env::var("HOST") {
        Ok(host) => host,
        Err(_) => String::from(constants::DEFAULT_ORIGIN_HOST),
    }
}

pub fn port() -> String {
    match env::var("POST") {
        Ok(port) => port,
        Err(_) => String::from(constants::DEFAULT_ORIGIN_PORT),
    }
}

pub fn server() -> String {
    format!("{}:{}", host(), port())
}

// TODO: Could do both of these with a HasUrl trait...
pub fn url_from_remote(remote: &Remote, uri: &str) -> String {
    format!("http://{}{}", remote.url, uri)
}

pub fn url_from_repo(remote: &RemoteRepository, uri: &str) -> String {
    format!("{}{}", remote.url, uri)
}

pub fn url_from_config(config: &AuthConfig, uri: &str) -> String {
    format!("http://{}{}", config.host, uri)
}

pub fn repo_url_from(name: &str) -> String {
    format!("http://{}/repositories/{}", server(), name)
}
