use crate::constants;
use crate::model::Remote;
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

pub fn url_from(uri: &str) -> String {
    format!("http://{}{}", server(), uri)
}

pub fn url_from_remote(remote: &Remote, uri: &str) -> String {
    format!("http://{}{}", remote.value, uri)
}

pub fn repo_url_from(name: &str) -> String {
    format!("http://{}/repositories/{}", server(), name)
}
