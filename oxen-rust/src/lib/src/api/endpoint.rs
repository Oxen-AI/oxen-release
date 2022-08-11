use crate::model::{Remote, RemoteRepository};

const API_NAMESPACE: &str = "/oxen";

pub fn url_from_host(host: &str, uri: &str) -> String {
    format!("http://{}{}{}", host, API_NAMESPACE, uri)
}

pub fn url_from_remote(remote: &Remote, uri: &str) -> String {
    format!("http://{}{}{}", remote.url, API_NAMESPACE, uri)
}

pub fn url_from_repo(remote: &RemoteRepository, uri: &str) -> String {
    format!("{}{}", remote.url, uri)
}
