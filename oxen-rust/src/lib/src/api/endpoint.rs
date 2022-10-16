use crate::error::OxenError;
use crate::model::{Remote, RemoteRepository};
use url::Url;

const API_NAMESPACE: &str = "/api/repos";

pub fn url_from_host(host: &str, uri: &str) -> String {
    format!("http://{}{}{}", host, API_NAMESPACE, uri)
}

pub fn url_from_remote_url(url: &str) -> Result<String, OxenError> {
    let mut parsed_url = Url::parse(url)?;
    let new_path = format!("{}{}", API_NAMESPACE, parsed_url.path());
    parsed_url.set_path(&new_path);
    Ok(parsed_url.to_string())
}

pub fn url_from_remote(remote: &Remote, uri: &str) -> Result<String, OxenError> {
    let mut parsed_url = Url::parse(&remote.url)?;
    let new_path = format!("{}{}{}", API_NAMESPACE, parsed_url.path(), uri);
    parsed_url.set_path(&new_path);
    Ok(parsed_url.to_string())
}

pub fn url_from_repo(repo: &RemoteRepository, uri: &str) -> Result<String, OxenError> {
    url_from_remote(&repo.remote, uri)
}
