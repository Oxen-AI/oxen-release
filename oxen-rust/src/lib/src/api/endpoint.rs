use crate::error::OxenError;
use crate::model::{Remote, RemoteRepository};
use url::Url;

const API_NAMESPACE: &str = "/api/repos";

pub fn url_from_host(host: &str, uri: &str) -> String {
    format!("http://{host}{API_NAMESPACE}{uri}")
}

pub fn remote_url_from_host(host: &str, namespace: &str, name: &str) -> String {
    format!("http://{host}/{namespace}/{name}")
}

pub fn url_from_remote_url(url: &str) -> Result<String, OxenError> {
    log::debug!("creating url_from_remote_url {url:?}");
    match Url::parse(url) {
        Ok(mut parsed_url) => {
            let new_path = format!("{}{}", API_NAMESPACE, parsed_url.path());
            parsed_url.set_path(&new_path);
            Ok(parsed_url.to_string())
        }
        Err(e) => {
            log::warn!("Invalid remote url: {:?}\n{:?}", url, e);
            Err(OxenError::invalid_set_remote_url(url))
        }
    }
}

pub fn url_from_remote(remote: &Remote, uri: &str) -> Result<String, OxenError> {
    log::debug!("creating url_from_remote {remote:?} -> {uri:?}");
    match Url::parse(&remote.url) {
        Ok(mut parsed_url) => {
            let new_path = format!("{}{}{}", API_NAMESPACE, parsed_url.path(), uri);
            // TODO: this is a workaround because to_string was URL encoding characters that we didn't want encoded
            // parsed_url.set_path(&new_path);
            // Ok(parsed_url.to_string())
            parsed_url.set_path("");
            let mut remote_url = parsed_url.to_string();
            remote_url.pop(); // to_string adds a trailing slash we don't want
            Ok(format!("{remote_url}{new_path}"))
        }
        Err(e) => {
            log::warn!("Invalid remote url: {:?}\n{:?}", remote.url, e);
            Err(OxenError::invalid_set_remote_url(&remote.url))
        }
    }
}

pub fn url_from_repo(repo: &RemoteRepository, uri: &str) -> Result<String, OxenError> {
    url_from_remote(&repo.remote, uri)
}
