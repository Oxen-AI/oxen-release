

use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository};
use crate::view::{BranchResponse};

use serde_json::json;

pub fn get_remote_branch(repository: &LocalRepository, branch_name: &str) -> Result<Option<Branch>, OxenError> {
    let config = AuthConfig::default()?;
    let remote = repository.remote().ok_or(OxenError::remote_not_set())?;
    let uri = format!("/repositories/{}/branches/{}", repository.name, branch_name);
    let url = api::endpoint::url_from_remote(&remote, &uri);

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let body = res.text()?;
        let response: Result<BranchResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(j_res.branch)),
            Err(err) => {
                log::debug!("get_remote_head() Could not serialize response [{}] {}", err, body);
                Ok(None)
            },
        }
    } else {
        Err(OxenError::basic_str("get_remote_head() Request failed"))
    }
}

pub fn create_or_get(repository: &LocalRepository, name: &str) -> Result<Branch, OxenError> {
    let remote = repository.remote().ok_or(OxenError::remote_not_set())?;
    let config = AuthConfig::default()?;
    let uri = format!("/repositories/{}/branches", repository.name);
    let url = api::endpoint::url_from_remote(&remote, &uri);
    let params = json!({ "name": name });

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .post(url)
        .json(&params)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let body = res.text()?;
        let response: Result<BranchResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(response.branch),
            Err(err) => {
                let err = format!(
                    "Could not create or find branch [{}]: {}\n{}",
                    repository.name, err, body
                );
                Err(OxenError::basic_str(&err))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "create_or_get() Could not create branch",
        ))
    }
}
