use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::{Branch, RemoteRepository};
use crate::view::BranchResponse;

use serde_json::json;

pub fn get_by_name(
    repository: &RemoteRepository,
    branch_name: &str,
) -> Result<Option<Branch>, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/repositories/{}/branches/{}", repository.name, branch_name);
    let url = api::endpoint::url_from(&uri);

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
        let response: Result<BranchResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(j_res.branch)),
            Err(err) => {
                log::debug!(
                    "remote::branches::get_by_name() Could not serialize response [{}] {}",
                    err,
                    body
                );
                Ok(None)
            }
        }
    } else {
        let err = "Failed to get branch";
        log::error!("remote::branches::get_by_name() err: {}", err);
        Err(OxenError::basic_str(&err))
    }
}

pub fn create_or_get(repository: &RemoteRepository, name: &str) -> Result<Branch, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/repositories/{}/branches", repository.name);
    let url = api::endpoint::url_from(&uri);
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
        let response: Result<BranchResponse, serde_json::Error> = serde_json::from_str(&body);
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
        let msg = format!("Could not create branch {}", name);
        log::error!("remote::branches::create_or_get() {}", msg);
        Err(OxenError::basic_str(&msg))
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_create_remote_branch() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|remote_repo| {
            let name = "my-branch";
            let branch = api::remote::branches::create_or_get(&remote_repo, name)?;
            assert_eq!(branch.name, name);

            Ok(())
        })
    }

    #[test]
    fn test_get_by_name() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|remote_repo| {
            let branch_name = "my-branch";
            api::remote::branches::create_or_get(&remote_repo, branch_name)?;

            let branch = api::remote::branches::get_by_name(&remote_repo, branch_name)?;
            assert!(branch.is_some());
            assert_eq!(branch.unwrap().name, branch_name);

            Ok(())
        })
    }
}
