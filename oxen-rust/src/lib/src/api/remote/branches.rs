use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::{Branch, RemoteRepository};
use crate::view::{BranchResponse, ListBranchesResponse};

use serde_json::json;

pub fn get_by_name(
    repository: &RemoteRepository,
    branch_name: &str,
) -> Result<Option<Branch>, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/branches/{}", branch_name);
    let url = api::endpoint::url_from_repo(repository, &uri);

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
    let url = api::endpoint::url_from_repo(repository, "/branches");
    log::debug!("create_or_get {}", url);

    let params = serde_json::to_string(&json!({ "name": name }))?;

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .post(url)
        .body(params)
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

pub fn list(repository: &RemoteRepository) -> Result<Vec<Branch>, OxenError> {
    let config = AuthConfig::default()?;
    let url = api::endpoint::url_from_repo(repository, "/branches");

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
        let response: Result<ListBranchesResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(j_res.branches),
            Err(err) => {
                log::debug!(
                    "remote::branches::list() Could not serialize response [{}] {}",
                    err,
                    body
                );
                Err(OxenError::basic_str("Could not list remote branches"))
            }
        }
    } else {
        let err = "Failed to list branches";
        log::error!("remote::branches::list() err: {}", err);
        Err(OxenError::basic_str(&err))
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
            let branch = api::remote::branches::create_or_get(remote_repo, name)?;
            assert_eq!(branch.name, name);

            Ok(())
        })
    }

    #[test]
    fn test_get_branch_by_name() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|remote_repo| {
            let branch_name = "my-branch";
            api::remote::branches::create_or_get(remote_repo, branch_name)?;

            let branch = api::remote::branches::get_by_name(remote_repo, branch_name)?;
            assert!(branch.is_some());
            assert_eq!(branch.unwrap().name, branch_name);

            Ok(())
        })
    }

    #[test]
    fn test_list_remote_branches() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|remote_repo| {
            api::remote::branches::create_or_get(remote_repo, "branch-1")?;
            api::remote::branches::create_or_get(remote_repo, "branch-2")?;

            let branches = api::remote::branches::list(remote_repo)?;
            assert_eq!(branches.len(), 3);

            assert!(branches.iter().any(|b| b.name == "branch-1"));
            assert!(branches.iter().any(|b| b.name == "branch-2"));
            assert!(branches.iter().any(|b| b.name == "main"));

            Ok(())
        })
    }
}
