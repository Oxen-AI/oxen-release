use crate::api;
use crate::command;
use crate::config::{AuthConfig, HTTPConfig};
use crate::constants;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteRepository};
use crate::view::{RepositoryResponse, StatusMessage};
use serde_json::json;

pub fn get_by_namespace_and_name(
    namespace: &str,
    name: &str,
) -> Result<Option<RemoteRepository>, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/{}/{}", namespace, name);
    let url = api::endpoint::url_from_auth_config(&config, &uri);
    get_by_namespaced_url(&url)
}

/// This url will not have the /oxen prefix, we need to extract the namespace and name and reformat
pub fn get_by_remote_url(url: &str) -> Result<Option<RemoteRepository>, OxenError> {
    let repo = LocalRepository::repo_new_from_url(url)?;
    get_by_namespace_and_name(&repo.namespace, &repo.name)
}

pub fn get_by_namespaced_url(url: &str) -> Result<Option<RemoteRepository>, OxenError> {
    let config = AuthConfig::default()?;
    let client = reqwest::blocking::Client::new();
    log::debug!("api::remote::repositories::get_by_url({})", url);
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let status = res.status();
        if 404 == status {
            return Ok(None);
        }

        let body = res.text()?;
        log::debug!(
            "repositories::get_by_url {}\nstatus[{}] {}",
            url,
            status,
            body
        );

        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(RemoteRepository::from_view(&j_res.repository, url))),
            Err(err) => {
                log::debug!("Err: {}", err);
                Err(OxenError::basic_str(&format!(
                    "api::repositories::get_by_url() Could not serialize repository [{}]",
                    url
                )))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::get_by_url() Request failed",
        ))
    }
}

pub fn create(repository: &LocalRepository, host: &str) -> Result<RemoteRepository, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/{}", constants::DEFAULT_NAMESPACE);
    let url = api::endpoint::url_from_host(host, &uri);
    let repo_url = format!("{}/{}", url, repository.name);
    let root_commit = command::root_commit(repository)?;
    let params = json!({ "name": repository.name, "namespace": repository.namespace, "root_commit": root_commit });
    log::debug!("Create remote: {}", url);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .post(url.to_owned())
        .json(&params)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let body = res.text()?;
        // println!("Response: {}", body);
        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(RemoteRepository::from_view(&response.repository, &repo_url)),
            Err(err) => {
                let err = format!(
                    "Could not create or find repository [{}]: {}\n{}",
                    repository.name, err, body
                );
                Err(OxenError::basic_str(&err))
            }
        }
    } else {
        let err = format!("Create repository could not connect to {}. Make sure you have the correct server and that it is running.", url);
        Err(OxenError::basic_str(err))
    }
}

pub fn delete(repository: RemoteRepository) -> Result<StatusMessage, OxenError> {
    let config = AuthConfig::default()?;
    let client = reqwest::blocking::Client::new();
    log::debug!("Deleting repository: {}", repository.url);
    if let Ok(res) = client
        .delete(repository.url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let status = res.status();
        let body = res.text()?;
        let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val),
            Err(_) => Err(OxenError::basic_str(&format!(
                "status_code[{}], could not delete repository \n\n{}",
                status, body
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::delete() Request failed",
        ))
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::config::{AuthConfig, HTTPConfig};
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_create_remote_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|local_repo| {
            let auth_config = AuthConfig::default()?;
            let repository = api::remote::repositories::create(&local_repo, auth_config.host())?;
            println!("got repository: {:?}", repository);
            assert_eq!(repository.name, local_repo.name);

            // cleanup
            api::remote::repositories::delete(repository)?;
            Ok(())
        })
    }

    #[test]
    fn test_get_by_name() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|local_repo| {
            let auth_config = AuthConfig::default()?;
            let repository = api::remote::repositories::create(&local_repo, auth_config.host())?;
            let url_repo = api::remote::repositories::get_by_namespace_and_name(
                &local_repo.namespace,
                &local_repo.name,
            )?
            .unwrap();

            assert_eq!(repository.namespace, url_repo.namespace);
            assert_eq!(repository.name, url_repo.name);

            // cleanup
            api::remote::repositories::delete(repository)?;

            Ok(())
        })
    }

    #[test]
    fn test_delete_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|local_repo| {
            let auth_config = AuthConfig::default()?;
            let repository = api::remote::repositories::create(&local_repo, auth_config.host())?;

            // delete
            api::remote::repositories::delete(repository)?;

            let result = api::remote::repositories::get_by_namespace_and_name(
                &local_repo.namespace,
                &local_repo.name,
            );
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
            Ok(())
        })
    }
}
