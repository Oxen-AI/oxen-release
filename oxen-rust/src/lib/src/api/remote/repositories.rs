use crate::api;
use crate::command;
use crate::config::UserConfig;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteRepository, RepositoryNew};
use crate::view::{RepositoryResolveResponse, RepositoryResponse, StatusMessage};
use serde_json::json;
use url::Url;

/// This url will not have the /oxen prefix, we need to extract the namespace and name and reformat
pub async fn get_by_remote_url(url: &str) -> Result<Option<RemoteRepository>, OxenError> {
    let repo = RepositoryNew::from_url(url)?;
    let parsed_url = Url::parse(url)?;
    let port = if parsed_url.port() == None {
        String::from("")
    } else {
        format!(":{}", parsed_url.port().unwrap())
    };
    let new_url = format!(
        "{}://{}{}/oxen/{}/{}",
        parsed_url.scheme(),
        parsed_url.host_str().unwrap(),
        port,
        repo.namespace,
        repo.name
    );
    get_by_namespaced_url(&new_url).await
}

pub async fn get_by_namespaced_url(url: &str) -> Result<Option<RemoteRepository>, OxenError> {
    let config = UserConfig::default()?;
    let client = reqwest::Client::new();
    log::debug!("api::remote::repositories::get_by_url({})", url);
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        let status = res.status();
        if 404 == status {
            return Ok(None);
        }

        let body = res.text().await?;
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

pub async fn create(
    repository: &LocalRepository,
    namespace: &str,
    name: &str,
    host: &str,
) -> Result<RemoteRepository, OxenError> {
    let config = UserConfig::default()?;
    let url = api::endpoint::url_from_host(host, "");
    let repo_url = format!("{}/{}/{}", url, namespace, name);
    let root_commit = command::root_commit(repository)?;
    let params = json!({ "name": name, "namespace": namespace, "root_commit": root_commit });
    log::debug!("Create remote: {}", url);
    let client = reqwest::Client::new();
    if let Ok(res) = client
        .post(url.to_owned())
        .json(&params)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        let body = res.text().await?;
        // println!("Response: {}", body);
        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(RemoteRepository::from_view(&response.repository, &repo_url)),
            Err(err) => {
                let err = format!(
                    "Could not create or find repository [{}]: {}\n{}",
                    name, err, body
                );
                Err(OxenError::basic_str(&err))
            }
        }
    } else {
        let err = format!("Create repository could not connect to {}. Make sure you have the correct server and that it is running.", url);
        Err(OxenError::basic_str(err))
    }
}

pub async fn delete(repository: &RemoteRepository) -> Result<StatusMessage, OxenError> {
    let config = UserConfig::default()?;
    let client = reqwest::Client::new();
    log::debug!("Deleting repository: {}", repository.url);
    if let Ok(res) = client
        .delete(repository.url.clone())
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        let status = res.status();
        let body = res.text().await?;
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

pub async fn resolve_api_url(url: &str) -> Result<Option<String>, OxenError> {
    let config = UserConfig::default()?;
    let client = reqwest::Client::new();
    log::debug!("api::remote::repositories::resolve_api_url({})", url);
    if let Ok(res) = client
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()?),
        )
        .send()
        .await
    {
        let status = res.status();
        if 404 == status {
            return Ok(None);
        }

        let body = res.text().await?;
        log::debug!(
            "repositories::resolve_api_url {}\nstatus[{}] {}",
            url,
            status,
            body
        );

        let response: Result<RepositoryResolveResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(j_res.repository_api_url)),
            Err(err) => {
                log::debug!("Err: {}", err);
                Err(OxenError::basic_str(&format!(
                    "api::repositories::resolve_api_url() Could not serialize repository [{}]",
                    url
                )))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::resolve_api_url() Request failed",
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::constants;
    use crate::error::OxenError;
    use crate::test;

    #[tokio::test]
    async fn test_create_remote_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();
            let repository =
                api::remote::repositories::create(&local_repo, namespace, &name, test::TEST_HOST)
                    .await?;
            println!("got repository: {:?}", repository);
            assert_eq!(repository.name, name);

            // cleanup
            api::remote::repositories::delete(&repository).await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_by_name() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();
            let repository =
                api::remote::repositories::create(&local_repo, namespace, &name, test::TEST_HOST)
                    .await?;
            let url_repo = api::remote::repositories::get_by_remote_url(&repository.url)
                .await?
                .unwrap();

            assert_eq!(repository.namespace, url_repo.namespace);
            assert_eq!(repository.name, url_repo.name);

            // cleanup
            api::remote::repositories::delete(&repository).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();
            let repository =
                api::remote::repositories::create(&local_repo, namespace, &name, test::TEST_HOST)
                    .await?;

            // delete
            api::remote::repositories::delete(&repository).await?;

            let result = api::remote::repositories::get_by_remote_url(&repository.url).await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());

            Ok(())
        })
        .await
    }
}
