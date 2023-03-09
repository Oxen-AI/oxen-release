use crate::api;
use crate::api::remote::client;
use crate::command;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote, RemoteRepository};
use crate::view::{RepositoryResolveResponse, RepositoryResponse, StatusMessage};
use serde_json::json;

pub async fn get_by_remote_repo(
    repo: &RemoteRepository,
) -> Result<Option<RemoteRepository>, OxenError> {
    get_by_remote(&repo.remote).await
}

pub async fn get_by_remote(remote: &Remote) -> Result<Option<RemoteRepository>, OxenError> {
    let url = api::endpoint::url_from_remote(remote, "")?;
    log::debug!("api::remote::repositories::get_by_remote({})", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        if 404 == res.status() {
            return Ok(None);
        }

        let body = client::parse_json_body(&url, res).await?;
        log::debug!("repositories::get_by_remote {}\n {}", url, body);

        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(RemoteRepository::from_view(&j_res.repository, remote))),
            Err(err) => {
                log::debug!("Err: {}", err);
                Err(OxenError::basic_str(format!(
                    "api::repositories::get_by_remote() Could not deserialize repository [{url}]"
                )))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::get_by_remote() Request failed",
        ))
    }
}

pub async fn create_no_root<S: AsRef<str>>(
    namespace: &str,
    name: &str,
    host: S,
) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_host(host.as_ref(), "");
    let params = json!({ "name": name, "namespace": namespace });
    log::debug!("Create remote: {} {} {}", url, namespace, name);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).json(&params).send().await {
        let body = client::parse_json_body(&url, res).await?;

        log::debug!("repositories::create response {}", body);
        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(RemoteRepository::from_view(
                &response.repository,
                &Remote {
                    url: api::endpoint::remote_url_from_host(host.as_ref(), namespace, name),
                    name: String::from("origin"),
                },
            )),
            Err(err) => {
                let err = format!("Could not create or find repository [{name}]: {err}\n{body}");
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        let err = format!("Create repository could not connect to {url}. Make sure you have the correct server and that it is running.");
        Err(OxenError::basic_str(err))
    }
}

pub async fn create<S: AsRef<str>>(
    repository: &LocalRepository,
    namespace: &str,
    name: &str,
    host: S,
) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_host(host.as_ref(), "");
    let root_commit = command::root_commit(repository)?;
    let params = json!({ "name": name, "namespace": namespace, "root_commit": root_commit });
    log::debug!("Create remote: {} {} {}", url, namespace, name);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).json(&params).send().await {
        let body = client::parse_json_body(&url, res).await?;

        log::debug!("repositories::create response {}", body);
        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(RemoteRepository::from_view(
                &response.repository,
                &Remote {
                    url: api::endpoint::remote_url_from_host(host.as_ref(), namespace, name),
                    name: String::from("origin"),
                },
            )),
            Err(err) => {
                let err = format!("Could not create or find repository [{name}]: {err}\n{body}");
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        let err = format!("Create repository could not connect to {url}. Make sure you have the correct server and that it is running.");
        Err(OxenError::basic_str(err))
    }
}

pub async fn delete(repository: &RemoteRepository) -> Result<StatusMessage, OxenError> {
    let url = api::endpoint::url_from_repo(repository, "")?;
    log::debug!("Deleting repository: {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.delete(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val),
            Err(_) => Err(OxenError::basic_str(format!(
                "Could not delete repository \n\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::delete() Request failed",
        ))
    }
}

pub async fn resolve_api_url(url: &str) -> Result<Option<String>, OxenError> {
    log::debug!("api::remote::repositories::resolve_api_url({})", url);
    let client = client::new_for_url(url)?;
    if let Ok(res) = client.get(url).send().await {
        let status = res.status();
        if 404 == status {
            return Ok(None);
        }

        let body = client::parse_json_body(url, res).await?;
        log::debug!("repositories::resolve_api_url {}\n{}", url, body);

        let response: Result<RepositoryResolveResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(j_res.repository_api_url)),
            Err(err) => {
                log::debug!("Err: {}", err);
                Err(OxenError::basic_str(format!(
                    "api::repositories::resolve_api_url() Could not deserialize repository [{url}]"
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
                api::remote::repositories::create(&local_repo, namespace, &name, test::test_host())
                    .await?;
            println!("got repository: {repository:?}");
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
                api::remote::repositories::create(&local_repo, namespace, &name, test::test_host())
                    .await?;
            let url_repo = api::remote::repositories::get_by_remote_repo(&repository)
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
                api::remote::repositories::create(&local_repo, namespace, &name, test::test_host())
                    .await?;

            // delete
            api::remote::repositories::delete(&repository).await?;

            let result = api::remote::repositories::get_by_remote_repo(&repository).await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());

            Ok(())
        })
        .await
    }
}
