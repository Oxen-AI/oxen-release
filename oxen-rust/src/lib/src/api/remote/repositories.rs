use crate::api;
use crate::api::remote::client;
use crate::constants::{DEFAULT_HOST, DEFAULT_REMOTE_NAME};
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote, RemoteRepository};
use crate::view::repository::{RepositoryDataTypesResponse, RepositoryDataTypesView};
use crate::view::{NamespaceView, RepositoryResponse, StatusMessage};
use serde_json::json;

/// Gets remote "origin" that is set on the local repo
pub async fn get_default_remote(repo: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let remote = repo
        .get_remote(DEFAULT_REMOTE_NAME)
        .ok_or(OxenError::remote_not_set(DEFAULT_REMOTE_NAME))?;
    let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };
    Ok(remote_repo)
}

pub async fn get_by_remote_repo(
    repo: &RemoteRepository,
) -> Result<Option<RemoteRepository>, OxenError> {
    get_by_remote(&repo.remote).await
}

/// Attempts to find a repo by name on the remote "origin". For example ox/CatDog
pub async fn get_by_name_default(
    name: impl AsRef<str>,
) -> Result<Option<RemoteRepository>, OxenError> {
    get_by_host_remote_name(DEFAULT_HOST, DEFAULT_REMOTE_NAME, name).await
}

pub async fn get_by_host_remote_name(
    host: impl AsRef<str>,
    remote: impl AsRef<str>,
    name: impl AsRef<str>,
) -> Result<Option<RemoteRepository>, OxenError> {
    let name = name.as_ref();
    let url = api::endpoint::remote_url_from_name(host.as_ref(), name);
    log::debug!(
        "api::remote::repositories::get_by_host_remote_name({}) remote url: {}",
        name,
        url
    );
    let remote = Remote {
        name: String::from(remote.as_ref()),
        url,
    };
    get_by_remote(&remote).await
}

pub async fn exists(repo: &RemoteRepository) -> Result<bool, OxenError> {
    let repo = get_by_remote_repo(repo).await?;
    Ok(repo.is_some())
}

pub async fn get_by_remote(remote: &Remote) -> Result<Option<RemoteRepository>, OxenError> {
    get_repo_data_by_remote(remote)
        .await
        .map(|view| view.map(|view| RemoteRepository::from_data_view(&view, remote)))
}

pub async fn get_repo_data_by_remote(
    remote: &Remote,
) -> Result<Option<RepositoryDataTypesView>, OxenError> {
    log::debug!("api::remote::repositories::get_by_remote({:?})", remote);
    let url = api::endpoint::url_from_remote(remote, "")?;
    log::debug!("api::remote::repositories::get_by_remote url: {}", url);

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            if 404 == res.status() {
                return Ok(None);
            }

            let body = client::parse_json_body(&url, res).await?;
            log::debug!("repositories::get_by_remote {}\n {}", url, body);

            let response: Result<RepositoryDataTypesResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(j_res) => Ok(Some(j_res.repository)),
                Err(err) => {
                    log::debug!("Err: {}", err);
                    Err(OxenError::basic_str(format!(
                        "api::repositories::get_by_remote() Could not deserialize repository [{url}]"
                    )))
                }
            }
        }
        Err(err) => {
            log::error!("Failed to get remote url {url}\n{err:?}");
            Err(OxenError::basic_str(format!(
                "api::repositories::get_by_remote() Request failed at url {url}"
            )))
        }
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
    match client.post(&url).json(&params).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;

            log::debug!("repositories::create response {}", body);
            let response: RepositoryResponse = serde_json::from_str(&body)?;
            Ok(RemoteRepository::from_view(
                &response.repository,
                &Remote {
                    url: api::endpoint::remote_url_from_namespace_name(
                        host.as_ref(),
                        namespace,
                        name,
                    ),
                    name: String::from("origin"),
                },
            ))
        }
        Err(err) => {
            log::error!("Failed to create remote url {url}\n{err:?}");
            let err = format!("Create repository could not connect to {url}. Make sure you have the correct server and that it is running.");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn create<S: AsRef<str>>(
    repository: &LocalRepository,
    namespace: &str,
    name: &str,
    host: S,
) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_host(host.as_ref(), "");
    let root_commit = api::local::commits::root_commit(repository)?;
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
                    url: api::endpoint::remote_url_from_namespace_name(
                        host.as_ref(),
                        namespace,
                        name,
                    ),
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
    let url = repository.api_url()?;
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

pub async fn transfer_namespace(
    repository: &RemoteRepository,
    to_namespace: &str,
) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_repo(repository, "/transfer")?;
    let params = serde_json::to_string(&NamespaceView {
        namespace: to_namespace.to_string(),
    })?;

    let client = client::new_for_url(&url)?;

    if let Ok(res) = client.patch(&url).body(params).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);

        match response {
            Ok(response) => {
                // Update remote to reflect new namespace
                let host = api::remote::client::get_host_from_url(&repository.remote.url)?;
                let new_remote_url = api::endpoint::remote_url_from_namespace_name(
                    &host,
                    &response.repository.namespace,
                    &repository.name,
                );
                let new_remote = Remote {
                    url: new_remote_url,
                    name: repository.remote.name.clone(),
                };

                Ok(RemoteRepository::from_view(
                    &response.repository,
                    &new_remote,
                ))
            }
            Err(err) => {
                let err = format!("Could not transfer repository: {err}\n{body}");
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::transfer_namespace() Request failed",
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

            // We delete in a background thread, so give it a second
            std::thread::sleep(std::time::Duration::from_secs(1));

            let result = api::remote::repositories::get_by_remote_repo(&repository).await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_transfer_remote_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();
            let repository =
                api::remote::repositories::create(&local_repo, namespace, &name, test::test_host())
                    .await?;

            let new_namespace = "new-namespace";
            let new_repository =
                api::remote::repositories::transfer_namespace(&repository, new_namespace).await?;

            assert_eq!(new_repository.namespace, new_namespace);
            assert_eq!(new_repository.name, name);

            // Delete repo - cleanup + check for correct remote namespace transferg
            api::remote::repositories::delete(&new_repository).await?;

            Ok(())
        })
        .await
    }
}
