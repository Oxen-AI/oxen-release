use crate::api;
use crate::command;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::{RemoteRepository, LocalRepository};
use crate::view::{RemoteRepositoryResponse, StatusMessage};
use serde_json::json;

pub fn get_by_url(url: &str) -> Result<Option<RemoteRepository>, OxenError> {
    let name = LocalRepository::dirname_from_url(url)?;
    get_by_name(&name)
}

pub fn get_by_name(name: &str) -> Result<Option<RemoteRepository>, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/repositories/{}", name);
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
        let status = res.status();
        if 404 == status {
            return Ok(None);
        }

        let body = res.text()?;
        log::debug!(
            "repositories::get_by_name status[{}] body:\n{}",
            status,
            body
        );

        let response: Result<RemoteRepositoryResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(j_res.repository)),
            Err(err) => {
                log::debug!("Err: {}", err);
                Err(OxenError::basic_str(&format!(
                    "api::repositories::get_by_name() Could not serialize repository [{}]",
                    name
                )))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::get_by_name() Request failed",
        ))
    }
}

pub fn create(repository: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let config = AuthConfig::default()?;
    let url = api::endpoint::url_from("/repositories");
    let root_commit = command::root_commit(repository)?;
    let params = json!({ "name": repository.name, "root_commit": root_commit });

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
        let response: Result<RemoteRepositoryResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(response.repository),
            Err(err) => {
                let err = format!(
                    "Could not create or find repository [{}]: {}\n{}",
                    repository.name, err, body
                );
                Err(OxenError::basic_str(&err))
            }
        }
    } else {
        let server = api::endpoint::server();
        let err = format!("Create repository could not connect to remote on. Make sure you have the correct server and that it is running: {}", server);
        Err(OxenError::basic_str(err))
    }
}

pub fn delete(repository: RemoteRepository) -> Result<StatusMessage, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/repositories/{}", repository.name);
    let url = api::endpoint::url_from(&uri);

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .delete(url)
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
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_create_remote_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|local_repo| {
            let repository = api::remote::repositories::create(&local_repo)?;
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
            let repository = api::remote::repositories::create(&local_repo)?;
            let url_repo = api::remote::repositories::get_by_name(&local_repo.name)?.unwrap();

            assert_eq!(repository.id, url_repo.id);

            // cleanup
            api::remote::repositories::delete(repository)?;

            Ok(())
        })
    }

    #[test]
    fn test_delete_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|local_repo| {
            let repository = api::remote::repositories::create(&local_repo)?;

            // delete
            api::remote::repositories::delete(repository)?;

            let result = api::remote::repositories::get_by_name(&local_repo.name);
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
            Ok(())
        })
    }
}
