use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::{RemoteRepository, LocalRepository};
use crate::view::{RemoteRepositoryResponse, StatusMessage};
use serde_json::json;

pub fn get_by_url(url: &str) -> Result<RemoteRepository, OxenError> {
    let name = LocalRepository::dirname_from_url(url)?;
    get_by_name(&name)
}

pub fn get_by_name(name: &str) -> Result<RemoteRepository, OxenError> {
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
        let body = res.text()?;

        log::debug!(
            "repositories::get_by_name status[{}] body:\n{}",
            status,
            body
        );

        let response: Result<RemoteRepositoryResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(j_res.repository),
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

pub fn create_or_get(repository: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let config = AuthConfig::default()?;
    let url = api::endpoint::url_from("/repositories");
    let params = json!({ "name": repository.name });

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
        Err(OxenError::basic_str(
            "create_or_get() Could not create repo",
        ))
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
            let repository = api::remote::repositories::create_or_get(&local_repo)?;
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
            let repository = api::remote::repositories::create_or_get(&local_repo)?;
            let url_repo = api::remote::repositories::get_by_name(&local_repo.name)?;

            assert_eq!(repository.id, url_repo.id);

            // cleanup
            api::remote::repositories::delete(repository)?;

            Ok(())
        })
    }

    #[test]
    fn test_delete_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|local_repo| {
            let repository = api::remote::repositories::create_or_get(&local_repo)?;

            // delete
            api::remote::repositories::delete(repository)?;

            let result = api::remote::repositories::get_by_name(&local_repo.name);
            assert!(result.is_err());
            Ok(())
        })
    }
}
