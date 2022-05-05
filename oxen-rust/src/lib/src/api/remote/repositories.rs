use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::{RemoteRepositoryResponse, StatusMessage};
use serde_json::json;
use urlencoding::encode;


pub fn create_or_get(name: &str) -> Result<RemoteRepository, OxenError> {
    let config = AuthConfig::default()?;
    let url = api::endpoint::url_from("/repositories");
    let params = json!({ "name": name });

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = 
        client
        .post(url)
        .json(&params)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", config.auth_token()))
        .send()
    {
        let body = res.text()?;
        let response: Result<RemoteRepositoryResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(response.repository),
            Err(err) => {
                let err = format!("Could not create or find repository: {}", err);
                Err(OxenError::basic_str(&err))
            },
        }
    } else {
        Err(OxenError::basic_str(
            "create_or_get_repo() Could not create repo",
        ))
    }
}

pub fn get_by_url(url: &str) -> Result<RemoteRepository, OxenError> {
    let config = AuthConfig::default()?;
    let encoded_url = encode(url);
    let uri = format!("/repositories/get_by_url?url={}", encoded_url);
    let url = api::endpoint::url_from(&uri);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(reqwest::header::AUTHORIZATION, config.auth_token())
        .send()
    {
        match res.json::<RemoteRepositoryResponse>() {
            Ok(j_res) => Ok(j_res.repository),
            Err(err) => Err(OxenError::basic_str(&format!(
                "api::repositories::get_by_url() Could not serialize repository [{}]",
                err
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::create() Request failed",
        ))
    }
}

pub fn delete(repository: RemoteRepository) -> Result<StatusMessage, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/repositories/{}", repository.id);
    let url = api::endpoint::url_from(&uri);

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .delete(url)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", config.auth_token()))
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
    fn test_create_repository() -> Result<(), OxenError> {
        test::init_test_env();

        let name: &str = "test_create_repository";

        let repository = api::remote::repositories::create_or_get(name)?;
        assert_eq!(repository.name, name);

        // cleanup
        api::remote::repositories::delete(repository)?;
        Ok(())
    }

    #[test]
    fn test_get_by_url() -> Result<(), OxenError> {
        let name: &str = "test_get_by_url";

        let repository = api::remote::repositories::create_or_get(name)?;
        let url_repo = api::remote::repositories::get_by_url( &repository.url)?;

        assert_eq!(repository.id, url_repo.id);

        // cleanup
        api::remote::repositories::delete(repository)?;
        Ok(())
    }
}
