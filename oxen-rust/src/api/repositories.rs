use crate::config::AuthConfig;
use crate::error::OxenError;
use crate::model::repository::{Repository, RepositoryResponse};
use crate::model::status_message::StatusMessage;
use serde_json::json;
use urlencoding::encode;

pub fn create(config: &AuthConfig, name: &str) -> Result<Repository, OxenError> {
    let url = format!("{}/repositories", config.endpoint());
    let params = json!({
        "name": name,
        "is_public": true
    });

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .post(url)
        .json(&params)
        .header(reqwest::header::AUTHORIZATION, &config.user.token)
        .send()
    {
        match res.json::<RepositoryResponse>() {
            Ok(j_res) => Ok(j_res.repository),
            Err(err) => Err(OxenError::basic_str(&format!(
                "api::repositories::create() Could not serialize repository [{}]",
                err
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::create() Request failed",
        ))
    }
}

pub fn get_by_url(config: &AuthConfig, url: &str) -> Result<Repository, OxenError> {
    let params = json!({
        "url": url
    });

    let encoded_url = encode(url);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(format!("{}/repositories/get_by_url?url={}", config.endpoint(), encoded_url))
        .json(&params)
        .header(reqwest::header::AUTHORIZATION, &config.user.token)
        .send()
    {
        match res.json::<RepositoryResponse>() {
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

pub fn delete(config: &AuthConfig, id: &str) -> Result<StatusMessage, OxenError> {
    let url = format!("{}/repositories/{}", config.endpoint(), id);

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .delete(url)
        .header(reqwest::header::AUTHORIZATION, &config.user.token)
        .send()
    {
        if let Ok(status) = res.json::<StatusMessage>() {
            Ok(status)
        } else {
            Err(OxenError::basic_str(
                "api::repositories::delete() Could not serialize status_message",
            ))
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
    use crate::config::AuthConfig;
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_create_repository() -> Result<(), OxenError> {
        let path = test::auth_cfg_file();
        let config = AuthConfig::from(path);
        let name: &str = "test_create_repository";

        let repository = api::repositories::create(&config, name)?;
        assert_eq!(repository.name, name);

        // cleanup
        api::repositories::delete(&config, &repository.id)?;
        Ok(())
    }

    #[test]
    fn test_get_by_url() -> Result<(), OxenError> {
        let path = test::auth_cfg_file();
        let config = AuthConfig::from(path);
        let name: &str = "test_get_by_url";

        let repository = api::repositories::create(&config, name)?;
        let url_repo = api::repositories::get_by_url(&config, &repository.url)?;

        assert_eq!(repository.id, url_repo.id);

        // cleanup
        api::repositories::delete(&config, &repository.id)?;
        Ok(())
    }
}
