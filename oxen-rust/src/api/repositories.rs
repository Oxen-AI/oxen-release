use crate::config::oxen_config::OxenConfig;
use crate::error::OxenError;
use crate::model::repository::{Repository, RepositoryResponse};
use crate::model::status_message::StatusMessage;
use serde_json::json;

pub fn create(config: &OxenConfig, name: &str) -> Result<Repository, OxenError> {
    if let Some(user) = &config.user {
        let url = format!("{}/repositories", config.endpoint());
        let params = json!({
          "name": name,
          "is_public": true
        });

        let client = reqwest::blocking::Client::new();
        if let Ok(res) = client
            .post(url)
            .json(&params)
            .header(reqwest::header::AUTHORIZATION, &user.token)
            .send()
        {
            if let Ok(j_res) = res.json::<RepositoryResponse>() {
                Ok(j_res.repository)
            } else {
                Err(OxenError::from_str(
                    "api::repositories::create() Could not serialize repository",
                ))
            }
        } else {
            Err(OxenError::from_str(
                "api::repositories::create() Request failed",
            ))
        }
    } else {
        Err(OxenError::from_str(
            "api::repositories::create() User is not logged in.",
        ))
    }
}

pub fn delete(config: &OxenConfig, id: &str) -> Result<StatusMessage, OxenError> {
    if let Some(user) = &config.user {
        let url = format!("{}/repositories/{}", config.endpoint(), id);

        let client = reqwest::blocking::Client::new();
        if let Ok(res) = client
            .delete(url)
            .header(reqwest::header::AUTHORIZATION, &user.token)
            .send()
        {
            if let Ok(status) = res.json::<StatusMessage>() {
                Ok(status)
            } else {
                Err(OxenError::from_str(
                    "api::repositories::delete() Could not serialize status_message",
                ))
            }
        } else {
            Err(OxenError::from_str(
                "api::repositories::delete() Request failed",
            ))
        }
    } else {
        Err(OxenError::from_str(
            "api::repositories::delete() User is not logged in.",
        ))
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::config::oxen_config::OxenConfig;
    use crate::error::OxenError;

    use std::path::Path;

    #[test]
    fn test_create_repository() -> Result<(), OxenError> {
        let path = Path::new("config/oxen_config_logged_in.toml");
        let config = OxenConfig::from(path);
        let name: &str = "my repo";
        let repository = api::repositories::create(&config, name)?;
        assert_eq!(repository.name, name);
        // cleanup
        api::repositories::delete(&config, &repository.id)?;
        Ok(())
    }
}
