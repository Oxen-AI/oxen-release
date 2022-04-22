use crate::config::RemoteConfig;
use crate::error::OxenError;
use crate::model::{User, UserResponse};
use reqwest::blocking::Client;
use serde_json::json;

pub mod endpoint;
pub mod entries;
pub mod local;
pub mod repositories;

pub fn login(config: &RemoteConfig, email: &str, password: &str) -> Result<User, OxenError> {
    let url = format!("{}/login", config.endpoint());
    let params = json!({
      "user": {
        "email": email,
        "password": password,
      }
    });

    if let Ok(res) = Client::new().post(&url).json(&params).send() {
        let body = res.text()?;
        let user: UserResponse = serde_json::from_str(&body)?;
        Ok(user.user)
    } else {
        Err(OxenError::basic_str(
            "login failed, invalid email or password",
        ))
    }
}
