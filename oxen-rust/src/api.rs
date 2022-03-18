use crate::config::{RemoteConfig};
use crate::error::OxenError;
use crate::model::{User, UserResponse};
use reqwest::blocking::Client;
use serde_json::json;

pub mod datasets;
pub mod repositories;
pub mod entries;

pub fn login(config: &RemoteConfig, email: &str, password: &str) -> Result<User, OxenError> {
    let url = format!("{}/login", config.endpoint());
    let params = json!({
      "user": {
        "email": email,
        "password": password,
      }
    });

    if let Ok(res) = Client::new().post(&url).json(&params).send() {
        let status = res.status();
        if let Ok(user_res) = res.json::<UserResponse>() {
            Ok(user_res.user)
        } else {
            let err = format!(
                "login failed status_code[{}], check email and password",
                status
            );
            Err(OxenError::Basic(err))
        }
    } else {
        Err(OxenError::Basic(format!("login failed [{}]", &url)))
    }
}
