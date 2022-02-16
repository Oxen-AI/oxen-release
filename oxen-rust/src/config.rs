
use std::path::Path;
use crate::util::file_util::FileUtil;
use serde::Deserialize;
use crate::model::user::User;

#[derive(Deserialize)]
pub struct Config {
  pub remote_ip: String,
  pub repository_id: String,
  pub email: String,
  pub password: String,
  pub user: Option<User>
}

impl Config {
  pub fn from(path: &Path) -> Config {
    let contents = FileUtil::read_from_path(path);
    toml::from_str(&contents).unwrap()
  }

  pub fn endpoint(&self) -> String {
    format!("http://{}/api/v1", self.remote_ip)
  }
}