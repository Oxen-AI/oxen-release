
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
  pub fn create(path: &Path) {
    FileUtil::write_to_path(&path, r#"
remote_ip = '127.0.0.1:4000'
repository_id = '6bac0c43-8bc8-4b14-ac9e-565c11dbd0ef'
email = 'gary@orlandomagic.com'
password = 'password'
    "#)
  }

  pub fn from(path: &Path) -> Config {
    let contents = FileUtil::read_from_path(path);
    toml::from_str(&contents).unwrap()
  }

  pub fn endpoint(&self) -> String {
    format!("http://{}/api/v1", self.remote_ip)
  }
}