
use std::path::Path;
use crate::util::file_util::FileUtil;
use serde_derive::Deserialize;

#[derive(Deserialize)]
pub struct Config {
  pub remote_ip: String,
  pub repository: String,
  pub email: String,
  pub password: String,
}

impl Config {
  pub fn from(path: &Path) -> Config {
    let contents = FileUtil::read_from_path(path);
    toml::from_str(&contents).unwrap()
  }
}