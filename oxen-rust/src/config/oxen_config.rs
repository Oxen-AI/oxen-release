
use std::path::Path;
use crate::util::file_util::FileUtil;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct OxenConfig {
  pub remote_ip: String
}

impl OxenConfig {
  pub fn from(path: &Path) -> OxenConfig {
    let contents = FileUtil::read_from_path(path);
    toml::from_str(&contents).unwrap()
  }

  pub fn endpoint(&self) -> String {
    format!("http://{}/api/v1", self.remote_ip)
  }
}

#[cfg(test)]
mod tests {
  use crate::config::oxen_config::OxenConfig;
  use std::path::Path;

  #[test]
  fn test_read_test() {
    let path = Path::new("config/test.toml");
    let config = OxenConfig::from(path);
    assert_eq!(config.endpoint(), "http://localhost:4000/api/v1");
  }
}

