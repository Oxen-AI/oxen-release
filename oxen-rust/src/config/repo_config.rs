use crate::model::user::User;
use crate::util::file_util::FileUtil;
use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize)]
pub struct OxenConfig {
    pub remote_ip: String,
}

#[derive(Deserialize)]
pub struct RepoConfig {
    pub remote_ip: String,
    pub repository_id: Option<String>,
    pub repository_url: Option<String>,
    pub token: Option<String>,
    pub user: Option<User>,
}

impl RepoConfig {
    pub fn create(path: &Path) {
        FileUtil::write_to_path(
            path,
            r#"
remote_ip = 'oxenai.com'
repository_id = 'f398e01c-e8dc-4e35-830d-df1eb97abcc4'
email = 'greg@oxen.ai'
password = 'password'
    "#,
        )
    }

    pub fn from(path: &Path) -> RepoConfig {
        let contents = FileUtil::read_from_path(path);
        toml::from_str(&contents).unwrap()
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}/api/v1", self.remote_ip)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::repo_config::RepoConfig;
    use std::path::Path;

    #[test]
    fn test_read_test() {
        let path = Path::new("config/oxen_config_test.toml");
        let config = RepoConfig::from(path);
        assert_eq!(config.endpoint(), "http://localhost:4000/api/v1");
    }
}
