use crate::api;
use crate::constants::{DEFAULT_REMOTE_NAME, NO_REPO_MSG};
use crate::error::OxenError;
use crate::model::{Remote, RemoteRepository};
use crate::util;
use crate::view::RepositoryView;

use http::Uri;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Deserialize, Debug, Clone)]
pub struct RepositoryNew {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalRepository {
    pub id: String,
    pub name: String,
    pub path: PathBuf,
    remote_name: Option<String>, // this is the current remote name
    remotes: Vec<Remote>,
}

impl LocalRepository {
    // Create a brand new repository with new ID
    pub fn new(path: &Path) -> Result<LocalRepository, OxenError> {
        // we're assuming the path is valid...
        let name = path.file_name().unwrap().to_str().unwrap();
        Ok(LocalRepository {
            // generate new uuid locally
            id: format!("{}", uuid::Uuid::new_v4()),
            name: String::from(name),
            path: path.to_path_buf(),
            remotes: vec![],
            remote_name: None,
        })
    }

    pub fn from_view(view: RepositoryView) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            // generate new uuid locally
            id: view.id.clone(),
            name: view.name.clone(),
            path: std::env::current_dir()?.join(view.name),
            remotes: vec![],
            remote_name: None,
        })
    }

    pub fn from_remote(view: RemoteRepository) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            // generate new uuid locally
            id: view.id.to_owned(),
            name: view.name.to_owned(),
            path: std::env::current_dir()?.join(view.name),
            remotes: vec![Remote {
                name: String::from(DEFAULT_REMOTE_NAME),
                value: view.url,
            }],
            remote_name: Some(String::from(DEFAULT_REMOTE_NAME)),
        })
    }

    pub fn from_cfg(path: &Path) -> Result<LocalRepository, OxenError> {
        let contents = util::fs::read_from_path(path)?;
        let repo: LocalRepository = toml::from_str(&contents)?;
        Ok(repo)
    }

    pub fn from_dir(dir: &Path) -> Result<LocalRepository, OxenError> {
        let config_path = util::fs::config_filepath(dir);
        if !config_path.exists() {
            return Err(OxenError::basic_str(NO_REPO_MSG));
        }
        let repo = LocalRepository::from_cfg(&config_path)?;
        Ok(repo)
    }

    pub fn save(&self, path: &Path) -> Result<(), OxenError> {
        let toml = toml::to_string(&self)?;
        util::fs::write_to_path(path, &toml);
        Ok(())
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        let filename = util::fs::config_filepath(&self.path);
        self.save(&filename)?;
        Ok(())
    }

    pub fn clone_remote(url: &str, dst: &Path) -> Result<LocalRepository, OxenError> {
        log::debug!("clone_remote {} -> {:?}", url, dst);
        let name = LocalRepository::dirname_from_url(url)?;
        match api::remote::repositories::get_by_name(&name) {
            Ok(remote_repo) => LocalRepository::clone_repo(remote_repo, dst),
            Err(_) => {
                let err = format!("Could not clone remote {} not found", url);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn set_remote(&mut self, name: &str, value: &str) {
        self.remote_name = Some(String::from(name));
        let remote = Remote {
            name: String::from(name),
            value: String::from(value),
        };
        if self.has_remote(name) {
            // find remote by name and set
            for i in 0..self.remotes.len() {
                if self.remotes[i].name == name {
                    self.remotes[i] = remote.clone()
                }
            }
        } else {
            // we don't have the key, just push
            self.remotes.push(remote);
        }
    }

    pub fn has_remote(&self, name: &str) -> bool {
        for remote in self.remotes.iter() {
            if remote.name == name {
                return true;
            }
        }
        false
    }

    pub fn remote(&self) -> Option<Remote> {
        if let Some(name) = &self.remote_name {
            for remote in self.remotes.iter() {
                if &remote.name == name {
                    return Some(remote.clone());
                }
            }
            None
        } else {
            None
        }
    }

    fn clone_repo(repo: RemoteRepository, dst: &Path) -> Result<LocalRepository, OxenError> {
        // get last part of URL for directory name
        let url = repo.url.to_owned();
        let dir_name = LocalRepository::dirname_from_url(&url)?;
        // if directory already exists -> return Err
        let repo_path = dst.join(&dir_name);
        if repo_path.exists() {
            let err = format!("Directory already exists: {}", dir_name);
            return Err(OxenError::basic_str(&err));
        }

        // if directory does not exist, create it
        std::fs::create_dir_all(&repo_path)?;

        // if create successful, create .oxen directory
        let oxen_hidden_path = util::fs::oxen_hidden_dir(&repo_path);
        std::fs::create_dir(&oxen_hidden_path)?;

        // save Repository in .oxen directory
        let repo_config_file = oxen_hidden_path.join(Path::new("config.toml"));
        let mut local_repo = LocalRepository::from_remote(repo)?;
        local_repo.path = repo_path;
        local_repo.set_remote("origin", &url);

        let toml = toml::to_string(&local_repo)?;
        util::fs::write_to_path(&repo_config_file, &toml);

        println!(
            "🐂 cloned {} to {}\n\ncd {}\noxen pull",
            url, dir_name, dir_name
        );

        Ok(local_repo)
    }

    pub fn dirname_from_url(url: &str) -> Result<String, OxenError> {
        let uri = url.parse::<Uri>()?;
        if let Some(dirname) = uri.path().split('/').last() {
            Ok(String::from(dirname))
        } else {
            Err(OxenError::basic_str(""))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::test;

    use std::path::Path;

    #[test]
    fn test_get_dirname_from_url() -> Result<(), OxenError> {
        let url = "http://0.0.0.0:3000/repositories/OxenData";
        let dirname = LocalRepository::dirname_from_url(url)?;
        assert_eq!(dirname, "OxenData");
        Ok(())
    }

    #[test]
    fn test_clone_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let remote_repo = api::remote::repositories::create_or_get(&repo)?;

            test::run_empty_dir_test(|dir| {
                let local_repo = LocalRepository::clone_remote(&remote_repo.url, &dir)?;

                let cfg_fname = ".oxen/config.toml".to_string();
                let config_path = local_repo.path.join(&cfg_fname);
                assert!(config_path.exists());
                assert_eq!(local_repo.name, local_repo.name);
                assert_eq!(local_repo.id, local_repo.id);

                let repository = LocalRepository::from_cfg(&config_path);
                assert!(repository.is_ok());

                let repository = repository.unwrap();
                let status = command::status(&repository)?;
                assert!(status.is_clean());

                // Cleanup
                api::remote::repositories::delete(remote_repo)?;

                Ok(())
            })
        })
    }

    #[test]
    fn test_read_cfg() -> Result<(), OxenError> {
        let path = test::repo_cfg_file();
        let repo = LocalRepository::from_cfg(path)?;
        assert_eq!(repo.id, "0af558cc-a57c-4197-a442-50eb889e9495");
        assert_eq!(repo.name, "Mini-Dogs-Vs-Cats");
        assert_eq!(repo.path, Path::new("/tmp/Mini-Dogs-Vs-Cats"));
        Ok(())
    }

    #[test]
    fn test_local_repository_save() -> Result<(), OxenError> {
        let final_path = Path::new("/tmp/repo_config.toml");
        let orig_repo = LocalRepository::from_cfg(test::repo_cfg_file())?;

        orig_repo.save(final_path)?;

        let repo = LocalRepository::from_cfg(final_path)?;
        assert_eq!(repo.id, orig_repo.id);
        assert_eq!(repo.name, orig_repo.name);

        std::fs::remove_file(final_path)?;

        Ok(())
    }
}
