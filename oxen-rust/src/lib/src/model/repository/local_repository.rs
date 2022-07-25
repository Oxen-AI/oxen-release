use crate::api;
use crate::constants;
use crate::error::OxenError;
use crate::index::Indexer;
use crate::model::{Commit, Remote, RemoteBranch, RemoteRepository};
use crate::util;
use crate::view::RepositoryView;

use http::Uri;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// For creating a remote repo we need the repo name
/// and we need the root commit so that we do not generate a new one on creation on the server
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RepositoryNew {
    pub namespace: String,
    pub name: String,
    pub root_commit: Option<Commit>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalRepository {
    pub namespace: String,
    pub name: String,
    pub path: PathBuf,
    remote_name: Option<String>, // this is the current remote name
    pub remotes: Vec<Remote>,
}

impl LocalRepository {
    // Create a brand new repository with new ID
    pub fn new(path: &Path) -> Result<LocalRepository, OxenError> {
        // we're assuming the path is valid...
        let name = path.file_name().unwrap().to_str().unwrap();
        Ok(LocalRepository {
            namespace: String::from(constants::DEFAULT_NAMESPACE),
            name: String::from(name),
            path: path.to_path_buf(),
            remotes: vec![],
            remote_name: None,
        })
    }

    pub fn from_view(view: RepositoryView) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            namespace: view.namespace.clone(),
            name: view.name.clone(),
            path: std::env::current_dir()?.join(view.name),
            remotes: vec![],
            remote_name: None,
        })
    }

    pub fn from_remote(repo: RemoteRepository, path: &Path) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            namespace: repo.namespace.to_owned(),
            name: repo.name.to_owned(),
            path: path.to_owned(),
            remotes: vec![Remote {
                name: String::from(constants::DEFAULT_REMOTE_NAME),
                url: repo.url,
            }],
            remote_name: Some(String::from(constants::DEFAULT_REMOTE_NAME)),
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
            return Err(OxenError::local_repo_not_found());
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

    pub fn clone_remote(url: &str, dst: &Path) -> Result<Option<LocalRepository>, OxenError> {
        log::debug!("clone_remote {} -> {:?}", url, dst);
        let repo = LocalRepository::repo_new_from_url(url)?;
        match api::remote::repositories::get_by_namespace_and_name(&repo.namespace, &repo.name) {
            Ok(Some(remote_repo)) => Ok(Some(LocalRepository::clone_repo(remote_repo, dst)?)),
            Ok(None) => Ok(None),
            Err(_) => {
                let err = format!("Could not clone remote {} not found", url);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn set_remote(&mut self, name: &str, url: &str) {
        self.remote_name = Some(String::from(name));
        let remote = Remote {
            name: String::from(name),
            url: String::from(url),
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

    pub fn get_remote(&self, name: &str) -> Option<Remote> {
        for remote in self.remotes.iter() {
            if remote.name == name {
                return Some(remote.clone());
            }
        }
        None
    }

    pub fn remote(&self) -> Option<Remote> {
        if let Some(name) = &self.remote_name {
            self.get_remote(name)
        } else {
            None
        }
    }

    fn clone_repo(repo: RemoteRepository, dst: &Path) -> Result<LocalRepository, OxenError> {
        // get last part of URL for directory name
        let url = String::from(&repo.url);
        let repo_new = LocalRepository::repo_new_from_url(&url)?;
        // if directory already exists -> return Err
        let repo_path = dst.join(&repo_new.name);
        if repo_path.exists() {
            let err = format!("Directory already exists: {}", repo_new.name);
            return Err(OxenError::basic_str(&err));
        }

        // if directory does not exist, create it
        std::fs::create_dir_all(&repo_path)?;

        // if create successful, create .oxen directory
        let oxen_hidden_path = util::fs::oxen_hidden_dir(&repo_path);
        std::fs::create_dir(&oxen_hidden_path)?;

        // save Repository in .oxen directory
        let repo_config_file = oxen_hidden_path.join(Path::new("config.toml"));
        let mut local_repo = LocalRepository::from_remote(repo, &repo_path)?;
        local_repo.path = repo_path;
        local_repo.set_remote("origin", &url);

        let toml = toml::to_string(&local_repo)?;
        util::fs::write_to_path(&repo_config_file, &toml);

        // Pull all commit objects, but not entries
        let indexer = Indexer::new(&local_repo)?;
        indexer.pull_all_commit_objects(&RemoteBranch::default())?;

        println!(
            "🐂 cloned {} to {}\n\ncd {}\noxen pull origin main",
            url, repo_new.name, repo_new.name
        );

        Ok(local_repo)
    }

    pub fn repo_new_from_url(url: &str) -> Result<RepositoryNew, OxenError> {
        let uri = url.parse::<Uri>()?;
        let mut split_path: Vec<&str> = uri.path().split('/').collect();

        if split_path.len() < 3 {
            return Err(OxenError::basic_str("Invalid repo url"));
        }

        let name = split_path.pop().unwrap();
        let namespace = split_path.pop().unwrap();
        Ok(RepositoryNew {
            name: String::from(name),
            namespace: String::from(namespace),
            root_commit: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::config::{AuthConfig, HTTPConfig};
    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::test;

    use std::path::Path;

    #[test]
    fn test_get_dirname_from_url() -> Result<(), OxenError> {
        let url = "http://0.0.0.0:3000/repositories/OxenData";
        let repo = LocalRepository::repo_new_from_url(url)?;
        assert_eq!(repo.name, "OxenData");
        assert_eq!(repo.namespace, "repositories");
        Ok(())
    }

    #[test]
    fn test_get_set_has_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|mut local_repo| {
            let url = "http://0.0.0.0:3000/repositories/OxenData";
            let remote_name = "origin";
            local_repo.set_remote(remote_name, url);
            let remote = local_repo.get_remote(remote_name).unwrap();
            assert_eq!(remote.name, remote_name);
            assert_eq!(remote.url, url);

            Ok(())
        })
    }

    #[test]
    fn test_clone_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|local_repo| {
            let config = AuthConfig::default()?;
            let remote_repo = api::remote::repositories::create(&local_repo, config.host())?;

            test::run_empty_dir_test(|dir| {
                let local_repo = LocalRepository::clone_remote(&remote_repo.url, dir)?.unwrap();

                let cfg_fname = ".oxen/config.toml".to_string();
                let config_path = local_repo.path.join(&cfg_fname);
                assert!(config_path.exists());
                assert_eq!(local_repo.name, local_repo.name);
                assert_eq!(local_repo.namespace, local_repo.namespace);

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
        assert_eq!(repo.namespace, "0af558cc-a57c-4197-a442-50eb889e9495");
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
        assert_eq!(repo.namespace, orig_repo.namespace);
        assert_eq!(repo.name, orig_repo.name);

        std::fs::remove_file(final_path)?;

        Ok(())
    }
}
