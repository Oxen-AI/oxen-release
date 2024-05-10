use crate::api;
use crate::config::RemoteConfig;
use crate::constants;
use crate::constants::DEFAULT_REMOTE_NAME;
use crate::constants::REPO_CONFIG_FILENAME;
use crate::constants::SHALLOW_FLAG;
use crate::core::index::EntryIndexer;
use crate::error::OxenError;
use crate::model::{Remote, RemoteBranch, RemoteRepository};
use crate::opts::CloneOpts;
use crate::opts::PullOpts;
use crate::util;
use crate::util::progress_bar::oxen_progress_bar;
use crate::util::progress_bar::ProgressBarType;
use crate::view::RepositoryView;

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalRepository {
    pub path: PathBuf,
    remote_name: Option<String>, // this is the current remote name
    pub remotes: Vec<Remote>,
}

impl LocalRepository {
    // Create a brand new repository with new ID
    pub fn new(path: &Path) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            path: path.to_path_buf(),
            remotes: vec![],
            remote_name: None,
        })
    }

    pub fn from_view(view: RepositoryView) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            path: std::env::current_dir()?.join(view.name),
            remotes: vec![],
            remote_name: None,
        })
    }

    pub fn from_remote(repo: RemoteRepository, path: &Path) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            path: path.to_owned(),
            remotes: vec![repo.remote],
            remote_name: Some(String::from(constants::DEFAULT_REMOTE_NAME)),
        })
    }

    pub fn from_dir(dir: &Path) -> Result<LocalRepository, OxenError> {
        let config_path = util::fs::config_filepath(dir);
        if !config_path.exists() {
            return Err(OxenError::local_repo_not_found());
        }
        let remote_cfg = RemoteConfig::from_file(&config_path)?;
        let repo = LocalRepository {
            path: dir.to_path_buf(),
            remotes: remote_cfg.remotes,
            remote_name: remote_cfg.remote_name,
        };
        Ok(repo)
    }

    pub fn dirname(&self) -> String {
        String::from(self.path.file_name().unwrap().to_str().unwrap())
    }

    pub fn save(&self, path: &Path) -> Result<(), OxenError> {
        let cfg = RemoteConfig {
            remote_name: self.remote_name.clone(),
            remotes: self.remotes.clone(),
        };
        let toml = toml::to_string(&cfg)?;
        util::fs::write_to_path(path, toml)?;
        Ok(())
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        let filename = util::fs::config_filepath(&self.path);
        self.save(&filename)?;
        Ok(())
    }

    pub async fn clone_remote(opts: &CloneOpts) -> Result<Option<LocalRepository>, OxenError> {
        log::debug!(
            "clone_remote {} -> {:?} -> shallow? {} -> all? {}",
            opts.url,
            opts.dst,
            opts.shallow,
            opts.all
        );

        let remote = Remote {
            name: String::from(DEFAULT_REMOTE_NAME),
            url: opts.url.to_owned(),
        };
        let remote_repo = api::remote::repositories::get_by_remote(&remote)
            .await?
            .ok_or_else(|| OxenError::remote_repo_not_found(&opts.url))?;
        let repo = LocalRepository::clone_repo(remote_repo, opts).await?;
        Ok(Some(repo))
    }

    pub fn set_remote(&mut self, name: &str, url: &str) -> Remote {
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
            self.remotes.push(remote.clone());
        }
        remote
    }

    pub fn delete_remote(&mut self, name: &str) {
        let mut new_remotes: Vec<Remote> = vec![];
        for i in 0..self.remotes.len() {
            if self.remotes[i].name != name {
                new_remotes.push(self.remotes[i].clone());
            }
        }
        self.remotes = new_remotes;
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
        log::debug!("Checking for remote {name} have {}", self.remotes.len());
        for remote in self.remotes.iter() {
            log::debug!("comparing: {name} -> {}", remote.name);
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

    async fn clone_repo(
        repo: RemoteRepository,
        opts: &CloneOpts,
    ) -> Result<LocalRepository, OxenError> {
        api::remote::repositories::pre_clone(&repo).await?;

        // if directory already exists -> return Err
        let repo_path = &opts.dst;
        if repo_path.exists() {
            let err = format!("Directory already exists: {}", repo.name);
            return Err(OxenError::basic_str(err));
        }

        // if directory does not exist, create it
        std::fs::create_dir_all(repo_path)?;

        // if create successful, create .oxen directory
        let oxen_hidden_path = util::fs::oxen_hidden_dir(repo_path);
        std::fs::create_dir(&oxen_hidden_path)?;

        // save LocalRepository in .oxen directory
        let repo_config_file = oxen_hidden_path.join(Path::new(REPO_CONFIG_FILENAME));
        let mut local_repo = LocalRepository::from_remote(repo.clone(), repo_path)?;
        repo_path.clone_into(&mut local_repo.path);
        local_repo.set_remote(DEFAULT_REMOTE_NAME, &repo.remote.url);

        // Save remote config in .oxen/config.toml
        let remote_cfg = RemoteConfig {
            remote_name: Some(DEFAULT_REMOTE_NAME.to_string()),
            remotes: vec![repo.remote.clone()],
        };

        let toml = toml::to_string(&remote_cfg)?;
        util::fs::write_to_path(&repo_config_file, &toml)?;

        // Pull all commit objects, but not entries
        let rb = RemoteBranch::from_branch(&opts.branch);
        let indexer = EntryIndexer::new(&local_repo)?;
        local_repo
            .maybe_pull_entries(&repo, &indexer, &rb, opts)
            .await?;

        if opts.all {
            log::debug!("pulling all entries");
            let remote_branches = api::remote::branches::list(&repo).await?;
            if remote_branches.len() > 1 {
                println!(
                    "🐂 Pre-fetching {} additional remote branches...",
                    remote_branches.len() - 1
                );
            }

            let n_other_branches: u64 = if remote_branches.len() > 1 {
                (remote_branches.len() - 1) as u64
            } else {
                0
            };

            let bar = oxen_progress_bar(n_other_branches as u64, ProgressBarType::Counter);

            for branch in remote_branches {
                // We've already pulled the target branch in full
                if branch.name == rb.branch {
                    continue;
                }

                let remote_branch = RemoteBranch::from_branch(&branch.name);
                indexer
                    .pull_most_recent_commit_object(&repo, &remote_branch, false)
                    .await?;
                bar.inc(1);
            }
            bar.finish_and_clear();
        }

        println!("\n🎉 cloned {} to {}/\n", repo.remote.url, repo.name);
        api::remote::repositories::post_clone(&repo).await?;

        Ok(local_repo)
    }

    async fn maybe_pull_entries(
        &self,
        repo: &RemoteRepository,
        indexer: &EntryIndexer,
        rb: &RemoteBranch,
        opts: &CloneOpts,
    ) -> Result<(), OxenError> {
        // Shallow means we will not pull the actual data until a user tells us to
        if opts.shallow {
            indexer
                .pull_most_recent_commit_object(repo, rb, true)
                .await?;
            self.write_is_shallow(true)?;
        } else {
            // Pull all entries
            indexer
                .pull(
                    rb,
                    PullOpts {
                        should_pull_all: opts.all,
                        should_update_head: true,
                    },
                )
                .await?;
        }
        Ok(())
    }

    pub fn write_is_shallow(&self, shallow: bool) -> Result<(), OxenError> {
        let shallow_flag_path = util::fs::oxen_hidden_dir(&self.path).join(SHALLOW_FLAG);
        log::debug!("Write is shallow [{shallow}] to path: {shallow_flag_path:?}");
        if shallow {
            util::fs::write_to_path(&shallow_flag_path, "true")?;
        } else if shallow_flag_path.exists() {
            util::fs::remove_file(&shallow_flag_path)?;
        }
        Ok(())
    }

    pub fn is_shallow_clone(&self) -> bool {
        let shallow_flag_path = util::fs::oxen_hidden_dir(&self.path).join(SHALLOW_FLAG);
        shallow_flag_path.exists()
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::model::{LocalRepository, RepoNew};
    use crate::opts::CloneOpts;
    use crate::test;
    use crate::util;

    #[test]
    fn test_get_dirname_from_url() -> Result<(), OxenError> {
        let url = "http://0.0.0.0:3000/repositories/OxenData";
        let repo = RepoNew::from_url(url)?;
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
    fn test_delete_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|mut local_repo| {
            let origin_url = "http://0.0.0.0:3000/repositories/OxenData";
            let origin_name = "origin";

            let other_url = "http://0.0.0.0:4000/repositories/OxenData";
            let other_name = "other";
            local_repo.set_remote(origin_name, origin_url);
            local_repo.set_remote(other_name, other_url);

            // Remove and make sure we cannot get again
            local_repo.delete_remote(origin_name);
            let remote = local_repo.get_remote(origin_name);
            assert!(remote.is_none());

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_clone_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            // Create remote repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            log::debug!("created the remote repo");

            test::run_empty_dir_test_async(|dir| async move {
                let opts = CloneOpts::new(remote_repo.remote.url.to_owned(), dir.join("new_repo"));

                log::debug!("about to clone the remote");
                let local_repo = LocalRepository::clone_remote(&opts).await?.unwrap();
                log::debug!("succeeded");
                let cfg_fname = ".oxen/config.toml".to_string();
                let config_path = local_repo.path.join(&cfg_fname);
                assert!(config_path.exists());

                let repository = LocalRepository::from_dir(&local_repo.path);
                assert!(repository.is_ok());

                let repository = repository.unwrap();
                let status = command::status(&repository)?;
                assert!(status.is_clean());

                // Cleanup
                api::remote::repositories::delete(&remote_repo).await?;

                Ok(dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_move_local_repo_path_valid() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            // Create remote repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            test::run_empty_dir_test_async(|dir| async move {
                let opts = CloneOpts::new(remote_repo.remote.url.to_owned(), dir.join("new_repo"));
                let local_repo = LocalRepository::clone_remote(&opts).await?.unwrap();

                api::remote::repositories::delete(&remote_repo).await?;

                command::status(&local_repo)?;

                let new_path = dir.join("new_path");

                util::fs::rename(&local_repo.path, &new_path)?;

                let new_repo = LocalRepository::from_dir(&new_path)?;
                command::status(&new_repo)?;
                assert_eq!(new_repo.path, new_path);

                Ok(dir)
            })
            .await
        })
        .await
    }
}
