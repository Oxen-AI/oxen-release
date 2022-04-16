use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use rayon::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use crate::api;
use crate::cli::committer::HISTORY_DIR;
use crate::cli::Committer;
use crate::config::{AuthConfig, RepoConfig};
use crate::error::OxenError;
use crate::model::{
    CommitHead, CommitMsg, CommitMsgResponse, Dataset,
    Repository, RepositoryResponse, RepositoryHeadResponse,
};
use crate::util::{hasher, FileUtil};

pub const OXEN_HIDDEN_DIR: &str = ".oxen";
pub const REPO_CONFIG_FILE: &str = "config.toml";

pub struct Indexer {
    pub root_dir: PathBuf,
    pub hidden_dir: PathBuf,
    config_file: PathBuf,
    auth_config: AuthConfig,
    repo_config: Option<RepoConfig>,
}

impl Indexer {
    pub fn new(root_dir: &Path) -> Indexer {
        let hidden_dir = PathBuf::from(&root_dir).join(Path::new(OXEN_HIDDEN_DIR));
        let config_file = PathBuf::from(&hidden_dir).join(Path::new(REPO_CONFIG_FILE));
        let auth_config = AuthConfig::default().unwrap();

        // Load repo config if exists
        let repo_config: Option<RepoConfig> = match config_file.exists() {
            true => Some(RepoConfig::new(&config_file)),
            false => None,
        };
        Indexer {
            root_dir: root_dir.to_path_buf(),
            hidden_dir,
            config_file,
            auth_config,
            repo_config,
        }
    }

    pub fn repo_exists(dirname: &Path) -> bool {
        let hidden_dir = PathBuf::from(dirname).join(Path::new(OXEN_HIDDEN_DIR));
        hidden_dir.exists()
    }

    pub fn is_initialized(&self) -> bool {
        Indexer::repo_exists(&self.root_dir)
    }

    pub fn init(&self) -> Result<(), OxenError> {
        if self.is_initialized() {
            println!("Repository already exists for: {:?}", self.root_dir);
            return Ok(());
        }

        // Get name from current directory name
        if let Some(name) = self.root_dir.file_name() {
            self.init_with_name(name.to_str().unwrap())
        } else {
            let err = format!(
                "Could not find parent directories name: {:?}",
                self.root_dir
            );
            Err(OxenError::basic_str(&err))
        }
    }

    pub fn init_with_name(&self, name: &str) -> Result<(), OxenError> {
        if self.is_initialized() {
            println!("Repository already exists for: {:?}", self.root_dir);
            return Ok(());
        }

        println!("Initializing 🐂 repository with name: {}", name);

        // Make hidden .oxen dir
        std::fs::create_dir(&self.hidden_dir)?;

        let auth_cfg = AuthConfig::default()?;
        let repository = Repository {
            id: format!("{}", uuid::Uuid::new_v4()),
            name: String::from(name),
            url: String::from(""), // no remote to start
        };
        let repo_config = RepoConfig::from(&auth_cfg, &repository);
        let repo_config_file = self.hidden_dir.join(REPO_CONFIG_FILE);
        repo_config.save(&repo_config_file)?;
        println!("Repository initialized at {:?}", self.hidden_dir);
        Ok(())
    }

    pub fn set_remote(&mut self, url: &str) -> Result<(), OxenError> {
        let repository = api::repositories::get_by_url(&self.auth_config, url)?;
        self.repo_config = Some(RepoConfig::from(&self.auth_config, &repository));
        self.repo_config
            .as_ref()
            .unwrap()
            .save(Path::new(&self.config_file))?;
        println!("Remote set: {}", url);
        Ok(())
    }

    fn push_entries(&self, committer: &Committer, commit: &CommitMsg) -> Result<(), OxenError> {
        let paths = committer.list_unsynced_files_for_commit(&commit.id)?;

        println!("🐂 push {} files", paths.len());

        // len is usize and progressbar requires u64, I don't think we'll overflow...
        let size: u64 = unsafe { std::mem::transmute(paths.len()) };
        let bar = ProgressBar::new(size);

        paths.par_iter().for_each(|path| {
            if let Ok(hash) = hasher::hash_file_contents(path) {
                match FileUtil::path_relative_to_dir(path, &self.root_dir) {
                    Ok(path) => {
                        match api::entries::create(self.repo_config.as_ref().unwrap(), &path, &hash) {
                            Ok(_entry) => {
                                // TODO: save the hash in DB so that we can quickly resume sync
                                println!("Created entry! Save hash {:?} => {}", path, hash);
                            }
                            Err(err) => {
                                eprintln!("Error uploading {:?} {}", path, err)
                            }
                        }
                    },
                    Err(_) => {
                        eprintln!("Could not get relative path...");
                    }
                }
            }

            bar.inc(1);
        });
        bar.finish();

        Ok(())
    }

    pub fn push(&self, committer: &Committer) -> Result<(), OxenError> {
        self.create_or_get_repo()?;
        match committer.get_head_commit() {
            Ok(Some(commit)) => {
                // maybe_push() will recursively check commits head against remote head
                // and sync ones that have not been synced
                let remote_head = self.get_remote_head()?;
                self.maybe_push(committer, &remote_head, &commit.id, 0)?;
                Ok(())
            }
            Ok(None) => Err(OxenError::basic_str("No commits to push.")),
            Err(err) => {
                let msg = format!("Err: {}", err);
                Err(OxenError::basic_str(&msg))
            }
        }
    }

    pub fn create_or_get_repo(&self) -> Result<(), OxenError> {
        // TODO move into another api class, and better error handling...just cranking this out
        let name = &self.repo_config.as_ref().unwrap().repository.name;
        let url = "http://0.0.0.0:3000/repositories".to_string();
        let params = json!({ "name": name });

        let client = reqwest::blocking::Client::new();
        if let Ok(res) = client.post(url).json(&params).send() {
            let status = res.status();
            let body = res.text()?;
            let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(_) => Ok(()),
                Err(_) => Err(OxenError::basic_str(&format!(
                    "status_code[{}] \n\n{}",
                    status, body
                ))),
            }
            
        } else {
            Err(OxenError::basic_str(
                "create_or_get_repo() Could not create repo",
            ))
        }
    }

    pub fn maybe_push(
        &self,
        committer: &Committer,
        remote_head: &Option<CommitHead>,
        commit_id: &str,
        depth: usize,
    ) -> Result<(), OxenError> {
        if let Some(head) = remote_head {
            if commit_id == head.commit_id {
                if depth == 0 {
                    println!("No commits to push, remote is synced.");
                } else {
                    println!("Done.");
                }
                return Ok(());
            }
        }

        if let Some(commit) = committer.get_commit_by_id(commit_id)? {
            if let Some(parent_id) = &commit.parent_id {
                self.maybe_push(committer, remote_head, parent_id, depth + 1)?;
            } else {
                println!("No parent commit... {} -> {}", commit.id, commit.message);
            }
            // Unroll stack to post in reverse order
            self.post_commit_to_server(&commit)?;
            self.push_entries(&committer, &commit)?;
        } else {
            eprintln!("Err: could not find commit: {}", commit_id);
        }

        Ok(())
    }

    pub fn get_remote_head(&self) -> Result<Option<CommitHead>, OxenError> {
        // TODO move into another api class, need to better delineate what we call these
        // also is this remote the one in the config? I think so, need to draw out a diagram
        let name = &self.repo_config.as_ref().unwrap().repository.name;
        let url = format!("http://0.0.0.0:3000/repositories/{}", name);
        let client = reqwest::blocking::Client::new();
        if let Ok(res) = client.get(url).send() {
            // TODO: handle if remote repo does not exist...
            // Do we create it then push for now? Or add separate command to create?
            // I think we create and push, and worry about authorized keys etc later
            let body = res.text()?;
            let response: Result<RepositoryHeadResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(j_res) => Ok(j_res.head),
                Err(err) => Err(OxenError::basic_str(&format!(
                    "get_remote_head() Could not serialize response [{}]\n{}",
                    err,
                    body
                ))),
            }
        } else {
            Err(OxenError::basic_str("get_remote_head() Request failed"))
        }
    }

    pub fn post_commit_to_server(&self, commit: &CommitMsg) -> Result<(), OxenError> {
        // zip up the rocksdb in history dir, and post to server
        let commit_dir = self.hidden_dir.join(HISTORY_DIR).join(&commit.id);
        let path_to_compress = format!("history/{}", commit.id);

        println!("Compressing commit {}", commit.id);
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);

        tar.append_dir_all(path_to_compress, commit_dir)?;
        tar.finish()?;
        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        self.post_tarball_to_server(&buffer, commit)?;

        Ok(())
    }

    fn post_tarball_to_server(
        &self,
        buffer: &[u8],
        commit: &CommitMsg,
    ) -> Result<(), OxenError> {
        println!("Syncing database {}", commit.id);
        println!("{:?}", commit);

        let name = &self.repo_config.as_ref().unwrap().repository.name;
        let client = reqwest::blocking::Client::new();
        let url = format!(
            "http://0.0.0.0:3000/repositories/{}/commits?{}",
            name,
            commit.to_uri_encoded()
        );
        if let Ok(res) = client
            .post(url)
            .body(reqwest::blocking::Body::from(buffer.to_owned()))
            .send()
        {
            let status = res.status();
            let body = res.text()?;
            let response: Result<CommitMsgResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(_) => Ok(()),
                Err(_) => Err(OxenError::basic_str(&format!(
                    "Error serializing CommitMsgResponse: status_code[{}] \n\n{}",
                    status, body
                ))),
            }
        } else {
            Err(OxenError::basic_str(
                "post_tarball_to_server error sending data from file",
            ))
        }
    }

    pub fn list_datasets(&self) -> Result<Vec<Dataset>, OxenError> {
        api::datasets::list(self.repo_config.as_ref().unwrap())
    }

    pub fn pull(&self) -> Result<(), OxenError> {
        let datasets = self.list_datasets()?;
        // Compute the total entries from the first pages, and make appropriate directories
        let mut total = 0;
        let mut dataset_pages: HashMap<&Dataset, usize> = HashMap::new();
        for dataset in datasets.iter() {
            let entry_page =
                api::entries::list_page(self.repo_config.as_ref().unwrap(), 1)?;
            let path = Path::new(&dataset.name);
            if !path.exists() {
                std::fs::create_dir(&path)?;
            }
            dataset_pages.insert(dataset, entry_page.total_pages);
            total += entry_page.total_entries;
        }

        println!("🐂 pulling {} entries", total);
        let size: u64 = unsafe { std::mem::transmute(total) };
        let bar = ProgressBar::new(size);
        dataset_pages.par_iter().for_each(|dataset_pages| {
            let (dataset, num_pages) = dataset_pages;
            match self.pull_dataset(dataset, num_pages, &bar) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("Error pulling dataset: {}", err)
                }
            }
        });
        bar.finish();
        Ok(())
    }

    fn pull_dataset(
        &self,
        dataset: &Dataset,
        num_pages: &usize,
        progress: &ProgressBar,
    ) -> Result<(), OxenError> {
        // println!("Pulling {} pages from dataset {}", num_pages, dataset.name);
        // Pages start at index 1, ie: 0 and 1 are the same
        (1..*num_pages + 1).into_par_iter().for_each(|page| {
            match api::entries::list_page(self.repo_config.as_ref().unwrap(), page) {
                Ok(entry_page) => {
                    // println!("Got page {}/{}, from {} with {} entries", page, num_pages, dataset.name, entry_page.page_size);
                    for entry in entry_page.entries {
                        match self.download_url(dataset, &entry) {
                            Ok(_) => {}
                            Err(error) => {
                                println!("Err downloading file: {}", error)
                            }
                        }
                        progress.inc(1);
                    }
                }
                Err(error) => {
                    println!("Err listing page [{}]: {}", page, error)
                }
            }
        });
        // println!("Done pulling {} pages from dataset {}", num_pages, dataset.name);
        Ok(())
    }

    fn download_url(
        &self,
        dataset: &Dataset,
        entry: &crate::model::Entry,
    ) -> Result<(), OxenError> {
        let path = Path::new(&dataset.name);
        let fname = path.join(&entry.filename);
        // println!("Downloading file {:?}", &fname);
        if !fname.exists() {
            let mut response = reqwest::blocking::get(&entry.url)?;
            let mut dest = { File::create(fname)? };
            response.copy_to(&mut dest)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::indexer::OXEN_HIDDEN_DIR;
    use crate::cli::Indexer;
    use crate::error::OxenError;
    use crate::model::Repository;
    use crate::test;

    const BASE_DIR: &str = "data/test/runs";

    #[test]
    fn test_1_indexer_init() -> Result<(), OxenError> {
        test::setup_env();

        let repo_dir = test::create_repo_dir(BASE_DIR)?;
        let indexer = Indexer::new(&repo_dir);
        indexer.init()?;

        let repository = Repository::from(&repo_dir);
        let hidden_dir = repo_dir.join(OXEN_HIDDEN_DIR);
        assert!(hidden_dir.exists());
        assert!(!repository.id.is_empty());
        let name = repo_dir.file_name().unwrap().to_str().unwrap();
        assert_eq!(repository.name, name);
        assert_eq!(repository.url, format!("http://0.0.0.0:2000/{}", name));

        // cleanup
        std::fs::remove_dir_all(repo_dir)?;

        Ok(())
    }

    #[test]
    fn test_1_indexer_init_with_name() -> Result<(), OxenError> {
        test::setup_env();

        let repo_dir = test::create_repo_dir(BASE_DIR)?;
        let indexer = Indexer::new(&repo_dir);

        let name = "gschoeni/Repo-Name";
        indexer.init_with_name(name)?;

        let repository = Repository::from(&repo_dir);
        let hidden_dir = repo_dir.join(OXEN_HIDDEN_DIR);
        assert!(hidden_dir.exists());
        assert!(!repository.id.is_empty());
        assert_eq!(repository.name, name);
        assert_eq!(repository.url, format!("http://0.0.0.0:2000/{}", name));

        // cleanup
        std::fs::remove_dir_all(repo_dir)?;

        Ok(())
    }
}
