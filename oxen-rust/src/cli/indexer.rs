use indicatif::ProgressBar;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
// use std::sync::Arc;
// use std::sync::atomic::{AtomicBool, Ordering};

use crate::api;
use crate::cli::Committer;
use crate::config::{AuthConfig, RepoConfig};
use crate::error::OxenError;
use crate::model::{CommitMsg, Dataset};
use crate::util::hasher;

pub const OXEN_HIDDEN_DIR: &str = ".oxen";

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
        let config_file = PathBuf::from(&hidden_dir).join(Path::new("config.toml"));
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
        Indexer::repo_exists(&self.hidden_dir)
    }

    pub fn init(&self) -> Result<(), OxenError> {
        if self.is_initialized() {
            println!("Repository already exists for: {:?}", self.root_dir);
            Ok(())
        } else {
            std::fs::create_dir(&self.hidden_dir)?;
            println!("Repository initialized at {:?}", self.hidden_dir);
            Ok(())
        }
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

    fn push_commit(&self, committer: &Committer, commit: &CommitMsg) -> Result<(), OxenError> {
        let paths = committer.list_unsynced_files_for_commit(&commit.id)?;

        println!("🐂 push {} files", paths.len());

        // first get directories and create dataset if not exists
        // TODO: recursive
        let mut datasets_to_files: HashMap<String, Vec<PathBuf>> = HashMap::new();
        for path in paths.iter() {
            if let Some(parent) = path.parent() {
                if parent != Path::new("") {
                    let key = String::from(parent.to_str().unwrap());
                    let value = path.to_path_buf();

                    match datasets_to_files.entry(key) {
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(vec![value]);
                        }
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            e.get_mut().push(value);
                        }
                    }
                }
            }
        }

        // Have to pass dataset objects to create entries, so lets create
        // the datasets if they dont exist
        let config = self.repo_config.as_ref().unwrap();
        let mut names_to_datasets: HashMap<String, Dataset> = HashMap::new();
        for (name, _files) in datasets_to_files.iter() {
            let dataset = match api::datasets::get_by_name(self.repo_config.as_ref().unwrap(), name)
            {
                Ok(dataset) => dataset,
                Err(_) => api::datasets::create(config, name)?,
            };
            names_to_datasets.insert(name.clone(), dataset);
        }

        // len is usize and progressbar requires u64, I don't think we'll overflow...
        let size: u64 = unsafe { std::mem::transmute(paths.len()) };
        let bar = ProgressBar::new(size);

        for (name, files) in datasets_to_files.iter() {
            files.par_iter().for_each(|path| {
                let dataset = &names_to_datasets[name];
                if let Ok(hash) = hasher::hash_file_contents(path) {
                    // Only upload file if it's hash doesn't already exist
                    match api::entries::create(self.repo_config.as_ref().unwrap(), dataset, path) {
                        Ok(_entry) => {
                            println!("Created entry! Save hash {:?} => {}", path, hash);
                        }
                        Err(err) => {
                            eprintln!("Error uploading {:?} {}", path, err)
                        }
                    }
                }

                bar.inc(1);
            });
        }
        bar.finish();

        Ok(())
    }

    pub fn push(&self, committer: &Committer) -> Result<(), OxenError> {
        // list all commit messages
        let commits: Vec<CommitMsg> = committer.list_commits()?;

        // TODO: We should keep track of local and remote refs
        // https://git-scm.com/book/en/v2/Git-Internals-Git-References#:~:text=Remotes,in%20the%20refs%2Fremotes%20directory.

        // for now just push one
        for commit in commits.iter() {
            println!("Pushing commit: {:?}", commit);
            self.push_commit(committer, commit)?;
            break;
        }

        Ok(())
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
                api::entries::list_page(self.repo_config.as_ref().unwrap(), dataset, 1)?;
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
            match api::entries::list_page(self.repo_config.as_ref().unwrap(), dataset, page) {
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
