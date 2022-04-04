use chrono::prelude::*;
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
// use std::sync::Arc;
// use std::sync::atomic::{AtomicBool, Ordering};

use crate::api;
use crate::config::{AuthConfig, RepoConfig};
use crate::error::OxenError;
use crate::model::{Dataset, Entry};
use crate::util::file_util::FileUtil;
use crate::util::hasher;

pub const OXEN_HIDDEN_DIR: &str = ".oxen";

pub struct Indexer {
    pub root_dir: PathBuf,
    pub hidden_dir: PathBuf,
    staging_file: PathBuf,
    config_file: PathBuf,
    commits_dir: PathBuf,
    synced_file: PathBuf,
    auth_config: AuthConfig,
    repo_config: Option<RepoConfig>,
}

impl Indexer {
    pub fn new(dirname: &Path) -> Indexer {
        let hidden_dir = PathBuf::from(dirname).join(Path::new(OXEN_HIDDEN_DIR));
        let staging_file = PathBuf::from(&hidden_dir).join(Path::new("staging"));
        let commits_dir = PathBuf::from(&hidden_dir).join(Path::new("commits"));
        let synced_file = PathBuf::from(&hidden_dir).join(Path::new("synced"));
        let config_file = PathBuf::from(&hidden_dir).join(Path::new("config.toml"));
        let auth_config = AuthConfig::default().unwrap();

        // Load repo config if exists
        let repo_config: Option<RepoConfig> = match config_file.exists() {
            true => Some(RepoConfig::new(&config_file)),
            false => None,
        };
        Indexer {
            root_dir: PathBuf::from(&hidden_dir.parent().unwrap()),
            hidden_dir,
            staging_file,
            config_file,
            commits_dir,
            synced_file,
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

    fn list_image_files_from_dir(&self, dirname: &Path) -> Vec<PathBuf> {
        let img_ext: HashSet<String> = vec!["jpg", "png"].into_iter().map(String::from).collect();
        FileUtil::recursive_files_with_extensions(dirname, &img_ext)
    }

    fn list_text_files_from_dir(&self, dirname: &Path) -> Vec<PathBuf> {
        let img_ext: HashSet<String> = vec!["txt"].into_iter().map(String::from).collect();
        FileUtil::recursive_files_with_extensions(dirname, &img_ext)
    }

    pub fn add_files(&self, dir: &Path) {
        println!("Adding files in: {}", dir.display());
        let mut paths: Vec<PathBuf> = vec![];
        let mut img_paths = self.list_image_files_from_dir(dir);
        let mut txt_paths = self.list_text_files_from_dir(dir);
        paths.append(&mut img_paths);
        paths.append(&mut txt_paths);

        match File::create(&self.staging_file) {
            Ok(file) => {
                for path in paths.iter() {
                    if let Ok(canonical) = fs::canonicalize(&path) {
                        match writeln!(&file, "{}", canonical.display()) {
                            Ok(_) => {}
                            Err(err) => {
                                eprintln!("Could not add path {} err: {}", path.display(), err)
                            }
                        }
                    }
                }
                println!("Added {} files", paths.len());
            }
            Err(err) => {
                eprintln!("add_files Could not add files... {}", err)
            }
        }
    }

    fn list_paths_from_staged(&self) -> Vec<PathBuf> {
        FileUtil::read_lines(&self.staging_file)
            .into_iter()
            .map(PathBuf::from)
            .collect()
    }

    pub fn commit_staged(&self) -> Result<(), OxenError> {
        let paths = self.list_paths_from_staged();
        let utc: DateTime<Utc> = Utc::now();
        // year_month_day_timestamp
        let commit_filename = format!(
            "{}_{:02}_{:02}_{}",
            utc.year(),
            utc.month(),
            utc.day(),
            utc.timestamp()
        );

        if !&self.commits_dir.exists() {
            fs::create_dir(&self.commits_dir)?;
        }

        let commit_path = PathBuf::from(&self.commits_dir).join(Path::new(&commit_filename));
        match File::create(&commit_path) {
            Ok(file) => {
                let hash = hasher::hash_buffer(commit_filename.as_bytes());
                match writeln!(&file, "{}", hash) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("Could not write hash {} err: {}", hash, err)
                    }
                }
                for path in paths.iter() {
                    if let Ok(canonical) = fs::canonicalize(&path) {
                        match writeln!(&file, "{}", canonical.display()) {
                            Ok(_) => {}
                            Err(err) => {
                                eprintln!("Could not add path {} err: {}", path.display(), err)
                            }
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("commit_staged Could not add files... {}", err)
            }
        }

        // Remove staged file for now
        match fs::remove_file(&self.staging_file) {
            Ok(_file) => {
                println!("🐂 commited {} files", paths.len());
                Ok(())
            }
            Err(err) => Err(OxenError::basic_str(&format!(
                "Could not remove staged file: {}",
                err
            ))),
        }
    }

    fn p_push_commit(&self, commit: &str, dataset: &Dataset) -> Result<(), OxenError> {
        let file_name = PathBuf::from(&self.commits_dir).join(Path::new(commit));
        // println!("Sync commit file: {:?}", file_name);
        let path = file_name.as_path();
        let paths: Vec<PathBuf> = FileUtil::read_lines(path)
            .into_iter()
            .map(PathBuf::from)
            .filter(|path| path.is_file())
            .collect();

        // let processed: Vec<AtomicBool> = Vec::with_capacity(paths.len());

        // if let Ok(mut s) = signal_hook::iterator::Signals::new(signal_hook::consts::TERM_SIGNALS) {
        //     std::thread::spawn(move || {
        //         for _ in s.forever() {
        //             let num_remaining = processed.iter().filter(|x| *x ).count();
        //             println!("got a signal, num_remaining {:?}", num_remaining);
        //             std::process::exit(1);
        //         }
        //     });
        // } else {
        //     println!("error with signals num_remaining {:?}", processed);
        // }

        // IF WE SPLIT INTO N THREADS, THEN INSIDE EACH THREAD CHECK FOR SIG, THEN MAYBE WE CAN GET ALL THE ONES IN PROGRESS

        println!("🐂 push {} files", paths.len());
        // len is usize and progressbar requires u64, I don't think we'll overflow...
        let size: u64 = unsafe { std::mem::transmute(paths.len()) };
        let bar = ProgressBar::new(size);
        paths.par_iter().for_each(|path| {
            // if let Ok(hash) = hasher::hash_file_contents(path) {
            //     if api::entries::from_hash(&self.config, &hash).is_ok() {
            //         // println!("Already have entry {:?}", entry);
            //     } else {
            // Only upload file if it's hash doesn't already exist
            match api::entries::create(self.repo_config.as_ref().unwrap(), dataset, path) {
                Ok(_entry) => {}
                Err(err) => {
                    eprintln!("Error uploading {:?} {}", path, err)
                }
            }
            //     }
            // }

            bar.inc(1);
        });
        bar.finish();

        // Remove committed file for now (TODO: mark as synced or something)
        match fs::remove_file(path) {
            Ok(_file) => {
                println!("Pushed {} files", paths.len());
                Ok(())
            }
            Err(err) => Err(OxenError::basic_str(&format!(
                "Could not remove commit file: {}",
                err
            ))),
        }
    }

    fn push_commit(&self, commit: &str, dataset: &Dataset) -> Result<(), OxenError> {
        self.p_push_commit(commit, dataset)
    }

    fn dataset_from_name(&self, name: &str) -> Result<Dataset, OxenError> {
        let datasets = api::datasets::list(self.repo_config.as_ref().unwrap())?;
        let result = datasets.iter().find(|&x| x.name == name);

        match result {
            Some(dataset) => Ok(dataset.clone()),
            None => Err(OxenError::basic_str(&format!(
                "Couldn't find dataset \"{}\"",
                name
            ))),
        }
    }

    pub fn push(&self, dataset_name: &str) -> Result<(), OxenError> {
        let dataset = self.dataset_from_name(dataset_name)?;

        // list all commit files
        let commits: Vec<String> = FileUtil::list_files_in_dir(&self.commits_dir)
            .iter()
            .filter_map(|path| path.as_path().file_name()?.to_str())
            .map(String::from)
            .collect();

        if let Ok(file) = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.synced_file)
        {
            let synced: HashSet<String> = FileUtil::read_lines_file(&file).into_iter().collect();
            let difference: Vec<_> = commits
                .iter()
                .filter(|item| !synced.contains(*item))
                .collect();

            // for commit in synced.iter() {
            //     println!("Already synced: [{}]", commit);
            // }

            // for commit in commits.iter() {
            //     println!("Commits: [{}]", commit);
            // }

            for commit in difference.iter() {
                // println!("Need to sync: {:?}", commit);
                self.push_commit(commit, &dataset)?
            }
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

    fn download_url(&self, dataset: &Dataset, entry: &Entry) -> Result<(), OxenError> {
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

    pub fn status(&self) -> Result<(), OxenError> {
        if !self.staging_file.exists() {
            return Err(OxenError::basic_str("No files staged."));
        }

        println!("🐂 status\n");
        let mut num_imgs = 0;
        let mut num_audio = 0;
        let mut num_video = 0;
        let mut num_text = 0;
        let lines = FileUtil::read_lines(&self.staging_file);
        for line in lines {
            let path = PathBuf::from(line);
            if FileUtil::is_image(&path) {
                num_imgs += 1;
            }
            if FileUtil::is_audio(&path) {
                num_audio += 1;
            }
            if FileUtil::is_video(&path) {
                num_video += 1;
            }
            if FileUtil::is_text(&path) {
                num_text += 1;
            }
        }

        println!("Staged files:");
        if num_imgs > 0 {
            println!("{} image files", num_imgs)
        }
        if num_audio > 0 {
            println!("{} audio files", num_audio)
        }
        if num_video > 0 {
            println!("{} video files", num_video)
        }
        if num_text > 0 {
            println!("{} text files", num_text)
        }

        Ok(())
    }

    pub fn commit(&self, _status: &str) -> Result<(), OxenError> {
        if !self.staging_file.exists() {
            return Err(OxenError::basic_str("No files staged."));
        }

        self.commit_staged()
    }

    pub fn create_dataset_if_not_exists(&self, name: &str) -> Result<Dataset, OxenError> {
        if !self.commits_dir.exists() {
            return Err(OxenError::basic_str(
                "No data committed yet. Run `oxen commit -m 'your message'`.",
            ));
        }

        let config = self.repo_config.as_ref().unwrap();
        api::datasets::create(config, name)
    }
}
