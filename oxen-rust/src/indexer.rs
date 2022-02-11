use std::path::Path;
use std::path::PathBuf;
use std::collections::HashSet;
use std::fs;
use rayon::prelude::*;
use chrono::prelude::*;
use std::fs::File;
use indicatif::ProgressBar;
use std::fs::OpenOptions;
use std::io::Write;

use crate::config::Config;
use crate::util::file_util::FileUtil;
use crate::util::hasher;

pub struct Indexer {
    root_dir: PathBuf,
    hidden_dir: PathBuf,
    staging_file: PathBuf,
    commits_dir: PathBuf,
    synced_file: PathBuf,
    config: Config,
}

impl Indexer {
    pub fn new(dirname: &PathBuf) -> Indexer {
        let hidden_dir = PathBuf::from(dirname).join(Path::new(".indexer"));
        let staging_file = PathBuf::from(&hidden_dir).join(Path::new("staging"));
        let commits_dir = PathBuf::from(&hidden_dir).join(Path::new("commits"));
        let synced_file = PathBuf::from(&hidden_dir).join(Path::new("synced"));
        let config_file = PathBuf::from(&hidden_dir).join(Path::new("config.toml"));
        Indexer {
            root_dir: PathBuf::from(&hidden_dir.parent().unwrap()),
            hidden_dir: hidden_dir,
            staging_file: staging_file,
            commits_dir: commits_dir,
            synced_file: synced_file,
            config: Config::from(&config_file),
        }
    }

    fn is_initialized(&self) -> bool {
        self.hidden_dir.exists()
    }

    pub fn init(&self) {
        if self.is_initialized() {
            println!("Repository already exists for: {:?}", self.root_dir);
        } else {
            // Create hidden dir
            match fs::create_dir(&self.hidden_dir) {
                Ok(_) => {
                    // Create commits dir
                    match fs::create_dir(&self.commits_dir) {
                        Ok(_) => {
                            println!("Initialized repository in: {:?}", self.root_dir);
                        },
                        Err(err) => {
                            println!("Could not initialize repo: {}", err)
                        }
                    }
                },
                Err(err) => {
                    println!("Could not initialize repo: {}", err)
                }
            }
        }
    }
    
    fn list_image_files_from_dir(&self, dirname: &Path) -> Vec<PathBuf> {
        let img_ext: HashSet<String> = vec!["jpg", "png"].into_iter().map(String::from).collect();
        FileUtil::recursive_files_with_extensions(dirname, &img_ext)
    }
    
    pub fn add_files(&self, dir: &Path) {
        println!("Adding files in: {}", dir.display());
        let paths = self.list_image_files_from_dir(&dir);
        match File::create(&self.staging_file) {
            Ok(file) => {
                for path in paths.iter() {
                    match fs::canonicalize(&path) {
                        Ok(canonical) => {
                            match write!(&file, "{}\n", canonical.display()) {
                                Ok(_) => {},
                                Err(err) => {
                                    eprintln!("Could not add path {} err: {}", path.display(), err)
                                },
                            }
                        }
                        Err(_) => {/* Cannot cannonicalize... */}
                    }
                }
                println!("Added {} files", paths.len());
            },
            Err(err) => {
                eprintln!("Could not add files... {}", err)
            }
        }
    }

    fn list_paths_from_staged(&self) -> Vec<PathBuf> {
        FileUtil::read_lines(&self.staging_file).into_iter().map(PathBuf::from).collect()
    }

    pub fn commit_staged(&self) {
        let paths = self.list_paths_from_staged();
        let utc: DateTime<Utc> = Utc::now();
        // year_month_day_timestamp
        let commit_filename = format!("{}_{:02}_{:02}_{}", utc.year(), utc.month(), utc.day(), utc.timestamp());
        let commit_path = PathBuf::from(&self.commits_dir).join(Path::new(&commit_filename));
        match File::create(&commit_path) {
            Ok(file) => {
              
                let hash = hasher::hash_buffer(&commit_filename.as_bytes());
                match write!(&file, "{}\n", hash) {
                    Ok(_) => {},
                    Err(err) => {
                        eprintln!("Could not write hash {} err: {}", hash, err)
                    },
                }
                for path in paths.iter() {
                    match fs::canonicalize(&path) {
                        Ok(canonical) => {
                            match write!(&file, "{}\n", canonical.display()) {
                                Ok(_) => {},
                                Err(err) => {
                                    eprintln!("Could not add path {} err: {}", path.display(), err)
                                },
                            }
                        }
                        Err(_) => {/* Cannot cannonicalize... */}
                    }
                }
                println!("Commited {} files", paths.len());
            },
            Err(err) => {
                eprintln!("Could not add files... {}", err)
            }
        }
    }

    fn sync_commit(&self, commit: &String) {
        let file_name = PathBuf::from(&self.commits_dir).join(Path::new(commit));
        println!("Sync commit file: {:?}", file_name);
        let paths: Vec<PathBuf> = FileUtil::read_lines(file_name.as_path()).into_iter().map(PathBuf::from).collect();
        // len is usize and progressbar requires u64, I don't think we'll overflow...
        let size: u64 = unsafe { std::mem::transmute(paths.len()) };
        let bar = ProgressBar::new(size);
        paths.par_iter().for_each(|path| {
            
            // let hash = hasher::hash_file_contents(&path);

            // println!("Compute hash: {:?} => {:?}", path, hash);
            if let Ok(form) = reqwest::blocking::multipart::Form::new()
                .file("file", path)
            {
                let client = reqwest::blocking::Client::new();
                // TODO: login using config to get token / dataset
                let token = "SFMyNTY.g2gDbQAAACRjZTU1NTlkZC05YjgzLTQ1MGUtOTIwMi1iNzBkZTVkOWEzNThuBgBxmQHbfgFiAAFRgA.A5nHbhegqSiZ12QsJHcgN0ZiSPY0h2SrwqgZLMGAlzQ";
                let dataset = "6f8f2178-6723-4b74-acd4-8e70cd105287";
                let url = format!("http://{}/api/v1/repositories/{}/datasets/{}/entries", self.config.remote_ip, self.config.repository, dataset);
                if let Ok(res) = client.post(url)
                    .header(reqwest::header::AUTHORIZATION, token)
                    .multipart(form)
                    .send() {
                    if res.status() != reqwest::StatusCode::OK {
                        eprintln!("Error {:?}", res.text());
                    }
                }
            }

            bar.inc(1);
        });
        bar.finish();
    }

    pub fn sync(&self) {
        // list all commit files
        let commits: Vec<String> = FileUtil::list_files_in_dir(&self.commits_dir)
            .iter()
            .map(|path| { path.as_path().file_name()?.to_str() })
            .flatten()
            .map(|s| String::from(s))
            .collect();

        if let Ok(file) = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.synced_file)
        {
            let synced: HashSet<String> = FileUtil::read_lines_file(&file).into_iter().collect();
            let difference: Vec<_> = commits.iter().filter(|item| !synced.contains(*item)).collect();

            for commit in synced.iter() {
                println!("Already synced: [{}]", commit);
            }

            for commit in commits.iter() {
                println!("Commits: [{}]", commit);
            }

            for commit in difference.iter() {
                println!("Need to sync: {:?}", commit);
                self.sync_commit(commit);
            }
        }
    }
}
