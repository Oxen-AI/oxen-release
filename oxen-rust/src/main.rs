use clap::{App, Arg};
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use indicatif::ProgressBar;
use std::fs::File;
use sha2::{Sha256, Digest};
use std::io::{BufReader, Read, Write};
use jwalk::{WalkDir};
use std::io::prelude::*;
use std::fs::OpenOptions;
use std::env;
use rayon::prelude::*;
use chrono::prelude::*;
use std::collections::HashSet;


fn hash_buffer(buffer: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(&buffer);
    format!("{:X}", hasher.finalize())
}

fn hash_file_contents(path: &Path) -> Result<String, String> {
    match File::open(path) {
        Ok(file) => {
            let mut reader = BufReader::new(file);
            let mut buffer = Vec::new();
            match reader.read_to_end(&mut buffer) {
                Ok(_) => {
                    // read hash digest and consume hasher
                    let result = hash_buffer(&buffer);
                    return Ok(result)
                },
                Err(_) => {
                    eprintln!("Could not read file to end {:?}", path);
                    Err(String::from("Could not read file to end"))
                }
            }
        },
        Err(_) => {
            eprintln!("Could not open file {:?}", path);
            Err(String::from("Could not open file"))
        }
    }
}

struct FileUtil {

}

impl FileUtil {
    fn read_lines_file(file: &File) -> Vec<String> {
        let mut lines: Vec<String> = Vec::new();
        let reader = BufReader::new(file);
        for line in reader.lines() {
            match line {
                Ok(valid) => {
                    let trimmed = valid.trim();
                    if !trimmed.is_empty() {
                        lines.push(String::from(trimmed));
                    }
                },
                Err(_) => {/* Couldnt read line */}
            }
        }
        lines
    }

    fn read_lines(path: &Path) -> Vec<String> {
        let mut lines: Vec<String> = Vec::new();
        match File::open(&path) {
            Ok(file) => {
                lines = FileUtil::read_lines_file(&file)
            },
            Err(_) => {
                eprintln!("Could not open staging file {}", path.display())
            }
        }
        lines
    }

    fn list_files_in_dir(dir: &Path) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = Vec::new();
        match fs::read_dir(dir) {
            Ok(paths) => {
                for path in paths {
                    match path {
                        Ok(val) => {
                            if fs::metadata(val.path()).unwrap().is_file() {
                                files.push(val.path());
                            }
                        }
                        Err(_) => {}
                    }
                }
            },
            Err(err) => {
                eprintln!("FileUtil::list_files_in_dir Could not find dir: {} err: {}", dir.display(), err)
            }
        }
        
        files
    }

    fn recursive_files_with_extensions(dir: &Path, exts: &HashSet<String>) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = Vec::new();
        for entry in WalkDir::new(dir) {
            match entry {
                Ok(val) => {
                    match val.path().extension() {
                        Some(extension) => {
                            match extension.to_str() {
                                Some(ext) => {
                                    if exts.contains(ext) {
                                        files.push(val.path());
                                    }
                                },
                                None => {
                                    eprintln!("Could not convert ext to string... {}", val.path().display())
                                }
                            }
    
                        },
                        None => {
                            // Ignore files with no extension
                        }
                    }
                },
                Err(err) => eprintln!("Could not iterate over dir... {}", err),
            }
        }
        files
    }
}

struct Indexer {
    root_dir: PathBuf,
    hidden_dir: PathBuf,
    staging_file: PathBuf,
    commits_dir: PathBuf,
    synced_file: PathBuf,
}

impl Indexer {
    fn new(dirname: &PathBuf) -> Indexer {
        let hidden_dir = PathBuf::from(dirname).join(Path::new(".indexer"));
        let staging_file = PathBuf::from(&hidden_dir).join(Path::new("staging"));
        let commits_dir = PathBuf::from(&hidden_dir).join(Path::new("commits"));
        let synced_file = PathBuf::from(&hidden_dir).join(Path::new("synced"));
        Indexer {
            root_dir: PathBuf::from(&hidden_dir.parent().unwrap()),
            hidden_dir: hidden_dir,
            staging_file: staging_file,
            commits_dir: commits_dir,
            synced_file: synced_file,
        }
    }

    fn is_initialized(&self) -> bool {
        self.hidden_dir.exists()
    }

    fn init(&self) {
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
    
    fn add_files(&self, dir: &Path) {
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

    fn commit_staged(&self) {
        let paths = self.list_paths_from_staged();
        let utc: DateTime<Utc> = Utc::now();
        // year_month_day_timestamp
        let commit_filename = format!("{}_{:02}_{:02}_{}", utc.year(), utc.month(), utc.day(), utc.timestamp());
        let commit_path = PathBuf::from(&self.commits_dir).join(Path::new(&commit_filename));
        match File::create(&commit_path) {
            Ok(file) => {
                let hash = hash_buffer(&commit_filename.as_bytes());
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
            
            let hash = hash_file_contents(&path);

            // println!("Compute hash: {:?} => {:?}", path, hash);
            if let Ok(form) = reqwest::blocking::multipart::Form::new()
                .file("file", path)
            {
                let client = reqwest::blocking::Client::new();
                
                if let Ok(res) = client.post("http://localhost:4000/api/v1/repositories/035fedfa-911d-464f-a928-c0abc367287c/datasets/6f8f2178-6723-4b74-acd4-8e70cd105287/entries")
                    .header(reqwest::header::AUTHORIZATION, "SFMyNTY.g2gDbQAAACRjZTU1NTlkZC05YjgzLTQ1MGUtOTIwMi1iNzBkZTVkOWEzNThuBgBxmQHbfgFiAAFRgA.A5nHbhegqSiZ12QsJHcgN0ZiSPY0h2SrwqgZLMGAlzQ")
                    .multipart(form)
                    .send() {
                    if res.status() != reqwest::StatusCode::OK {
                        eprintln!("Error {:?}", res.text());
                    }
                }
            }

            bar.inc(1);
        })
    }

    fn sync(&self) {
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

fn main() {
    let args = App::new("indexer")
        .version("0.0.1")
        .about("Bulk uploads data to our server")
        .arg(
            Arg::with_name("command")
                .help("The command to run (add, commit, push)")
                .takes_value(true)
                .required(true)
        )
        .arg(
            Arg::with_name("directory")
                .help("The directory to find the data in")
                .takes_value(true)
        )
        .get_matches();

    let command = args.value_of("command").unwrap();
    
    

    match command {
        "init" => {
            let dirname = String::from(args.value_of("directory").unwrap());
            let directory = PathBuf::from(&dirname);
            let indexer = Indexer::new(&directory);
            indexer.init()
        },
        "add" => {
            let current_dir = env::current_dir().unwrap();
            let indexer = Indexer::new(&current_dir);
            let dirname = String::from(args.value_of("directory").unwrap());
            let directory = PathBuf::from(&dirname);
            indexer.add_files(&directory)
        },
        "commit" => {
            let current_dir = env::current_dir().unwrap();
            let indexer = Indexer::new(&current_dir);
            indexer.commit_staged()
        },
        "sync" => {
            let current_dir = env::current_dir().unwrap();
            let indexer = Indexer::new(&current_dir);
            indexer.sync();
        },
        "encode" => {
            let value = String::from(args.value_of("directory").unwrap());
            let encoded = hash_buffer(value.as_bytes());
            println!("{} => {}", value, encoded)
        },
        _ => {
            println!("Unknown command: {}", command)
        },
    }
}
