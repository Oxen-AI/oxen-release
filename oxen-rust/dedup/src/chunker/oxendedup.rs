use std::fs::File;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

use liboxen::model::LocalRepository;
use liboxen::repositories::{self, commits};
use serde::{Serialize, Deserialize};

use crate::chunker::get_chunker;

use super::Algorithm;

// const NAME: &str = "pack";
 
const VERSION_FILE_NAME: &str = "data";
pub struct PackCmd;

pub struct OxenStats {
    pub pack_time: f64,
    pub unpack_time: f64,
    pub pack_cpu_usage: f32,
    pub pack_memory_usage_bytes: u64,
    pub unpack_cpu_usage: f32,
    pub unpack_memory_usage_bytes: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OxenChunker {
    chunk_size: usize,
    chunk_algorithm: String,
    root_path: PathBuf,
}

impl OxenChunker {
    pub fn new(chunk_size: usize, chunk_algorithm: String, root_path: PathBuf) -> Result<Self, Error> {
        Ok(Self { chunk_size, chunk_algorithm, root_path })
    }

    fn name(&self) -> &'static str {
        "oxen-chunker"
    }
    fn version_dir(&self, hash: &str) -> PathBuf {
        let topdir = &hash[..2];
        let subdir = &hash[2..];
        self.root_path.join(topdir).join(subdir)
    }

    fn version_path(&self, hash: &str) -> PathBuf {
        self.version_dir(hash).join(VERSION_FILE_NAME)
    }


    pub fn pack(&self, algo: Algorithm, input_file: &Path, output_dir: &Path, n: u8 ) -> Result<PathBuf, Error> {
        
        std::fs::create_dir_all(output_dir)?;

        let input = File::open(input_file)?;
        let _metadata = input.metadata()?;
        let _paths = vec![input_file];

        let repo = LocalRepository::from_current_dir().map_err(|e| Error::new(ErrorKind::NotFound, format!("cannot load local repository {}", e) ))?;

        let chunker = get_chunker(&algo).map_err(|e| Error::new(ErrorKind::NotFound, format!("Chunker not found {}", e)))?;

        let commits = commits::list(&repo).map_err(|e| Error::new(ErrorKind::NotFound, format!("Commit not found {}", e)))?;

        let latest_n_commits = commits.iter().take(n as usize).collect::<Vec<_>>();

        for commit in latest_n_commits {

            let commit_hash = commit.hash().map_err(|e| Error::new(ErrorKind::NotFound, format!("Commit Hash not found {}", e)))?;
            let commit_output_dir = output_dir.join(&commit_hash.to_string());
            
            std::fs::create_dir_all(&commit_output_dir)?;

            let file_location = repositories::tree::get_file_by_path(&repo, commit, input_file);

            match file_location {
                Ok(Some(file)) => {
                    println!("Found file: {:?}", file);
                    // get file content
                    let hash = file.hash().to_string();
                    let file_path = self.version_path(&hash);
                    println!("File path: {:?}", file_path.display());
                    chunker.pack(&file_path, &commit_output_dir)?;
                }
                Ok(None) => {
                    println!("No file found for path: {:?}", &file_location);
                }
                Err(e) => {
                    eprintln!("Error getting file for path {:?}: {:?}", input_file, e);
                }
            }            
        }

        println!("Packing repository data with content-defined chunking...");

        Ok(output_dir.to_path_buf())
    }

    pub fn unpack(&self, input_dir: &Path, output_file: &Path) -> Result<PathBuf, std::io::Error> {

        println!("Unpacking repository data with content-defined chunking... {}", input_dir.display());
        Ok(output_file.to_path_buf())
    }
}