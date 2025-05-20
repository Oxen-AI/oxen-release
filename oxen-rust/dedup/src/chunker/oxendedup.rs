use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write, Error};
use std::path::{Path, PathBuf};
use std::time::Instant;

use async_trait::async_trait;
use cargo::ops::print;
use clap::{Arg, Command};
use flate2::write::GzEncoder;
use flate2::Compression;
use liboxen::model::LocalRepository;
use liboxen::repositories::{self, commits};
use serde::{Serialize, Deserialize};

use crate::chunker::{Chunker, map_bincode_error};

pub const NAME: &str = "pack";
pub const VERSION_FILE_NAME: &str = "data";
pub struct PackCmd;

const METADATA_FILE_NAME: &str = "metadata.bin";
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

    /// Get the full path for a version file
    fn version_path(&self, hash: &str) -> PathBuf {
        self.version_dir(hash).join(VERSION_FILE_NAME)
    }

    pub fn pack(&self, input_file: &Path, output_dir: &Path, n: u8 ) -> Result<PathBuf, Error> {
        // Create the output directory if it doesn't exist
        std::fs::create_dir_all(output_dir)?;

        // Open the input file
        let input = File::open(input_file)?;
        let metadata = input.metadata()?;
        let paths = vec![input_file]; // Example paths to pack

        // Get repository
        let repo = LocalRepository::from_current_dir().map_err(|e| std::io::Error::new(std::io::ErrorKind::AlreadyExists, "Some error"))?;

        let latest_commit = commits::latest_commit(&repo).map_err(|e| Error::new(std::io::ErrorKind::AlreadyExists, "Some error"))?;;


        let commits = commits::list(&repo).map_err(|e| Error::new(std::io::ErrorKind::AlreadyExists, "Some error"))?;

        for commit in commits.iter() {
            println!("Commit: {:?}", commit);
        }



        for path in paths.iter() {
            let node_result = repositories::tree::get_node_by_path(&repo, &latest_commit, path);
    
            match node_result {
                Ok(Some(node)) => {
                    println!("Found node: {:?}", node);
                }
                Ok(None) => {
                    println!("No node found for path: {:?}", path);
                }
                Err(e) => {
                    eprintln!("Error getting node for path {:?}: {:?}", path, e);
                }
            }

            // get file location
            let file_location = repositories::tree::get_file_by_path(&repo, &latest_commit, path);

            match file_location {
                Ok(Some(file)) => {
                    println!("Found file: {:?}", file);
                    // get file content
                    let mut file_content = Vec::new();
                    let hash = file.hash().to_string();
                    let file_path = self.version_path(&hash);
                    println!("File path: {:?}", file_path.display());
                    let mut file = File::open(file_path)?;
                    file.read_to_end(&mut file_content)?;
                }
                Ok(None) => {
                    println!("No file found for path: {:?}", path);
                }
                Err(e) => {
                    eprintln!("Error getting file for path {:?}: {:?}", path, e);
                }
            }

            let version_store = repo.version_store().map_err(|e| Error::new(std::io::ErrorKind::AlreadyExists, "Some error"))?;

        }
                
        println!("Packing repository data with content-defined chunking...");

        Ok(output_dir.to_path_buf())
    }

    pub fn unpack(&self, input_dir: &Path, output_file: &Path) -> Result<PathBuf, std::io::Error> {


        Ok(output_file.to_path_buf())
    }
}