pub mod fixedsize;
use std::{collections::HashMap, io, path::{Path, PathBuf}};
use clap::ValueEnum;

pub use crate::chunker::fixedsize::FixedSizeChunker;

pub trait Chunker {

    fn pack(&self, input_dir: &Path, output_dir: &Path) -> Result<PathBuf, io::Error>;

    fn unpack(&self, input_dir: &Path, output_dir: &Path) -> Result<PathBuf, io::Error>;

    fn name(&self) -> &'static str;
}


pub fn get_available_chunkers() -> HashMap<&'static str, Box<dyn Chunker>> {
    let mut chunkers: HashMap<&'static str, Box<dyn Chunker>> = HashMap::new();

    for variant in Algorithm::value_variants() {
        let chunker = variant.create_chunker(); 
        let name = variant.as_str(); 

        chunkers.insert(name, chunker);
    }

    chunkers
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum Algorithm {
    #[value(name = "fixed-size-4k")]
    FixedSize4k,
    #[value(name = "fixed-size-64k")]
    FixedSize64k,
    
}

impl Algorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Algorithm::FixedSize4k => "fixed-size-4k",
            Algorithm::FixedSize64k => "fixed-size-64k",
        }
    }

    fn create_chunker(&self) -> Box<dyn Chunker> {
        match self {
            Algorithm::FixedSize4k => {
                let chunker = FixedSizeChunker::new(4096).expect("Failed to create FixedSizeChunker");
                Box::new(chunker)
            },
            Algorithm::FixedSize64k => {
                let chunker = FixedSizeChunker::new(65536).expect("Failed to create FixedSizeChunker");
                Box::new(chunker)
            },
        }
    }
}
