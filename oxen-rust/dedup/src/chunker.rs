use std::{collections::HashMap, io, path::{Path, PathBuf}};
use clap::ValueEnum;
use thiserror::Error;
pub use crate::chunker::fixedsize::FixedSizeChunker;
pub use crate::chunker::fixedsize_multithreaded::FixedSizeMultiChunker;
pub use crate::chunker::copier::Copier;
pub use crate::chunker::fastcdchunker::FastCDChunker;

pub mod fixedsize;
pub mod copier;
pub mod fixedsize_multithreaded;
pub mod oxendedup;
pub mod fastcdchunker;

fn map_bincode_error(err: bincode::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("Bincode error: {:?}", err))
}

#[derive(Error, Debug)]
pub enum FrameworkError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Bincode serialization/deserialization error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Chunker '{name}' not found")]
    ChunkerNotFound { name: String },
    #[error("System time error: {message}")]
    TimeError {
        message: String,
        source: std::time::SystemTimeError,
    },
    #[error("File Verification failed")]
    VerificationFailed,

    #[error("Internal error: {message}")]
    InternalError{
        message: String,
    },
}

pub type FrameworkResult<T> = Result<T, FrameworkError>;

pub trait Chunker {

    fn pack(&self, input_file: &Path, output_dir: &Path) -> Result<PathBuf, io::Error>;

    fn unpack(&self, input_dir: &Path, output_path: &Path) -> Result<PathBuf, io::Error>;

    fn name(&self) -> &'static str;
}

pub fn get_chunker(algorithm: &Algorithm) -> FrameworkResult<Box<dyn Chunker>> {
    let mut chunkers: HashMap<&'static str, Box<dyn Chunker>> = HashMap::new();

    for variant in Algorithm::value_variants() {
        let chunker = variant.create_chunker(); 
        let name = variant.as_str(); 

        chunkers.insert(name, chunker);
    }
    let chunker_name = algorithm.as_str();
    chunkers.remove(chunker_name)
        .ok_or_else(|| FrameworkError::ChunkerNotFound { name: chunker_name.to_string() })
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum Algorithm {
    #[value(name = "fixed-size-4k")]
    FixedSize4k,
    #[value(name = "fixed-size-64k")]
    FixedSize64k,
    #[value(name = "fixed-size-256k")]
    FixedSize256k,
    #[value(name = "fixed-size-512k")]
    FixedSize512k,
    #[value(name = "fixed-size-1m")]
    FixedSize1M,
    #[value(name = "fixed-size-64k-multithreaded")]
    FixedSize64kMultiThreaded,
    #[value(name = "copier")]
    Copier,
    #[value(name = "fastcdc")]
    FastCDC,
}

impl Algorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Algorithm::FixedSize4k => "fixed-size-4k",
            Algorithm::FixedSize64k => "fixed-size-64k",
            Algorithm::FixedSize256k => "fixed-size-256k",
            Algorithm::FixedSize512k => "fixed-size-512k",
            Algorithm::FixedSize1M => "fixed-size-1m",
            Algorithm::Copier => "copier",
            Algorithm::FixedSize64kMultiThreaded => "fixed-size-64k-multithreaded",
            Algorithm::FastCDC => "fastcdc",
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
            Algorithm::FixedSize256k => {
                let chunker = FixedSizeChunker::new(262144).expect("Failed to create FixedSizeChunker");
                Box::new(chunker)
            },
            Algorithm::FixedSize512k => {
                let chunker = FixedSizeChunker::new(524288).expect("Failed to create FixedSizeChunker");
                Box::new(chunker)
            },
            Algorithm::FixedSize1M => {
                let chunker = FixedSizeChunker::new(1048576).expect("Failed to create FixedSizeChunker");
                Box::new(chunker)
            },
            Algorithm::Copier => {
                let mover = Copier {};
                Box::new(mover)
            },
            Algorithm::FixedSize64kMultiThreaded => {
                let chunker = FixedSizeMultiChunker::new(65536, 16).expect("Failed to create FixedSizeMultiChunker");
                Box::new(chunker)
            },
            Algorithm::FastCDC => {
                let chunker = FastCDChunker::new(65536, 16).expect("Failed to create FastCDChunker");
                Box::new(chunker)
            },
        }
    }
}
