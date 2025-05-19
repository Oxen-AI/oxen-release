use std::{
    fs::{self, File},
    io::{self, Read, Write, BufReader, BufWriter, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File as AsyncFile,
    io::{AsyncReadExt, AsyncWriteExt,},
    sync::Semaphore,
    task::JoinHandle,
};
use std::error::Error;
use crate::chunker::Chunker;
use crate::xhash;
use rayon::prelude::*;

const METADATA_FILE_NAME: &str = "metadata.bin";

#[derive(Serialize, Deserialize, Debug)]
struct ChunkMetadata {
    original_file_name: String,
    original_file_size: u64,
    chunk_size: usize,
    chunks: Vec<String>,
}

pub struct FixedSizeMultiChunker {
    chunk_size: usize,
    concurrency: usize,
}

impl FixedSizeMultiChunker {
    pub fn new(chunk_size: usize, concurrency: usize) -> Result<Self, io::Error> {
        if chunk_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Chunk size cannot be zero",
            ));
        }
        if concurrency == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Concurrency must be greater than zero",
            ));
        }
        Ok(Self {
            chunk_size,
            concurrency,
        })
    }

    fn split_and_hash_chunks_rayon(
        &self,
        input_path: &Path,
        output_dir: &Path,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let input_file_metadata = fs::metadata(input_path)?;
        let total_size = input_file_metadata.len();

        if total_size == 0 {
            println!("Input file is empty. No chunks created.");
            return Ok(Vec::new());
        }

        fs::create_dir_all(output_dir)?;

        let chunk_size_u64 = self.chunk_size as u64;
        let num_chunks = (total_size + chunk_size_u64 - 1) / chunk_size_u64;

        println!(
            "Splitting '{}' ({} bytes) into {} chunks of approx. {} bytes each using Rayon...",
            input_path.display(),
            total_size,
            num_chunks,
            self.chunk_size
        );


        let tasks: Vec<(usize, u64, usize)> = (0..num_chunks)
            .map(|i| {
                let start = i * chunk_size_u64;
                let actual_chunk_size = std::cmp::min(chunk_size_u64, total_size - start) as usize;
                (i as usize, start, actual_chunk_size)
            })
            .collect();

        let results: Vec<Result<(usize, String), io::Error>> = tasks
            .into_par_iter()
            .map(|(chunk_index, start_offset, chunk_data_size)| -> Result<(usize, String), io::Error> {
                if chunk_data_size == 0 { 
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Chunk size is zero"));
                }

                let mut infile = File::open(input_path)?;
                infile.seek(SeekFrom::Start(start_offset))?;

                let mut buffer = vec![0; chunk_data_size];
                infile.read_exact(&mut buffer)?;

                let hash_str = xhash::hash_buffer_128bit(&buffer).to_string();
                let output_filepath = output_dir.join(&hash_str);

                if !output_filepath.exists() {
                    let mut outfile = File::create(&output_filepath)?;
                    outfile.write_all(&buffer)?;
                } else {
                    // println!(
                    //     "  Chunk {} (HASH: {}) already exists. Skipping write. {}",
                    //     chunk_index, hash_str, output_filepath.display()
                    // );
                }
                Ok((chunk_index, hash_str))
            })
            .collect();

        let mut indexed_hashes: Vec<(usize, String)> = Vec::with_capacity(num_chunks as usize);
        for result in results {
            match result {
                Ok(pair) => indexed_hashes.push(pair),
                Err(e) => return Err(Box::new(e)), // Propagate the first io::Error
            }
        }

        indexed_hashes.sort_by_key(|k| k.0);

        let chunk_filenames: Vec<String> = indexed_hashes.into_iter().map(|(_, hash)| hash).collect();

        println!("Rayon-based file splitting and hashing complete.");
        Ok(chunk_filenames)
    }
}


impl Chunker for FixedSizeMultiChunker {
    fn name(&self) -> &'static str {
        "fixed-size-64k-multithreaded"
    }

    fn pack(&self, input_file: &Path, output_dir: &Path) -> Result<PathBuf, io::Error> {
        fs::create_dir_all(output_dir)?;

        let original_file_metadata = fs::metadata(input_file)
            .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("Failed to read input file metadata '{}': {}", input_file.display(), e)))?;
        
        let original_file_size = original_file_metadata.len();
        let original_file_name = input_file
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("unknown_file"))
            .to_string_lossy()
            .into_owned();

        let chunk_filenames = self.split_and_hash_chunks_rayon(input_file, output_dir)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Chunking process failed: {}", e)))?;

        let metadata = ChunkMetadata {
            original_file_name,
            original_file_size,
            chunk_size: self.chunk_size,
            chunks: chunk_filenames, // This list is now correctly ordered and contains hashes
        };

        let metadata_path = output_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufWriter::new(
            File::create(&metadata_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to create metadata file '{}': {}", metadata_path.display(), e)))?
        );

        bincode::serialize_into(metadata_file, &metadata)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Metadata serialization error: {}", e)))?;

        Ok(output_dir.to_path_buf())
    }

    fn unpack(&self, chunk_dir: &Path, output_path: &Path) -> Result<PathBuf, io::Error> {
        let concurrency = self.concurrency; // Use configured concurrency for Tokio tasks

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to build Tokio runtime: {}", e)))?
            .block_on(async move {
                let metadata_path = chunk_dir.join(METADATA_FILE_NAME);
                let metadata_file = BufReader::new(File::open(&metadata_path).map_err(|e| {
                    io::Error::new(io::ErrorKind::NotFound, format!("Failed to open metadata file '{}': {}", metadata_path.display(), e))
                })?);

                let metadata: ChunkMetadata = bincode::deserialize_from(metadata_file).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("Failed to deserialize metadata: {}", e))
                })?;

                if let Some(parent_dir) = output_path.parent() {
                    if !parent_dir.exists() {
                        fs::create_dir_all(parent_dir).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to create output directory '{}': {}", parent_dir.display(), e)))?;
                    }
                }
                
                let mut output_file = AsyncFile::create(output_path).await.map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("Failed to create output file '{}': {}", output_path.display(), e))
                })?;

                if metadata.original_file_size > 0 {
                    output_file.set_len(metadata.original_file_size).await.map_err(|e| {
                        io::Error::new(io::ErrorKind::Other, format!("Failed to set output file length: {}", e))
                    })?;
                }
                

                let semaphore = Arc::new(Semaphore::new(concurrency));
                let mut chunk_read_futures = Vec::new();

                for (index, chunk_filename) in metadata.chunks.iter().enumerate() {
                    let chunk_path = chunk_dir.join(chunk_filename);

                    if !chunk_path.exists() {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("Chunk file not found during unpack: {}", chunk_path.display()),
                        ));
                    }

                    let chunk_path_clone = chunk_path.clone();
                    let semaphore_clone = Arc::clone(&semaphore);

                    let future: JoinHandle<Result<(usize, Vec<u8>), io::Error>> = tokio::spawn(async move {
                        let _permit = semaphore_clone.acquire_owned().await
                            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Semaphore acquire failed (broken semaphore)"))?;

                        let mut chunk_file = AsyncFile::open(&chunk_path_clone).await?;
                        let mut buffer = Vec::with_capacity(metadata.chunk_size);
                        chunk_file.read_to_end(&mut buffer).await?;
                        Ok((index, buffer))
                    });
                    chunk_read_futures.push(future);
                }

                let mut collected_chunk_data: Vec<(usize, Vec<u8>)> = Vec::with_capacity(metadata.chunks.len());
                for future_result in join_all(chunk_read_futures).await {
                    match future_result {
                        Ok(Ok(data_pair)) => collected_chunk_data.push(data_pair),
                        Ok(Err(io_err)) => return Err(io_err), 
                        Err(join_err) => return Err(io::Error::new(io::ErrorKind::Other, format!("Unpack task join error: {}", join_err))),
                    }
                }

                collected_chunk_data.sort_by_key(|k| k.0);

                for (_index, data) in collected_chunk_data {
                    output_file.write_all(&data).await?;
                }

                output_file.flush().await?;

                Ok(output_path.to_path_buf())
            })
    }
}
