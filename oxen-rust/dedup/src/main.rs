use clap::{Parser, Subcommand};
use std::fs;
use std::io;
use std::path::{PathBuf};
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use thiserror::Error;
use crate::chunker::{Algorithm,get_available_chunkers};

pub mod chunker;
pub mod xhash;


#[derive(Error, Debug)]
enum FrameworkError {
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
}

type FrameworkResult<T> = Result<T, FrameworkError>;




#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {

    Pack {
        #[arg(short, long, value_enum)]
        algorithm: Algorithm,

        #[arg(short, long)]
        file: PathBuf,

        #[arg(short, long)]
        output_dir: PathBuf,
    },

    Unpack {

        #[arg(short, long, value_enum)]
        algorithm: Algorithm,

        #[arg(short, long)]
        input_dir: PathBuf,

        #[arg(short, long)]
        output_file: PathBuf,
    },


    Test {

        #[arg(short, long, value_enum)]
        algorithm: Algorithm,

        #[arg(short, long)]
        input_file: PathBuf,

        #[arg(short, long)]
        use_temp: bool,
    },
}

fn main() -> FrameworkResult<()> {
    let args = Args::parse();

    match args.command {
        Commands::Pack { algorithm, file, output_dir } => {
            let available_chunkers = get_available_chunkers();
            let chunker_name = algorithm.as_str();
            let chunker = available_chunkers.get(chunker_name)
                .ok_or_else(|| FrameworkError::ChunkerNotFound { name: chunker_name.to_string() })?;

            chunker.pack(&file, &output_dir)?;

            Ok(())
        }
        Commands::Unpack { algorithm, input_dir, output_file } => {
            let available_chunkers = get_available_chunkers();
            let chunker_name = algorithm.as_str();
            let chunker = available_chunkers.get(chunker_name)
                .ok_or_else(|| FrameworkError::ChunkerNotFound { name: chunker_name.to_string() })?;
            _ = chunker.unpack(&input_dir, &output_file);
            Ok(())
        }

        Commands::Test { algorithm, input_file , use_temp} =>{
            let available_chunkers = get_available_chunkers();
            let chunker_name = algorithm.as_str();
            let chunker = available_chunkers.get(chunker_name)
                .ok_or_else(|| FrameworkError::ChunkerNotFound { name: chunker_name.to_string() })?;


            let base_dir = if use_temp {
                let temp_dir = std::env::temp_dir();
                temp_dir
            } else {
                std::env::current_dir()?
            };

            let timestamp_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| FrameworkError::TimeError {
                    message: "System time went backwards".to_string(),
                    source: e,
                })?
                .as_nanos();

            let test_dir_name = format!("chunker_test_{}", timestamp_nanos);
            let test_dir = base_dir.join(test_dir_name);

            println!("Creating test directory: {:?}", test_dir);

            fs::create_dir_all(&test_dir)?;

            println!("Packing {:?} into {:?}", input_file, test_dir);
            let pack_start_time = Instant::now();
            chunker.pack(&input_file, &test_dir)?;
            let pack_elapsed_time = pack_start_time.elapsed();
            println!("Pack step in test finished in {:?}", pack_elapsed_time);

            let unpacked_output_file = test_dir.join("unpacked_output");
            println!("Unpacking from {:?} to {:?}", test_dir, unpacked_output_file);

            let unpack_start_time = Instant::now();
            chunker.unpack(&test_dir, &unpacked_output_file)?;
            let unpack_elapsed_time = unpack_start_time.elapsed();
            println!("Unpack step in test finished in {:?}", unpack_elapsed_time);


            println!("Test completed successfully. Packed and unpacked files are in: {:?}", test_dir);

            Ok(())
        }
    }
}
